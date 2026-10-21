//! Live control: pause, resume and scaling, and the `externally-controlled`
//! executor.

use std::time::{Duration, Instant};

use k9_core::engine::{ControlError, EngineError, RunControl, RunOptions, run, run_with};
use k9_core::segment::Segment;
use k9_core::{Format, Plan, load_plan};
use k9_testserver::TestServer;

fn plan(yaml: &str) -> Plan {
    load_plan(yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"))
}

fn external(url: &str, vus: u32, max: u32, duration: &str) -> Plan {
    plan(&format!(
        "options:\n  scenarios:\n    live: {{ executor: externally-controlled, vus: {vus}, maxVUs: {max}, duration: {duration} }}\nflows:\n  default:\n    - request: {{ url: '{url}' }}\n"
    ))
}

/// Waits (up to 2 s) until `ok` holds for the run's status.
async fn until(control: &RunControl, what: &str, ok: impl Fn(&k9_core::engine::Status) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let status = control.status();
        if ok(&status) {
            return;
        }
        assert!(Instant::now() < deadline, "{what}: {status:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn externally_controlled_vus_can_be_scaled_up_to_max_vus() {
    let server = TestServer::start().await.unwrap();
    let plan = external(&server.url("/delay/50"), 2, 5, "1500ms");
    let control = RunControl::new();
    let started = Instant::now();
    let run = tokio::spawn({
        let control = control.clone();
        async move { run(&plan, control).await }
    });
    until(&control, "starts with `vus`", |s| s.running && s.vus == 2).await;
    assert_eq!(control.status().vus_max, 5);
    control.scale(4).unwrap();
    until(&control, "scaled up", |s| s.vus == 4).await;
    assert_eq!(
        control.scale(6),
        Err(ControlError::TooManyVus { vus: 6, max: 5 })
    );
    control.scale(1).unwrap();
    // Retired VUs finish their iteration first.
    until(&control, "scaled down", |s| s.vus == 1).await;
    let result = run.await.unwrap().unwrap();
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(1500) && took < Duration::from_millis(2500),
        "{took:?}"
    );
    assert_eq!(result.max_vus, 5);
    assert!(result.iterations_complete > 20);
    assert!(!control.status().running);
}

#[tokio::test(flavor = "multi_thread")]
async fn pausing_holds_new_iterations_and_the_duration_clock() {
    let server = TestServer::start().await.unwrap();
    let plan = external(&server.url("/delay/20"), 2, 2, "1s");
    let control = RunControl::new();
    let started = Instant::now();
    let run = tokio::spawn({
        let control = control.clone();
        async move { run(&plan, control).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    control.pause().unwrap();
    assert!(control.status().paused);
    // In-flight iterations finish; then nothing new starts. (Progress is
    // published every 250 ms.)
    tokio::time::sleep(Duration::from_millis(350)).await;
    let before = control.progress().borrow().iterations_complete;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(control.progress().borrow().iterations_complete, before);
    control.resume().unwrap();
    let result = run.await.unwrap().unwrap();
    // 1s of running plus ~850ms paused.
    let took = started.elapsed();
    assert!(took >= Duration::from_millis(1800), "{took:?}");
    assert!(result.iterations_complete > before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_can_start_paused_whatever_its_executors() {
    let server = TestServer::start().await.unwrap();
    let plan = plan(&format!(
        "options: {{ vus: 1, duration: 300ms }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
        server.url("/ok")
    ));
    let control = RunControl::new();
    control.pause().unwrap();
    let run = tokio::spawn({
        let control = control.clone();
        async move { run(&plan, control).await }
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let status = control.status();
    assert!(status.paused && !status.running, "{status:?}");
    assert_eq!(control.progress().borrow().iterations_complete, 0);
    control.resume().unwrap();
    let result = run.await.unwrap().unwrap();
    // The run's clock starts on resume.
    assert!(
        result.duration < Duration::from_millis(400),
        "{:?}",
        result.duration
    );
    assert!(result.iterations_complete > 0);
}

/// A control API serves before the run starts: scaling must work at once,
/// and the run must keep what was set.
#[tokio::test(flavor = "multi_thread")]
async fn a_prepared_run_can_be_scaled_before_it_starts() {
    let server = TestServer::start().await.unwrap();
    let plan = external(&server.url("/delay/20"), 1, 4, "10m");
    let control = RunControl::new();
    assert!(matches!(control.scale(3), Err(ControlError::NotScalable)));
    control.prepare(&plan);
    control.pause().unwrap();
    control.scale(3).unwrap();
    assert!(matches!(
        control.scale(9),
        Err(ControlError::TooManyVus { vus: 9, max: 4 })
    ));
    let run = tokio::spawn({
        let control = control.clone();
        async move { run(&plan, control).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    control.resume().unwrap();
    until(&control, "running with 3 VUs", |s| s.running && s.vus == 3).await;
    control.stop();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn only_externally_controlled_runs_can_be_paused_or_scaled_once_started() {
    let server = TestServer::start().await.unwrap();
    let plan = plan(&format!(
        "options: {{ vus: 1, duration: 500ms }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
        server.url("/ok")
    ));
    let control = RunControl::new();
    let run = tokio::spawn({
        let control = control.clone();
        async move { run(&plan, control).await }
    });
    until(&control, "running", |s| s.running).await;
    assert_eq!(control.pause(), Err(ControlError::NotPausable));
    assert_eq!(control.scale(2), Err(ControlError::NotScalable));
    control.stop();
    run.await.unwrap().unwrap();
    assert!(control.status().stopped);
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_a_paused_run_ends_it() {
    let server = TestServer::start().await.unwrap();
    let plan = external(&server.url("/ok"), 1, 1, "10m");
    let control = RunControl::new();
    control.pause().unwrap();
    let run = tokio::spawn({
        let control = control.clone();
        async move { run(&plan, control).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    control.stop();
    let result = tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .expect("a stop ends a paused run")
        .unwrap()
        .unwrap();
    assert!(result.stopped);
    assert_eq!(result.iterations_complete, 0);
}

#[tokio::test]
async fn externally_controlled_scenarios_cant_be_split() {
    let plan = external("http://127.0.0.1:9/", 2, 4, "1s");
    let options = RunOptions {
        segment: Segment::split(2).remove(0),
        ..RunOptions::default()
    };
    let err = run_with(&plan, RunControl::new(), options)
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(&err, EngineError::Unsplittable { scenario } if scenario == "live"),
        "{err}"
    );
}

#[test]
fn at_most_one_scenario_is_externally_controlled() {
    let err = load_plan(
        "options:\n  scenarios:\n    a: { executor: externally-controlled, duration: 1s }\n    b: { executor: externally-controlled, duration: 1s }\nflows: { default: [] }\n",
        Format::Yaml,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("externally-controlled"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn live_metrics_are_published_every_second_to_watchers() {
    let server = TestServer::start().await.unwrap();
    let plan = external(&server.url("/delay/20"), 2, 2, "1500ms");
    let control = RunControl::new();
    let mut metrics = control.metrics();
    let run = tokio::spawn({
        let control = control.clone();
        async move { run(&plan, control).await }
    });
    tokio::time::timeout(Duration::from_secs(2), metrics.changed())
        .await
        .expect("published within a second")
        .unwrap();
    let live = metrics.borrow_and_update().clone();
    let reqs = live
        .iter()
        .find(|m| m.name == "http_reqs")
        .expect("http_reqs");
    assert_eq!(reqs.kind, k9_metrics::MetricKind::Counter);
    let count = reqs.sample.iter().find(|(k, _)| k == "count").unwrap().1;
    assert!(count > 0.0, "{reqs:?}");
    assert!(reqs.sample.iter().any(|(k, _)| k == "rate"));
    let duration = live
        .iter()
        .find(|m| m.name == "http_req_duration")
        .expect("http_req_duration");
    assert_eq!(duration.contains, k9_metrics::ValueType::Time);
    let keys: Vec<&str> = duration.sample.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, ["avg", "max", "med", "min", "p(90)", "p(95)"]);
    let min = duration.sample[3].1;
    assert!(min >= 20.0, "times in ms: {min}");
    run.await.unwrap().unwrap();
    // A last update at the end has every iteration.
    let last = metrics.borrow().clone();
    let reqs = last.iter().find(|m| m.name == "http_reqs").unwrap();
    assert!(reqs.sample[0].1 > count);
}

#[tokio::test(flavor = "multi_thread")]
async fn live_frames_carry_each_second_checks_and_thresholds() {
    let server = TestServer::start().await.unwrap();
    let plan = load_plan(
        &format!(
            "options:\n  vus: 2\n  duration: 2500ms\n  thresholds: {{ http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<1'] }}\nflows:\n  default:\n    - request: {{ url: '{}', checks: {{ ok: {{ status: 200 }}, created: {{ status: 201 }} }} }}\n",
            server.url("/delay/20")
        ),
        Format::Yaml,
    )
    .unwrap();
    let control = RunControl::new();
    let mut frames = control.frames();
    let run = tokio::spawn({
        let control = control.clone();
        async move { run(&plan, control).await }
    });
    tokio::time::timeout(Duration::from_secs(2), frames.changed())
        .await
        .expect("published within a second")
        .unwrap();
    let frame = frames.borrow_and_update().clone();
    assert!(
        frame.elapsed >= Duration::from_millis(900),
        "{:?}",
        frame.elapsed
    );
    // This second's requests: 2 VUs × ~40 per second.
    let reqs = frame.second("http_reqs").expect("http_reqs this second");
    assert!(
        matches!(reqs, k9_metrics::Stats::Counter { sum } if (40.0..=100.0).contains(&sum)),
        "{reqs:?}"
    );
    assert!(matches!(
        frame.second("http_req_duration"),
        Some(k9_metrics::Stats::Trend { p95, .. }) if p95 >= 20.0
    ));
    assert!(frame.totals.iter().any(|m| m.name == "iterations"));
    let names: Vec<&str> = frame.checks.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["created", "ok"]);
    assert!(frame.checks[1].passes > 0 && frame.checks[1].fails == 0);
    assert!(frame.checks[0].passes == 0 && frame.checks[0].fails > 0);
    let thresholds: Vec<(&str, bool)> = frame
        .thresholds
        .iter()
        .map(|t| (t.selector.as_str(), t.passed))
        .collect();
    assert_eq!(
        thresholds,
        [("http_req_failed", true), ("http_req_duration", false)]
    );
    run.await.unwrap().unwrap();
}
