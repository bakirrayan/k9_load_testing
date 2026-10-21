//! The engine against k9-testserver.

use std::time::{Duration, Instant};

use k9_core::engine::{RunControl, RunResult, run};
use k9_core::{Format, Plan, load_plan};
use k9_metrics::{MetricId, Value};
use k9_testserver::TestServer;

fn plan(yaml: &str) -> Plan {
    load_plan(yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"))
}

async fn run_plan(yaml: &str) -> RunResult {
    run(&plan(yaml), RunControl::new()).await.unwrap()
}

fn merged(result: &RunResult, metric: MetricId, filter: &[(&str, &str)]) -> Option<Value> {
    let m = result.aggregator.metrics();
    result
        .aggregator
        .totals()
        .merged(metric, filter, m.interner())
}

fn counter(result: &RunResult, metric: MetricId, filter: &[(&str, &str)]) -> f64 {
    match merged(result, metric, filter) {
        Some(Value::Counter(c)) => c.sum,
        None => 0.0,
        other => panic!("not a counter: {other:?}"),
    }
}

fn rate(result: &RunResult, metric: MetricId) -> f64 {
    match merged(result, metric, &[]) {
        Some(Value::Rate(r)) => r.rate(),
        other => panic!("not a rate: {other:?}"),
    }
}

fn trend(result: &RunResult, metric: MetricId, filter: &[(&str, &str)]) -> k9_metrics::TrendValue {
    match merged(result, metric, filter) {
        Some(Value::Trend(t)) => t,
        other => panic!("not a trend: {other:?}"),
    }
}

fn assert_between(what: &str, value: Duration, min: Duration, max: Duration) {
    assert!(
        value >= min && value <= max,
        "{what} = {value:?}, expected {min:?}..={max:?}"
    );
}

#[tokio::test]
async fn constant_vus_runs_for_its_duration_and_records_k6_metrics() {
    let server = TestServer::start().await.unwrap();
    let url = server.url("/delay/50");
    let result = run_plan(&format!(
        "options: {{ vus: 5, duration: 1s }}\nflows:\n  default:\n    - request: {{ url: '{url}' }}\n"
    ))
    .await;
    let b = result.builtins;

    assert_between(
        "run duration",
        result.duration,
        Duration::from_millis(1000),
        Duration::from_millis(1300),
    );
    // 5 VUs × 1 s / ~51 ms per iteration ≈ 98 iterations.
    let iterations = result.iterations_complete;
    assert!(
        (75..=100).contains(&iterations),
        "iterations = {iterations}"
    );
    assert_eq!(counter(&result, b.iterations, &[]), iterations as f64);
    assert_eq!(counter(&result, b.http_reqs, &[]), iterations as f64);
    assert_eq!(result.iterations_interrupted, 0);
    assert_eq!(result.max_vus, 5);

    let duration = trend(&result, b.http_req_duration, &[]);
    assert!(
        (50.0..=65.0).contains(&duration.avg()),
        "http_req_duration avg = {}",
        duration.avg()
    );
    assert!(trend(&result, b.iteration_duration, &[]).avg() >= 50.0);
    assert_eq!(rate(&result, b.http_req_failed), 0.0);
    assert!(counter(&result, b.data_sent, &[]) > 0.0);
    assert!(counter(&result, b.data_received, &[]) > 0.0);
    assert_eq!(server.connections(), 5, "one keep-alive connection per VU");

    // k6's system tags on HTTP metrics.
    let tagged = counter(
        &result,
        b.http_reqs,
        &[
            ("scenario", "default"),
            ("method", "GET"),
            ("status", "200"),
            ("url", url.as_str()),
            ("name", url.as_str()),
            ("expected_response", "true"),
            ("proto", "HTTP/1.1"),
            ("group", ""),
        ],
    );
    assert_eq!(tagged, iterations as f64);

    match merged(&result, b.vus_max, &[]) {
        Some(Value::Gauge(g)) => assert_eq!(g.max, 5.0),
        other => panic!("vus_max: {other:?}"),
    }
    // Like k6, `vus` is sampled while the test runs, never after VUs have exited
    // (VUs finishing during the graceful stop may still show up as fewer than 5).
    match merged(&result, b.vus, &[]) {
        Some(Value::Gauge(g)) => {
            assert_eq!(g.max, 5.0, "{g:?}");
            assert!(g.min > 0.0 && g.value > 0.0, "{g:?}");
        }
        other => panic!("vus: {other:?}"),
    }
}

#[tokio::test]
async fn short_runs_still_sample_vus() {
    let server = TestServer::start().await.unwrap();
    let result = run_plan(&format!(
        "options: {{ vus: 3, iterations: 3 }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
        server.url("/ok")
    ))
    .await;
    match merged(&result, result.builtins.vus, &[]) {
        Some(Value::Gauge(g)) => assert_eq!(g.max, 3.0, "{g:?}"),
        other => panic!("vus: {other:?}"),
    }
}

#[tokio::test]
async fn failed_requests_are_counted_and_errors_reported() {
    let server = TestServer::start().await.unwrap();
    let closed_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let result = run_plan(&format!(
        "options: {{ vus: 1, iterations: 3 }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n    - request: {{ url: 'http://127.0.0.1:{closed_port}/' }}\n",
        server.url("/status/503")
    ))
    .await;
    let b = result.builtins;

    assert_eq!(counter(&result, b.http_reqs, &[]), 6.0);
    assert_eq!(rate(&result, b.http_req_failed), 1.0);
    assert_eq!(
        counter(
            &result,
            b.http_reqs,
            &[("status", "503"), ("expected_response", "false")]
        ),
        3.0
    );
    assert_eq!(counter(&result, b.http_reqs, &[("status", "0")]), 3.0);
    assert_eq!(result.request_errors.len(), 1);
    assert_eq!(result.request_errors[0].1, 3);
    assert!(
        result.request_errors[0].0.contains("connect"),
        "{:?}",
        result.request_errors
    );
    assert_eq!(
        result.iterations_complete, 3,
        "failed requests don't abort iterations"
    );
}

#[tokio::test]
async fn graceful_stop_lets_in_flight_iterations_finish() {
    let server = TestServer::start().await.unwrap();
    let started = Instant::now();
    let result = run_plan(&format!(
        r#"
options:
  scenarios:
    s: {{ executor: constant-vus, vus: 2, duration: 200ms, gracefulStop: 2s }}
flows:
  default:
    - request: {{ url: '{}' }}
"#,
        server.url("/delay/600")
    ))
    .await;
    assert_between(
        "run",
        started.elapsed(),
        Duration::from_millis(600),
        Duration::from_millis(900),
    );
    assert_eq!(result.iterations_complete, 2);
    assert_eq!(result.iterations_interrupted, 0);
}

#[tokio::test]
async fn iterations_past_graceful_stop_are_interrupted() {
    let server = TestServer::start().await.unwrap();
    let started = Instant::now();
    let result = run_plan(&format!(
        r#"
options:
  scenarios:
    s: {{ executor: constant-vus, vus: 2, duration: 200ms, gracefulStop: 300ms }}
flows:
  default:
    - request: {{ url: '{}' }}
"#,
        server.url("/delay/5000")
    ))
    .await;
    assert_between(
        "run",
        started.elapsed(),
        Duration::from_millis(500),
        Duration::from_millis(800),
    );
    assert_eq!(result.iterations_complete, 0);
    assert_eq!(result.iterations_interrupted, 2);
    assert_eq!(
        counter(&result, result.builtins.iterations, &[]),
        0.0,
        "interrupted iterations aren't counted"
    );
}

#[tokio::test]
async fn scenarios_run_in_parallel_and_honour_start_time() {
    let server = TestServer::start().await.unwrap();
    let started = Instant::now();
    let result = run_plan(&format!(
        r#"
options:
  scenarios:
    early: {{ executor: constant-vus, vus: 1, duration: 600ms }}
    late: {{ executor: constant-vus, vus: 2, duration: 300ms, startTime: 300ms, exec: other }}
flows:
  default:
    - request: {{ url: '{}' }}
  other:
    - request: {{ url: '{}' }}
"#,
        server.url("/delay/20"),
        server.url("/status/201"),
    ))
    .await;
    assert_between(
        "run",
        started.elapsed(),
        Duration::from_millis(600),
        Duration::from_millis(850),
    );
    let b = result.builtins;
    assert!(
        counter(
            &result,
            b.http_reqs,
            &[("scenario", "early"), ("status", "200")]
        ) > 10.0
    );
    assert!(
        counter(
            &result,
            b.http_reqs,
            &[("scenario", "late"), ("status", "201")]
        ) > 10.0
    );
    assert_eq!(
        counter(
            &result,
            b.http_reqs,
            &[("scenario", "late"), ("status", "200")]
        ),
        0.0
    );
    assert_eq!(result.max_vus, 3);
}

#[tokio::test]
async fn stop_interrupts_everything_and_still_returns_results() {
    let server = TestServer::start().await.unwrap();
    let control = RunControl::new();
    let plan = plan(&format!(
        "options: {{ vus: 3, duration: 30s }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
        server.url("/delay/5000")
    ));
    let stopper = {
        let control = control.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            control.stop();
        })
    };
    let started = Instant::now();
    let result = run(&plan, control).await.unwrap();
    stopper.await.unwrap();

    assert!(
        started.elapsed() < Duration::from_millis(600),
        "{:?}",
        started.elapsed()
    );
    assert!(result.stopped);
    assert_eq!(result.iterations_interrupted, 3);
}

#[tokio::test]
async fn groups_and_sleeps_are_timed_and_tagged() {
    let server = TestServer::start().await.unwrap();
    let result = run_plan(&format!(
        r#"
options: {{ vus: 1, iterations: 2 }}
flows:
  default:
    - group:
        name: login
        steps:
          - request: {{ url: '{}' }}
          - group:
              name: profile
              steps:
                - request: {{ url: '{}', name: profile-page }}
    - sleep: 100ms
"#,
        server.url("/delay/10"),
        server.url("/ok"),
    ))
    .await;
    let b = result.builtins;
    assert_eq!(counter(&result, b.http_reqs, &[("group", "::login")]), 2.0);
    assert_eq!(
        counter(
            &result,
            b.http_reqs,
            &[("group", "::login::profile"), ("name", "profile-page")]
        ),
        2.0
    );
    let login = trend(&result, b.group_duration, &[("group", "::login")]);
    assert_eq!(login.count, 2);
    assert!(login.min >= 10.0);
    assert!(trend(&result, b.iteration_duration, &[]).min >= 110.0);
}

#[tokio::test]
async fn progress_reports_active_vus() {
    let server = TestServer::start().await.unwrap();
    let control = RunControl::new();
    let mut progress = control.progress();
    let plan = plan(&format!(
        "options: {{ vus: 4, duration: 700ms }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
        server.url("/delay/20")
    ));
    let run = tokio::spawn(async move { run(&plan, control).await.unwrap() });
    let mut seen_active = 0;
    while !run.is_finished() {
        if tokio::time::timeout(Duration::from_millis(100), progress.changed())
            .await
            .is_ok()
        {
            seen_active = seen_active.max(progress.borrow().active_vus);
        }
    }
    let result = run.await.unwrap();
    assert_eq!(seen_active, 4);
    let last = *progress.borrow();
    assert_eq!(last.iterations_complete, result.iterations_complete);
    assert_eq!(last.max_vus, 4);
}

#[tokio::test]
async fn ramping_vus_follows_its_stages() {
    let server = TestServer::start().await.unwrap();
    let control = RunControl::new();
    let mut progress = control.progress();
    let plan = plan(&format!(
        r#"
options:
  scenarios:
    ramp:
      executor: ramping-vus
      startVUs: 0
      stages:
        - {{ duration: 1s, target: 4 }}
        - {{ duration: 1s, target: 0 }}
flows:
  default:
    - request: {{ url: '{}' }}
"#,
        server.url("/delay/20")
    ));
    let started = Instant::now();
    let handle = tokio::spawn(async move { run(&plan, control).await.unwrap() });
    // (elapsed, active VUs) as seen through progress updates.
    let mut seen = Vec::new();
    while !handle.is_finished() {
        if tokio::time::timeout(Duration::from_millis(50), progress.changed())
            .await
            .is_ok()
        {
            let p = *progress.borrow();
            seen.push((p.elapsed, p.active_vus));
        }
    }
    let result = handle.await.unwrap();

    assert_between(
        "run",
        started.elapsed(),
        Duration::from_millis(2000),
        Duration::from_millis(2300),
    );
    // k6's schedule: one more VU every 250 ms up to 4 at 1 s, then one fewer
    // every 250 ms. Progress samples on the same 250 ms grid, so check the
    // shape: rises to 4, then falls, never jumping more than one step.
    let counts: Vec<u32> = seen.iter().map(|(_, v)| *v).collect();
    let peak = counts.iter().position(|v| *v == 4).expect("reached 4 VUs");
    assert!(
        counts[..=peak]
            .windows(2)
            .all(|w| w[1] >= w[0] && w[1] - w[0] <= 1),
        "{seen:?}"
    );
    assert!(
        counts[peak..]
            .windows(2)
            .all(|w| w[1] <= w[0] && w[0] - w[1] <= 1),
        "{seen:?}"
    );
    assert!(counts[0] <= 1 && *counts.last().unwrap() <= 1, "{seen:?}");
    assert_eq!(result.max_vus, 4);
    assert_eq!(
        result.iterations_interrupted, 0,
        "20 ms iterations finish within gracefulRampDown"
    );
    assert!(result.iterations_complete > 50);
}

#[tokio::test]
async fn ramp_down_interrupts_iterations_after_graceful_ramp_down() {
    let server = TestServer::start().await.unwrap();
    let started = Instant::now();
    let result = run_plan(&format!(
        r#"
options:
  scenarios:
    ramp:
      executor: ramping-vus
      startVUs: 2
      gracefulRampDown: 200ms
      stages:
        - {{ duration: 100ms, target: 0 }}
flows:
  default:
    - request: {{ url: '{}' }}
"#,
        server.url("/delay/5000")
    ))
    .await;
    // Retired at 50 ms and 100 ms, interrupted 200 ms later.
    assert_between(
        "run",
        started.elapsed(),
        Duration::from_millis(280),
        Duration::from_millis(600),
    );
    assert_eq!(result.iterations_complete, 0);
    assert_eq!(result.iterations_interrupted, 2);
}

#[tokio::test]
async fn invalid_request_urls_are_rejected_up_front() {
    let err = run(
        &plan("flows:\n  default:\n    - group: { name: g, steps: [{ request: { url: 'ftp://x/' } }] }\n"),
        RunControl::new(),
    )
    .await
    .err()
    .expect("ftp isn't supported");
    let msg = err.to_string();
    assert!(msg.contains("flows.default[0].steps[0]"), "{msg}");
    assert!(msg.contains("unsupported scheme"), "{msg}");
}

// --- HTTP options -------------------------------------------------

#[tokio::test]
async fn insecure_skip_tls_verify_option_allows_self_signed_targets() {
    let server = TestServer::start().await.unwrap();
    let url = server.https_url("/ok");
    let flow = format!("flows:\n  default:\n    - request: {{ url: '{url}' }}\n");
    let strict = run_plan(&format!("options: {{ iterations: 1 }}\n{flow}")).await;
    assert_eq!(
        strict.request_errors.len(),
        1,
        "self-signed is rejected by default"
    );

    let relaxed = run_plan(&format!(
        "options: {{ iterations: 1, insecureSkipTLSVerify: true }}\n{flow}"
    ))
    .await;
    assert!(
        relaxed.request_errors.is_empty(),
        "{:?}",
        relaxed.request_errors
    );
    assert_eq!(
        counter(&relaxed, relaxed.builtins.http_reqs, &[("status", "200")]),
        1.0
    );
}

#[tokio::test]
async fn each_redirect_hop_is_its_own_request_sample() {
    let server = TestServer::start().await.unwrap();
    let start = server.url("/redirect/2");
    let result = run_plan(&format!(
        "options: {{ iterations: 1 }}\nflows:\n  default:\n    - request: {{ url: '{start}', name: login }}\n"
    ))
    .await;
    let b = result.builtins;
    assert_eq!(counter(&result, b.http_reqs, &[]), 3.0);
    assert_eq!(
        counter(
            &result,
            b.http_reqs,
            &[("status", "302"), ("url", start.as_str())]
        ),
        1.0
    );
    assert_eq!(
        counter(
            &result,
            b.http_reqs,
            &[
                ("status", "302"),
                ("url", server.url("/redirect/1").as_str())
            ]
        ),
        1.0
    );
    assert_eq!(
        counter(
            &result,
            b.http_reqs,
            &[
                ("status", "200"),
                ("url", server.url("/redirect/0").as_str()),
                ("name", "login")
            ]
        ),
        1.0,
        "the final response keeps the request's name"
    );
    assert_eq!(
        rate(&result, b.http_req_failed),
        0.0,
        "3xx hops count as expected responses"
    );
    assert_eq!(trend(&result, b.http_req_duration, &[]).count, 3);

    let capped = run_plan(&format!(
        "options: {{ iterations: 1, maxRedirects: 0 }}\nflows:\n  default:\n    - request: {{ url: '{start}' }}\n"
    ))
    .await;
    assert_eq!(
        counter(&capped, capped.builtins.http_reqs, &[("status", "302")]),
        1.0
    );
    assert_eq!(counter(&capped, capped.builtins.http_reqs, &[]), 1.0);
}

#[tokio::test]
async fn cookie_jar_is_reset_every_iteration() {
    let server = TestServer::start().await.unwrap();
    let result = run_plan(&format!(
        r#"
options: {{ vus: 1, iterations: 3 }}
flows:
  default:
    - request: {{ url: '{}', name: before }}
    - request: {{ url: '{}' }}
    - request: {{ url: '{}', name: after }}
"#,
        server.url("/cookies/has/session"),
        server.url("/cookies/set/session/abc"),
        server.url("/cookies/has/session"),
    ))
    .await;
    let b = result.builtins;
    assert_eq!(
        counter(
            &result,
            b.http_reqs,
            &[("name", "before"), ("status", "404")]
        ),
        3.0
    );
    assert_eq!(
        counter(
            &result,
            b.http_reqs,
            &[("name", "after"), ("status", "200")]
        ),
        3.0
    );
}

#[tokio::test]
async fn user_agent_option_is_sent() {
    let server = TestServer::start().await.unwrap();
    let result = run_plan(&format!(
        "options: {{ iterations: 1, userAgent: probe/1 }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
        server.url("/expect-header/user-agent/probe%2F1")
    ))
    .await;
    assert_eq!(
        counter(&result, result.builtins.http_reqs, &[("status", "200")]),
        1.0
    );
}

// --- checks and thresholds -----------------------------------------

fn outcome<'a>(
    result: &'a RunResult,
    selector: &str,
    expr: &str,
) -> &'a k9_core::engine::ThresholdOutcome {
    result
        .thresholds
        .iter()
        .find(|t| t.selector == selector && t.expression == expr)
        .unwrap_or_else(|| panic!("no outcome for {selector} {expr}: {:?}", result.thresholds))
}

#[tokio::test]
async fn checks_record_pass_and_fail_per_check_name() {
    let server = TestServer::start().await.unwrap();
    let result = run_plan(&format!(
        r#"
options: {{ vus: 1, iterations: 4 }}
flows:
  default:
    - group:
        name: api
        steps:
          - request:
              url: '{}'
              checks:
                "status is 200": {{ status: 200 }}
                "says delayed": {{ bodyContains: delayed }}
                "under 5ms": {{ durationBelow: 5ms }}
                "is an error": {{ status: [500, 503] }}
"#,
        server.url("/delay/20")
    ))
    .await;
    let b = result.builtins;
    assert_eq!(rate(&result, b.checks), 0.5);
    let check = |name: &str| match merged(&result, b.checks, &[("check", name), ("group", "::api")])
    {
        Some(Value::Rate(r)) => (r.trues, r.total),
        other => panic!("{name}: {other:?}"),
    };
    assert_eq!(check("status is 200"), (4, 4));
    assert_eq!(check("says delayed"), (4, 4));
    assert_eq!(check("under 5ms"), (0, 4));
    assert_eq!(check("is an error"), (0, 4));
}

#[tokio::test]
async fn checks_on_failed_requests_fail() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let result = run_plan(&format!(
        "options: {{ iterations: 2 }}\nflows:\n  default:\n    - request: {{ url: 'http://127.0.0.1:{port}/', checks: {{ ok: {{ status: 200 }} }} }}\n"
    ))
    .await;
    assert_eq!(rate(&result, result.builtins.checks), 0.0);
}

#[tokio::test]
async fn thresholds_are_evaluated_at_the_end() {
    let server = TestServer::start().await.unwrap();
    let result = run_plan(&format!(
        r#"
options:
  vus: 2
  iterations: 6
  thresholds:
    http_req_duration: ["p(95)<1000", "avg<10"]
    "http_reqs{{status:200}}": ["count==6"]
    http_req_failed: ["rate<0.01"]
flows:
  default:
    - request: {{ url: '{}' }}
"#,
        server.url("/delay/30")
    ))
    .await;
    let p95 = outcome(&result, "http_req_duration", "p(95)<1000");
    assert!(p95.passed, "{p95:?}");
    let avg = outcome(&result, "http_req_duration", "avg<10");
    assert!(!avg.passed && avg.observed >= 30.0, "{avg:?}");
    assert!(outcome(&result, "http_reqs{status:200}", "count==6").passed);
    assert!(outcome(&result, "http_req_failed", "rate<0.01").passed);
    assert!(result.thresholds_failed());
    assert!(!result.aborted_by_threshold);
}

#[tokio::test]
async fn abort_on_fail_stops_the_run_early() {
    let server = TestServer::start().await.unwrap();
    let started = Instant::now();
    let result = run_plan(&format!(
        r#"
options:
  vus: 2
  duration: 20s
  thresholds:
    http_req_failed: [{{ threshold: "rate<0.1", abortOnFail: true }}]
flows:
  default:
    - request: {{ url: '{}' }}
"#,
        server.url("/status/500")
    ))
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(1800),
        "{:?}",
        started.elapsed()
    );
    assert!(result.aborted_by_threshold);
    assert!(!result.stopped, "not a user stop");
    assert!(result.thresholds_failed());
}

#[tokio::test]
async fn delay_abort_eval_postpones_the_abort() {
    let server = TestServer::start().await.unwrap();
    let started = Instant::now();
    let result = run_plan(&format!(
        r#"
options:
  vus: 1
  duration: 20s
  thresholds:
    http_req_failed: [{{ threshold: "rate<0.1", abortOnFail: true, delayAbortEval: 2s }}]
flows:
  default:
    - request: {{ url: '{}' }}
"#,
        server.url("/status/500")
    ))
    .await;
    assert_between(
        "run",
        started.elapsed(),
        Duration::from_millis(2000),
        Duration::from_millis(3300),
    );
    assert!(result.aborted_by_threshold);
}

#[tokio::test]
async fn thresholds_on_unknown_metrics_or_wrong_aggregations_are_rejected() {
    let err = run(
        &plan("options: { thresholds: { http_req_latency: ['avg<1'] } }\nflows: { default: [] }\n"),
        RunControl::new(),
    )
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(
        err.contains("http_req_latency") && err.contains("no such metric"),
        "{err}"
    );

    let err = run(
        &plan(
            "options: { thresholds: { http_req_failed: ['p(95)<1'] } }\nflows: { default: [] }\n",
        ),
        RunControl::new(),
    )
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(err.contains("doesn't apply to a Rate metric"), "{err}");
}

#[tokio::test]
async fn outputs_receive_every_sample() {
    let server = TestServer::start().await.unwrap();
    let path = std::env::temp_dir().join(format!("k9-engine-json-{}.jsonl", std::process::id()));
    let output = k9_output::JsonOutput::create(&path, Default::default()).unwrap();
    let plan = plan(&format!(
        "options: {{ vus: 2, iterations: 10 }}\nflows:\n  default:\n    - request: {{ url: '{}', checks: {{ ok: {{ status: 200 }} }} }}\n",
        server.url("/ok")
    ));
    let result =
        k9_core::engine::run_with_outputs(&plan, RunControl::new(), vec![Box::new(output)])
            .await
            .unwrap();
    assert!(
        result.output_errors.is_empty(),
        "{:?}",
        result.output_errors
    );

    let lines: Vec<serde_json::Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let _ = std::fs::remove_file(&path);
    let points = |metric: &str| {
        lines
            .iter()
            .filter(|l| l["type"] == "Point" && l["metric"] == metric)
            .count()
    };
    assert_eq!(points("http_reqs"), 10);
    assert_eq!(points("http_req_duration"), 10);
    assert_eq!(points("iterations"), 10);
    assert_eq!(points("checks"), 10);
    let check = lines
        .iter()
        .find(|l| l["type"] == "Point" && l["metric"] == "checks")
        .unwrap();
    assert_eq!(check["data"]["tags"]["check"], "ok");
    assert_eq!(
        lines
            .iter()
            .filter(|l| l["type"] == "Metric" && l["metric"] == "http_reqs")
            .count(),
        1
    );
}

#[tokio::test]
async fn https_runs_over_http2_or_http3_and_tags_proto() {
    let server = TestServer::start().await.unwrap();
    let url = server.https_url("/ok");
    let run = |version: &'static str| {
        let url = url.clone();
        async move {
            run_plan(&format!(
                "options: {{ vus: 2, iterations: 10, insecureSkipTLSVerify: true, httpVersion: '{version}' }}\nflows:\n  default:\n    - request: {{ url: '{url}' }}\n"
            ))
            .await
        }
    };
    let result = run("auto").await;
    let b = result.builtins;
    assert_eq!(
        counter(&result, b.http_reqs, &[("proto", "HTTP/2.0")]),
        10.0
    );
    assert!(counter(&result, b.data_received, &[]) > 0.0);
    let result = run("1.1").await;
    assert_eq!(
        counter(&result, b.http_reqs, &[("proto", "HTTP/1.1")]),
        10.0
    );
    let result = run("3").await;
    assert_eq!(
        counter(&result, b.http_reqs, &[("proto", "HTTP/3.0")]),
        10.0
    );
    assert!(counter(&result, b.data_sent, &[]) > 0.0);
}
