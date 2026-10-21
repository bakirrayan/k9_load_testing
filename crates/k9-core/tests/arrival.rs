//! Arrival-rate executors against k9-testserver.

use std::time::{Duration, Instant};

use k9_core::engine::{RunControl, RunResult, run, run_segment};
use k9_core::{Format, Plan, Segment, load_plan};
use k9_metrics::{MetricId, TrendValue, Value};
use k9_testserver::TestServer;

fn plan(scenario: &str, url: &str) -> Plan {
    let yaml = format!(
        "options:\n  scenarios:\n    s: {{ {scenario} }}\nflows:\n  default:\n    - request: {{ url: '{url}' }}\n"
    );
    load_plan(&yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"))
}

fn metric(result: &RunResult, name: &str) -> MetricId {
    result.aggregator.metrics().registry().get(name).unwrap()
}

fn count(result: &RunResult, name: &str) -> f64 {
    let m = result.aggregator.metrics();
    match result
        .aggregator
        .totals()
        .merged(metric(result, name), &[], m.interner())
    {
        Some(Value::Counter(c)) => c.sum,
        Some(Value::Trend(t)) => t.count as f64,
        None => 0.0,
        other => panic!("{name}: {other:?}"),
    }
}

fn trend(result: &RunResult, name: &str) -> TrendValue {
    let m = result.aggregator.metrics();
    match result
        .aggregator
        .totals()
        .merged(metric(result, name), &[], m.interner())
    {
        Some(Value::Trend(t)) => t,
        other => panic!("{name}: {other:?}"),
    }
}

#[tokio::test]
async fn constant_arrival_rate_holds_its_rate() {
    let server = TestServer::start().await.unwrap();
    let plan = plan(
        "executor: constant-arrival-rate, rate: 100, timeUnit: 1s, duration: 2s, preAllocatedVUs: 5, maxVUs: 20",
        &server.url("/ok"),
    );
    let started = Instant::now();
    let result = run(&plan, RunControl::new()).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(2500),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(result.iterations_complete, 200);
    assert_eq!(count(&result, "http_reqs"), 200.0);
    assert_eq!(count(&result, "dropped_iterations"), 0.0);
    assert_eq!(count(&result, "iteration_duration_corrected"), 200.0);
    assert_eq!(result.max_vus, 20);
}

#[tokio::test]
async fn ramping_arrival_rate_follows_its_stages() {
    let server = TestServer::start().await.unwrap();
    let plan = plan(
        "executor: ramping-arrival-rate, startRate: 0, timeUnit: 1s, preAllocatedVUs: 5, maxVUs: 20, stages: [{ duration: 1s, target: 100 }, { duration: 1s, target: 100 }]",
        &server.url("/ok"),
    );
    let result = run(&plan, RunControl::new()).await.unwrap();
    assert_eq!(result.iterations_complete, 150);
}

#[tokio::test]
async fn busy_vus_drop_iterations_and_every_due_iteration_is_counted() {
    let server = TestServer::start().await.unwrap();
    let plan = plan(
        "executor: constant-arrival-rate, rate: 50, timeUnit: 1s, duration: 1s, preAllocatedVUs: 2, maxVUs: 2",
        &server.url("/delay/200"),
    );
    let result = run(&plan, RunControl::new()).await.unwrap();
    let dropped = count(&result, "dropped_iterations");
    assert_eq!(result.iterations_complete as f64 + dropped, 50.0);
    assert!(
        dropped >= 35.0,
        "2 VUs × 200 ms can't keep up with 50/s: {dropped} dropped"
    );
    assert_eq!(count(&result, "iteration_duration_corrected"), 50.0);
    assert!(
        trend(&result, "iteration_duration_corrected").max
            > trend(&result, "iteration_duration").max
    );
}

#[tokio::test]
async fn a_stall_shows_in_the_corrected_metric() {
    let server = TestServer::start().await.unwrap();
    let plan = plan(
        "executor: constant-arrival-rate, rate: 40, timeUnit: 1s, duration: 2s, preAllocatedVUs: 4, maxVUs: 4",
        &server.url("/ok"),
    );
    let stall = async {
        tokio::time::sleep(Duration::from_millis(400)).await;
        server.stall();
        tokio::time::sleep(Duration::from_millis(1000)).await;
        server.resume();
    };
    let (result, ()) = tokio::join!(run(&plan, RunControl::new()), stall);
    let result = result.unwrap();
    let raw = trend(&result, "iteration_duration");
    let corrected = trend(&result, "iteration_duration_corrected");
    assert_eq!(corrected.count, 80, "every due iteration");
    // Measured from when they actually ran, most iterations look fast: the
    // stall only held up the few that were in flight.
    assert!(
        raw.percentile(80.0) < 100.0,
        "raw p(80) = {}",
        raw.percentile(80.0)
    );
    // Measured from when they were due, the stall shows.
    assert!(
        corrected.percentile(80.0) > 300.0,
        "corrected p(80) = {}",
        corrected.percentile(80.0)
    );
    assert!(count(&result, "dropped_iterations") >= 30.0);
}

#[tokio::test]
async fn segments_split_the_arrivals_exactly() {
    let server = TestServer::start().await.unwrap();
    let plan = plan(
        "executor: constant-arrival-rate, rate: 90, timeUnit: 1s, duration: 1s, preAllocatedVUs: 1, maxVUs: 1",
        &server.url("/ok"),
    );
    let mut total = 0;
    for segment in Segment::split(3) {
        let result = run_segment(&plan, segment, RunControl::new(), Vec::new(), None)
            .await
            .unwrap();
        // Each part gets a VU for its arrivals even though maxVUs is 1.
        assert_eq!(count(&result, "dropped_iterations"), 0.0);
        total += result.iterations_complete;
    }
    assert_eq!(total, 90);
}
