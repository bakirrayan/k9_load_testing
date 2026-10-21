//! `batch` steps: requests sent in parallel (k6's `http.batch`), within
//! the `batch` and `batchPerHost` limits.

use std::time::Duration;

use k9_core::engine::{RunControl, RunResult, run};
use k9_core::{Format, Plan, load_plan};
use k9_metrics::Value;
use k9_testserver::TestServer;

fn plan(yaml: &str, server: &TestServer) -> Plan {
    let yaml = yaml.replace("BASE", &server.url(""));
    load_plan(&yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"))
}

fn value(result: &RunResult, name: &str, filter: &[(&str, &str)]) -> Option<Value> {
    let m = result.aggregator.metrics();
    let id = m.registry().get(name).unwrap();
    result.aggregator.totals().merged(id, filter, m.interner())
}

/// The slowest iteration.
fn slowest(result: &RunResult) -> Duration {
    match value(result, "iteration_duration", &[]) {
        Some(Value::Trend(t)) => Duration::from_secs_f64(t.max / 1e3),
        other => panic!("{other:?}"),
    }
}

fn check(result: &RunResult, name: &str) -> (u64, u64) {
    match value(result, "checks", &[("check", name)]) {
        Some(Value::Rate(r)) => (r.trues, r.total),
        other => panic!("{name}: {other:?}"),
    }
}

fn requests(result: &RunResult) -> f64 {
    match value(result, "http_reqs", &[]) {
        Some(Value::Counter(c)) => c.sum,
        other => panic!("{other:?}"),
    }
}

const THREE_SLOW: &str = r#"
    - batch:
        - { url: 'BASE/delay/200', checks: { a: { status: 200 } } }
        - { url: 'BASE/delay/200?vu={{ __VU }}', checks: { b: { status: 200 } } }
        - { method: POST, url: 'BASE/login', body: '{"username": "u{{ __VU }}", "password": "secret"}',
            headers: { Content-Type: application/json },
            extract: { token: { json: '$.token' } } }
    - request: { url: 'BASE/me', headers: { Authorization: 'Bearer {{ token }}' },
                 checks: { me: { status: 200 } } }
"#;

#[tokio::test]
async fn a_batch_runs_its_requests_in_parallel() {
    let server = TestServer::start().await.unwrap();
    let yaml = format!("options: {{ vus: 2, iterations: 4 }}\nflows:\n  default:{THREE_SLOW}");
    let result = run(&plan(&yaml, &server), RunControl::new()).await.unwrap();
    assert!(
        result.request_errors.is_empty(),
        "{:?}",
        result.request_errors
    );
    assert!(
        result.script_errors.is_empty(),
        "{:?}",
        result.script_errors
    );
    for name in ["a", "b", "me"] {
        assert_eq!(check(&result, name), (4, 4), "{name}");
    }
    assert_eq!(requests(&result), 16.0);
    assert!(
        slowest(&result) < Duration::from_millis(390),
        "two 200 ms requests side by side: {:?}",
        slowest(&result)
    );
}

#[tokio::test]
async fn batch_limits_hold_requests_back() {
    let server = TestServer::start().await.unwrap();
    for (options, floor) in [
        ("batchPerHost: 1", 400),
        ("batch: 1", 400),
        ("batch: 2, batchPerHost: 0", 0),
    ] {
        let yaml =
            format!("options: {{ iterations: 1, {options} }}\nflows:\n  default:{THREE_SLOW}");
        let result = run(&plan(&yaml, &server), RunControl::new()).await.unwrap();
        assert_eq!(check(&result, "me"), (1, 1), "{options}");
        let took = slowest(&result);
        assert!(took >= Duration::from_millis(floor), "{options}: {took:?}");
        if floor == 0 {
            assert!(took < Duration::from_millis(390), "{options}: {took:?}");
        }
    }
}

#[tokio::test]
async fn a_failed_batch_request_is_recorded_and_its_extractor_stops_the_iteration() {
    let server = TestServer::start().await.unwrap();
    let refused = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let yaml = format!(
        r#"
options: {{ iterations: 2 }}
flows:
  default:
    - batch:
        - {{ url: 'http://{refused}/', checks: {{ down: {{ status: 0 }} }} }}
        - {{ url: 'BASE/ok', extract: {{ missing: {{ json: '$.nope' }} }}, checks: {{ up: {{ status: 200 }} }} }}
        - {{ url: 'BASE/ok', checks: {{ also: {{ status: 200 }} }} }}
    - request: {{ url: 'BASE/ok', checks: {{ unreached: {{ status: 200 }} }} }}
"#
    );
    let result = run(&plan(&yaml, &server), RunControl::new()).await.unwrap();
    for name in ["down", "up", "also"] {
        assert_eq!(check(&result, name), (2, 2), "{name}");
    }
    assert!(value(&result, "checks", &[("check", "unreached")]).is_none());
    assert_eq!(requests(&result), 6.0);
    assert_eq!(result.request_errors.iter().map(|(_, n)| n).sum::<u64>(), 2);
    assert!(
        result
            .script_errors
            .iter()
            .any(|(e, _)| e.contains("missing")),
        "{:?}",
        result.script_errors
    );
}
