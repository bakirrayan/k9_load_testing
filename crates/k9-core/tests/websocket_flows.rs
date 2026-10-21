//! WebSocket steps in declarative flows: send, receive (with checks and
//! extractors), ping, sleep and close, all in Rust.

use k9_core::engine::{EngineError, RunControl, RunResult, run};
use k9_core::{Format, Plan, load_plan};
use k9_metrics::Value;
use k9_testserver::TestServer;

/// `WS` in `yaml` becomes the test server's `ws://` base URL.
fn plan(yaml: &str, server: &TestServer) -> Plan {
    let yaml = yaml.replace("WS", &server.url("").replacen("http", "ws", 1));
    load_plan(&yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"))
}

fn value(result: &RunResult, name: &str, filter: &[(&str, &str)]) -> Option<Value> {
    let m = result.aggregator.metrics();
    let id = m.registry().get(name).unwrap();
    result.aggregator.totals().merged(id, filter, m.interner())
}

fn count(result: &RunResult, name: &str) -> f64 {
    match value(result, name, &[]) {
        Some(Value::Counter(c)) => c.sum,
        Some(Value::Trend(t)) => t.count as f64,
        Some(Value::Rate(r)) => r.total as f64,
        None => 0.0,
        other => panic!("{other:?}"),
    }
}

/// (passed, total) for one check.
fn check(result: &RunResult, name: &str) -> (u64, u64) {
    match value(result, "checks", &[("check", name)]) {
        Some(Value::Rate(r)) => (r.trues, r.total),
        None => (0, 0),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_flow_chats_over_a_websocket() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options: { vus: 2, iterations: 4 }
flows:
  default:
    - websocket:
        url: WS/ws/echo
        name: chat
        tags: { room: a }
        checks: { upgraded: { status: 101 } }
        steps:
          - send: 'hello vu{{ __VU }}'
          - receive:
              timeout: 2s
              checks: { greeted: { bodyContains: hello } }
              extract: { who: { regex: 'hello (\w+)' } }
          - send: '{{ who }} again'
          - receive:
              checks: { again: { bodyContains: again }, quick: { durationBelow: 1s } }
          - ping
          - sleep: 10ms
          - close: { code: 1000, reason: done }
    - websocket:
        url: 'WS/ws/echo?user={{ who }}'
        steps:
          - send: 'still {{ who }}'
          - receive: { checks: { remembered: { bodyContains: still vu } } }
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(result.iterations_complete, 4);
    for name in ["upgraded", "greeted", "again", "quick", "remembered"] {
        assert_eq!(check(&result, name), (4, 4), "{name}");
    }
    assert_eq!(count(&result, "ws_sessions"), 8.0);
    assert_eq!(count(&result, "ws_msgs_sent"), 12.0);
    assert_eq!(count(&result, "ws_msgs_received"), 12.0);
    assert_eq!(count(&result, "ws_ping"), 4.0);
    assert_eq!(count(&result, "ws_session_duration"), 8.0);
    assert!(count(&result, "data_received") > 0.0);
    let tagged = value(&result, "ws_sessions", &[("name", "chat"), ("room", "a")]);
    assert!(
        matches!(tagged, Some(Value::Counter(c)) if c.sum == 4.0),
        "{tagged:?}"
    );
    assert!(
        result.script_errors.is_empty(),
        "{:?}",
        result.script_errors
    );
}

#[tokio::test]
async fn a_receive_that_gets_nothing_fails_its_checks_and_ends_the_session() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options: { iterations: 1 }
flows:
  default:
    - websocket:
        url: WS/ws/push/2
        steps:
          - receive: { checks: { first: { bodyContains: '1' } } }
          - receive: { checks: { second: { bodyContains: '2' } } }
          - receive: { checks: { third: { bodyContains: '3' } } }
          - receive: { checks: { never: { bodyContains: '4' } } }
    - websocket:
        url: WS/ws/echo
        steps:
          - receive: { timeout: 200ms, checks: { silent: { bodyContains: x } } }
          - send: unreached
    - sleep: 1ms
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(check(&result, "first"), (1, 1));
    assert_eq!(check(&result, "second"), (1, 1));
    assert_eq!(check(&result, "third"), (0, 1), "closed before a third");
    assert_eq!(check(&result, "never"), (0, 0), "the session ended there");
    assert_eq!(check(&result, "silent"), (0, 1));
    assert_eq!(
        count(&result, "ws_msgs_sent"),
        0.0,
        "nothing after the timeout"
    );
    let errors: Vec<&str> = result
        .request_errors
        .iter()
        .map(|(e, _)| e.as_str())
        .collect();
    assert!(
        errors.iter().any(|e| e.contains("closed")) && errors.iter().any(|e| e.contains("200ms")),
        "{errors:?}"
    );
    assert_eq!(result.iterations_complete, 1, "the flow goes on");
}

#[tokio::test]
async fn a_refused_websocket_fails_its_handshake_checks() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options: { iterations: 1 }
flows:
  default:
    - websocket:
        url: WS/status/404
        checks: { upgraded: { status: 101 } }
        steps: [ { send: hi } ]
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(check(&result, "upgraded"), (0, 1));
    assert_eq!(count(&result, "ws_sessions"), 0.0);
    assert_eq!(result.request_errors.len(), 1);
}

#[tokio::test]
async fn websocket_mistakes_are_reported_before_the_run_starts() {
    let server = TestServer::start().await.unwrap();
    for (steps, expected) in [
        (
            "url: 'http://x/', steps: []",
            "flows.default[0]: invalid URL",
        ),
        (
            "url: WS/ws/echo, steps: [ { receive: { checks: { s: { status: 200 } } } } ]",
            "steps[0]: check `s`",
        ),
        (
            "url: WS/ws/echo, steps: [ { receive: { extract: { a: { header: X } } } } ]",
            "steps[0]: extract `a`",
        ),
        (
            "url: WS/ws/echo, steps: [ { send: '{{ nobody }}' } ]",
            "steps[0]: `{{ nobody }}`",
        ),
        (
            "url: WS/ws/echo, checks: { b: { bodyContains: x } }, steps: []",
            "check `b`",
        ),
    ] {
        let yaml = format!(
            "options: {{ vus: 1, iterations: 1 }}\nflows:\n  default:\n    - websocket: {{ {steps} }}\n"
        );
        let err = run(&plan(&yaml, &server), RunControl::new())
            .await
            .err()
            .unwrap_or_else(|| panic!("{steps}: no error"));
        assert!(
            matches!(err, EngineError::InvalidRequest { .. }),
            "{steps}: {err:?}"
        );
        assert!(err.to_string().contains(expected), "{steps}: {err}");
    }
}
