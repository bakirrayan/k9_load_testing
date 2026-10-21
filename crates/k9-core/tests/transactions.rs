//! Weighted transactions with onStart/onStop, from Goose.

use std::collections::BTreeMap;

use k9_core::engine::{RunControl, RunResult, run};
use k9_core::{Format, Plan, load_plan};
use k9_metrics::Value;
use k9_testserver::TestServer;

fn plan(yaml: &str, server: &TestServer) -> Plan {
    let yaml = yaml.replace("BASE", &server.url(""));
    load_plan(&yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"))
}

fn count(result: &RunResult, metric: &str, filter: &[(&str, &str)]) -> f64 {
    let m = result.aggregator.metrics();
    let id = m.registry().get(metric).unwrap();
    match result.aggregator.totals().merged(id, filter, m.interner()) {
        Some(Value::Counter(c)) => c.sum,
        Some(Value::Trend(t)) => t.count as f64,
        None => 0.0,
        other => panic!("{other:?}"),
    }
}

/// A flow per name, each requesting `/ok` under its own request name.
fn flows(names: &[&str]) -> String {
    names
        .iter()
        .map(|n| format!("  {n}:\n    - request: {{ url: 'BASE/ok', name: {n} }}\n"))
        .collect()
}

#[tokio::test]
async fn each_vu_runs_the_exact_mix_of_weights() {
    let server = TestServer::start().await.unwrap();
    let yaml = format!(
        "options:\n  scenarios:\n    shop:\n      executor: per-vu-iterations\n      vus: 2\n      iterations: 18\n      transactions: {{ browse: 5, search: 3, checkout: 1 }}\nflows:\n{}",
        flows(&["browse", "search", "checkout"])
    );
    let result = run(&plan(&yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(result.iterations_complete, 36);
    for (name, n) in [("browse", 20.0), ("search", 12.0), ("checkout", 4.0)] {
        assert_eq!(count(&result, "http_reqs", &[("name", name)]), n, "{name}");
        assert_eq!(
            count(&result, "iterations", &[("transaction", name)]),
            n,
            "{name}"
        );
        assert_eq!(
            count(&result, "iteration_duration", &[("transaction", name)]),
            n
        );
    }
}

#[tokio::test]
async fn on_start_and_on_stop_run_once_per_vu_around_its_iterations() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options:
  scenarios:
    s:
      executor: per-vu-iterations
      vus: 3
      iterations: 2
      onStart: login
      onStop: logout
      transactions: { me: 1 }
feeders:
  users:
    mode: per-vu
    rows: [{ name: tx-a }, { name: tx-b }, { name: tx-c }]
flows:
  login:
    - request:
        method: POST
        url: BASE/login
        name: login
        body: '{"username": "{{ users.name }}", "password": "secret"}'
        extract: { token: { json: $.token } }
  me:
    - request:
        url: BASE/me
        headers: { Authorization: 'Bearer {{ token }}' }
        checks: { ok: { status: 200 } }
  logout:
    - request: { url: 'BASE/ok', name: logout }
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(result.script_errors, vec![]);
    assert_eq!(
        result.iterations_complete, 6,
        "onStart/onStop aren't iterations"
    );
    assert_eq!(count(&result, "iterations", &[]), 6.0);
    assert_eq!(count(&result, "http_reqs", &[("name", "login")]), 3.0);
    assert_eq!(count(&result, "http_reqs", &[("name", "logout")]), 3.0);
    let users: BTreeMap<String, u64> = [("tx-a", 2), ("tx-b", 2), ("tx-c", 2)]
        .map(|(u, n)| (u.to_owned(), n))
        .into();
    assert_eq!(server.users(), users);
}

#[tokio::test]
async fn on_stop_runs_when_a_duration_ends_but_not_when_a_vu_is_interrupted() {
    let server = TestServer::start().await.unwrap();
    let yaml = "options:\n  scenarios:\n    s: { executor: constant-vus, vus: 2, duration: 300ms, onStop: bye }\nflows:\n  default:\n    - request: { url: 'BASE/ok', name: work }\n    - sleep: 50ms\n  bye:\n    - request: { url: 'BASE/ok', name: bye }\n";
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(count(&result, "http_reqs", &[("name", "bye")]), 2.0);

    // Interrupted mid-iteration (gracefulStop: 0): no onStop.
    let yaml = "options:\n  scenarios:\n    s: { executor: constant-vus, vus: 2, duration: 200ms, gracefulStop: 0s, onStop: bye }\nflows:\n  default:\n    - request: { url: 'BASE/delay/1000' }\n  bye:\n    - request: { url: 'BASE/ok', name: bye }\n";
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(result.iterations_interrupted, 2);
    assert_eq!(count(&result, "http_reqs", &[("name", "bye")]), 0.0);
}

#[test]
fn transactions_are_validated() {
    let base = "flows:\n  a: []\n  b: []\n";
    for (scenario, path, expected) in [
        (
            "transactions: { a: 1 }, exec: b",
            "options.scenarios.s.exec",
            "not both",
        ),
        (
            "transactions: { a: 0 }",
            "options.scenarios.s.transactions.a",
            "at least 1",
        ),
        (
            "transactions: { c: 1 }",
            "options.scenarios.s.transactions.c",
            "no flow named `c`",
        ),
        (
            "transactions: {}",
            "options.scenarios.s.transactions",
            "at least one",
        ),
        (
            "transactions: { a: 9000, b: 1001 }",
            "options.scenarios.s.transactions",
            "more than 10000",
        ),
        (
            "exec: a, onStart: x",
            "options.scenarios.s.onStart",
            "no flow named `x`",
        ),
        (
            "exec: a, onStop: y",
            "options.scenarios.s.onStop",
            "no flow named `y`",
        ),
    ] {
        let yaml = format!(
            "options:\n  scenarios:\n    s: {{ executor: shared-iterations, {scenario} }}\n{base}"
        );
        let err = load_plan(&yaml, Format::Yaml)
            .err()
            .unwrap_or_else(|| panic!("{scenario}: no error"))
            .to_string();
        assert!(
            err.contains(&format!("{path}: ")) && err.contains(expected),
            "{scenario}: {err}"
        );
    }
    // Transactions stand in for `exec`: no `default` flow is needed.
    let yaml = format!(
        "options:\n  scenarios:\n    s: {{ executor: shared-iterations, transactions: {{ a: 2, b: 1 }}, onStart: a }}\n{base}"
    );
    let plan = load_plan(&yaml, Format::Yaml).unwrap();
    let s = &plan.options.scenarios["s"];
    assert_eq!(s.transactions.get("a"), Some(&2));
    assert_eq!(s.on_start.as_deref(), Some("a"));
}
