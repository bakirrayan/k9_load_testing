//! Declarative flows with data: templates, extractors, feeders and generators
//!. Everything here runs in Rust, with no script.

use std::collections::BTreeMap;

use k9_core::engine::{EngineError, RunControl, RunResult, run, run_segment};
use k9_core::{Format, Plan, Segment, load_plan};
use k9_metrics::Value;
use k9_testserver::TestServer;

/// `BASE` in `yaml` becomes the test server's URL.
fn plan(yaml: &str, server: &TestServer) -> Plan {
    let yaml = yaml.replace("BASE", &server.url(""));
    load_plan(&yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"))
}

fn checks(result: &RunResult) -> (f64, f64) {
    let m = result.aggregator.metrics();
    match result
        .aggregator
        .totals()
        .merged(result.builtins.checks, &[], m.interner())
    {
        Some(Value::Rate(r)) => (r.trues as f64, r.total as f64),
        None => (0.0, 0.0),
        other => panic!("{other:?}"),
    }
}

fn users(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs.iter().map(|(u, n)| (u.to_string(), *n)).collect()
}

const LOGIN: &str = r#"
    - request:
        method: POST
        url: BASE/login
        headers:
          Content-Type: application/json
        body: '{"username": "{{ users.username }}", "password": "{{ users.password }}"}'
        extract:
          token: { json: $.token }
          request_id: { header: X-Request-Id }
    - request:
        url: BASE/me
        headers:
          Authorization: 'Bearer {{ token }}'
          X-Request-Id: '{{ request_id }}'
        checks:
          ok: { status: 200 }
"#;

#[tokio::test]
async fn a_login_flow_runs_with_each_vu_as_its_own_user() {
    let server = TestServer::start().await.unwrap();
    let yaml = format!(
        r#"
options:
  scenarios:
    s: {{ executor: per-vu-iterations, vus: 3, iterations: 4 }}
feeders:
  users:
    mode: per-vu
    rows:
      - {{ username: alice, password: secret }}
      - {{ username: bob, password: secret }}
      - {{ username: carol, password: secret }}
flows:
  default:{LOGIN}"#
    );
    let result = run(&plan(&yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(result.script_errors, vec![]);
    assert_eq!(checks(&result), (12.0, 12.0));
    assert_eq!(
        server.users(),
        users(&[("alice", 4), ("bob", 4), ("carol", 4)])
    );
}

#[tokio::test]
async fn a_sequential_csv_feeder_is_split_between_segments() {
    let server = TestServer::start().await.unwrap();
    let dir = std::env::temp_dir().join(format!("k9-flows-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let csv = dir.join("users.csv");
    std::fs::write(
        &csv,
        "username,password\r\nu1,secret\r\n\"u2\",secret\r\nu3,\"sec\"\"ret\"\r\nu4,secret\n",
    )
    .unwrap();
    let yaml = format!(
        r#"
options:
  scenarios:
    s: {{ executor: shared-iterations, vus: 2, iterations: 4 }}
feeders:
  users: {{ file: '{}' }}
flows:
  default:{LOGIN}"#,
        csv.display()
    );
    let plan = plan(&yaml, &server);
    let mut failed = 0.0;
    for segment in Segment::split(2) {
        let result = run_segment(&plan, segment, RunControl::new(), Vec::new(), None)
            .await
            .unwrap();
        failed += checks(&result).1 - checks(&result).0;
    }
    // u3's password is `sec"ret`: the quotes are CSV escaping, so it fails
    // to log in, and its iteration stops before `/me`.
    assert_eq!(failed, 0.0);
    assert_eq!(server.users(), users(&[("u1", 1), ("u2", 1), ("u4", 1)]));
}

#[tokio::test]
async fn an_extractor_that_finds_nothing_stops_the_iteration() {
    let server = TestServer::start().await.unwrap();
    let yaml = format!(
        r#"
options: {{ vus: 1, iterations: 2 }}
feeders:
  users:
    rows: [{{ username: mallory, password: wrong }}]
flows:
  default:{LOGIN}"#
    );
    let result = run(&plan(&yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(result.iterations_complete, 2);
    assert_eq!(server.users(), users(&[]), "/me is never requested");
    assert_eq!(checks(&result), (0.0, 0.0));
    assert_eq!(result.script_errors.len(), 1, "{:?}", result.script_errors);
    let (message, count) = &result.script_errors[0];
    assert_eq!(*count, 2);
    assert!(
        message.contains("flows.default[0]") && message.contains("`token`"),
        "{message}"
    );
}

#[tokio::test]
async fn generators_and_builtins_render_in_templates() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options:
  scenarios:
    s: { executor: per-vu-iterations, vus: 2, iterations: 3, env: { K9_FLOWS_TEST: from-env } }
flows:
  default:
    - request:
        method: POST
        url: 'BASE/echo?vu={{ __VU }}'
        body: 'id={{ uuid() }} n={{ randomInt(7, 7) }} s={{ randomString(12) }} t={{ timestamp() }} it={{ __ITER }} env={{ __ENV.K9_FLOWS_TEST }}'
        extract:
          id: { regex: 'id=([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}) ' }
          n: { regex: 'n=(\d+)' }
          s: { regex: 's=[A-Za-z0-9]{12} ' }
          t: { regex: 't=(1\d{12}) ' }
          it: { regex: 'it=(\d+)' }
          vu: { json: '$.query' }
          env: { json: "$['body']" }
    - request:
        url: 'BASE/expect-header/x-env/from-env'
        headers: { X-Env: '{{ __ENV.K9_FLOWS_TEST }}' }
        checks: { env: { status: 200 } }
    - request:
        url: 'BASE/expect-header/x-n/7'
        headers: { X-N: '{{ n }}' }
        checks: { n: { status: 200 } }
    - request:
        url: 'BASE/expect-header/x-vu/vu={{ __VU }}'
        headers: { X-VU: '{{ vu }}' }
        checks: { vu: { status: 200 } }
    - request:
        url: 'BASE/expect-header/x-iter/{{ it }}'
        headers: { X-Iter: '{{ __ITER }}' }
        checks: { iter: { status: 200 } }
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(result.script_errors, vec![]);
    assert_eq!(checks(&result), (24.0, 24.0));
}

#[tokio::test]
async fn templated_urls_are_tagged_by_their_template() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options: { vus: 1, iterations: 3 }
flows:
  default:
    - request: { url: 'BASE/delay/{{ randomInt(0, 2) }}', checks: { ok: { status: 200 } } }
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    let m = result.aggregator.metrics();
    let template = server.url("/delay/{{ randomInt(0, 2) }}");
    let reqs = result.aggregator.totals().merged(
        result.builtins.http_reqs,
        &[("name", &template), ("url", &template)],
        m.interner(),
    );
    assert!(
        matches!(reqs, Some(Value::Counter(c)) if c.sum == 3.0),
        "{reqs:?}"
    );
    assert_eq!(checks(&result), (3.0, 3.0));
}

#[tokio::test]
async fn mistakes_are_reported_before_the_run_starts() {
    let server = TestServer::start().await.unwrap();
    for (flow, expected) in [
        (
            "- request: { url: 'BASE/{{ token }}' }",
            "flows.default[0]: `{{ token }}`: no extractor sets `token`",
        ),
        (
            "- request: { url: 'BASE/{{ users.name }}' }",
            "no feeder named `users`",
        ),
        ("- request: { url: 'BASE/{{ uuid( }}' }", "`{{ uuid( }}`"),
        (
            "- request: { url: 'BASE/{{ randomInt(3, 1) }}' }",
            "randomInt",
        ),
        (
            "- request: { url: 'BASE/{{ nope() }}' }",
            "unknown function `nope`",
        ),
        ("- request: { url: 'BASE/{{ x' }", "unclosed `{{`"),
        (
            "- request: { url: 'BASE/', extract: { a: { json: 'token' } } }",
            "JSON path",
        ),
        (
            "- request: { url: 'BASE/', extract: { a: { regex: '(' } } }",
            "regex",
        ),
        (
            "- request: { url: 'BASE/', extract: { 'a.b': { header: X } } }",
            "`a.b`",
        ),
    ] {
        let yaml =
            format!("options: {{ vus: 1, iterations: 1 }}\nflows:\n  default:\n    {flow}\n");
        let err = run(&plan(&yaml, &server), RunControl::new())
            .await
            .err()
            .unwrap_or_else(|| panic!("{flow}: no error"));
        assert!(
            matches!(err, EngineError::InvalidRequest { .. }),
            "{flow}: {err:?}"
        );
        assert!(err.to_string().contains(expected), "{flow}: {err}");
    }
    let bad_feeders = [
        ("users: { rows: [] }", "feeder `users`: no rows"),
        (
            "users: { file: /nonexistent/users.csv }",
            "feeder `users`: /nonexistent/users.csv",
        ),
        (
            "users: { rows: [{ a: 1 }], file: x.csv }",
            "feeder `users`: give either `file` or `rows`",
        ),
    ];
    for (feeder, expected) in bad_feeders {
        let yaml = format!(
            "options: {{ vus: 1, iterations: 1 }}\nfeeders:\n  {feeder}\nflows:\n  default:\n    - request: {{ url: 'BASE/' }}\n"
        );
        let err = run(&plan(&yaml, &server), RunControl::new())
            .await
            .err()
            .unwrap_or_else(|| panic!("{feeder}: no error"));
        assert!(err.to_string().contains(expected), "{feeder}: {err}");
    }
    assert_eq!(server.requests(), 0);
}
