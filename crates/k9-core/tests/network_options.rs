//! k6's network options in plans: `hosts`, `dns`, `blockHostnames`,
//! `blacklistIPs`, `localIPs`, `noConnectionReuse` and `noVUConnectionReuse`.

use k9_core::engine::{RunControl, RunResult, run};
use k9_core::{Format, Plan, load_plan};
use k9_metrics::Value;
use k9_testserver::TestServer;

/// `PORT` → the test server's HTTP port, `URL` → its base URL.
fn plan(yaml: &str, server: &TestServer) -> Plan {
    let yaml = yaml
        .replace("PORT", &server.http_addr().port().to_string())
        .replace("URL", &server.url(""));
    load_plan(&yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"))
}

/// (passed, total) for one check.
fn check(result: &RunResult, name: &str) -> (u64, u64) {
    let m = result.aggregator.metrics();
    let id = m.registry().get("checks").unwrap();
    match result
        .aggregator
        .totals()
        .merged(id, &[("check", name)], m.interner())
    {
        Some(Value::Rate(r)) => (r.trues, r.total),
        None => (0, 0),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn hosts_and_blocks_decide_where_requests_go() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options:
  iterations: 2
  hosts: { api.k9.test: 127.0.0.1, 'other.k9.test:80': '127.0.0.1:PORT', denied.k9.test: 127.0.0.1 }
  blockHostnames: ['denied.k9.test']
  dns: { ttl: 1m, select: roundRobin, policy: onlyIPv4 }
flows:
  default:
    - request: { url: 'http://api.k9.test:PORT/ok', checks: { api: { status: 200 } } }
    - request: { url: 'http://other.k9.test/ok', checks: { other: { status: 200 } } }
    - request: { url: 'http://denied.k9.test:PORT/ok' }
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(check(&result, "api"), (2, 2));
    assert_eq!(check(&result, "other"), (2, 2));
    let [(message, 2)] = &result.request_errors[..] else {
        panic!("{:?}", result.request_errors)
    };
    assert!(message.contains("denied.k9.test"), "{message}");

    let yaml = "options:\n  iterations: 1\n  blacklistIPs: [10.0.0.0/8, 127.0.0.0/8]\nflows:\n  default:\n    - request: { url: 'URL/ok' }\n";
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    let [(message, 1)] = &result.request_errors[..] else {
        panic!("{:?}", result.request_errors)
    };
    assert!(message.contains("127.0.0.0/8"), "{message}");
    assert_eq!(server.requests(), 4);
}

#[tokio::test]
async fn vus_take_turns_with_the_local_ips() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options:
  localIPs: 127.0.0.2-127.0.0.3
  scenarios:
    each: { executor: per-vu-iterations, vus: 3, iterations: 2 }
flows:
  default:
    - request:
        url: 'URL/ip'
        checks: { two: { bodyContains: 127.0.0.2 }, three: { bodyContains: 127.0.0.3 } }
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert!(
        result.request_errors.is_empty(),
        "{:?}",
        result.request_errors
    );
    assert_eq!(check(&result, "two"), (4, 6), "VUs 1 and 3");
    assert_eq!(check(&result, "three"), (2, 6), "VU 2");
}

#[tokio::test]
async fn connection_reuse_can_be_turned_off() {
    for (option, connections) in [
        ("", 1),
        ("noVUConnectionReuse: true", 3),
        ("noConnectionReuse: true", 6),
    ] {
        let server = TestServer::start().await.unwrap();
        let yaml = format!(
            "options:\n  iterations: 3\n  {option}\nflows:\n  default:\n    - request: {{ url: 'URL/ok' }}\n    - request: {{ url: 'URL/ok' }}\n"
        );
        let result = run(&plan(&yaml, &server), RunControl::new()).await.unwrap();
        assert!(
            result.request_errors.is_empty(),
            "{:?}",
            result.request_errors
        );
        assert_eq!(server.connections(), connections, "{option}");
    }
}

const FLOW: &str = "\nflows: { default: [{ request: { url: 'http://h/' } }] }";

#[test]
fn dns_takes_k6s_text_form_too() {
    let object = load_plan(
        &format!("options: {{ dns: {{ ttl: inf, select: first }} }}{FLOW}"),
        Format::Yaml,
    )
    .unwrap();
    let text = load_plan(
        &format!(
            "options: {{ dns: 'ttl=inf,select=first', localIPs: [10.0.0.1, 10.0.1.0/30] }}{FLOW}"
        ),
        Format::Yaml,
    )
    .unwrap();
    assert_eq!(object.options.http.dns, text.options.http.dns);
    let json = serde_json::to_value(&text.options.http).unwrap();
    assert_eq!(
        json["dns"],
        serde_json::json!({ "ttl": "inf", "select": "first", "policy": "preferIPv4" })
    );
    assert_eq!(
        json["localIPs"],
        serde_json::json!(["10.0.0.1", "10.0.1.0/30"])
    );
    let defaults = load_plan(FLOW, Format::Yaml).unwrap();
    let json = serde_json::to_value(&defaults.options.http).unwrap();
    assert_eq!(
        json["dns"],
        serde_json::json!({ "ttl": "5m", "select": "random", "policy": "preferIPv4" })
    );
}

#[test]
fn network_mistakes_are_reported() {
    for (options, expected) in [
        ("hosts: { a.test: nowhere }", "options.hosts.a.test"),
        ("hosts: { 'a.test:x': 127.0.0.1 }", "port"),
        ("dns: { policy: ipv4 }", "options.dns"),
        ("dns: 'ttl=soon'", "options.dns"),
        ("dns: 'colour=blue'", "colour"),
        ("blockHostnames: ['a.*.test']", "options.blockHostnames[0]"),
        ("blacklistIPs: [10.0.0.0/40]", "options.blacklistIPs[0]"),
        ("localIPs: '10.0.0.1,nope'", "options.localIPs[1]"),
    ] {
        let err = load_plan(&format!("options: {{ {options} }}{FLOW}"), Format::Yaml)
            .err()
            .unwrap_or_else(|| panic!("{options}: accepted"));
        assert!(err.to_string().contains(expected), "{options}: {err}");
    }
}
