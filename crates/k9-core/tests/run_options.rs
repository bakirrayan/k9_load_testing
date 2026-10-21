//! k6's run options: `minIterationDuration`, `systemTags`, `noCookiesReset`,
//! `throw` and `summaryTimeUnit`.

use std::time::{Duration, Instant};

use k9_core::engine::{RunControl, RunResult, run};
use k9_core::{Format, Plan, load_plan};
use k9_metrics::Value;
use k9_testserver::TestServer;

/// `URL` → the test server's base URL.
fn plan(yaml: &str, server: &TestServer) -> Plan {
    let yaml = yaml.replace("URL", &server.url(""));
    load_plan(&yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"))
}

fn value(result: &RunResult, name: &str, filter: &[(&str, &str)]) -> Option<Value> {
    let m = result.aggregator.metrics();
    let id = m.registry().get(name).unwrap();
    result.aggregator.totals().merged(id, filter, m.interner())
}

fn count(result: &RunResult, name: &str, filter: &[(&str, &str)]) -> u64 {
    match value(result, name, filter) {
        Some(Value::Trend(t)) => t.count,
        Some(Value::Rate(r)) => r.total,
        Some(Value::Counter(c)) => c.sum as u64,
        None => 0,
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn short_iterations_are_padded_to_the_minimum() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options: { iterations: 3, minIterationDuration: 150ms }
flows:
  default:
    - request: { url: 'URL/ok' }
"#;
    let started = Instant::now();
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(450),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(result.iterations_complete, 3);
    let Some(Value::Trend(t)) = value(&result, "iteration_duration", &[]) else {
        panic!("no iteration_duration");
    };
    assert!(
        t.max >= 150.0,
        "the padding is part of the iteration: {}",
        t.max
    );
}

#[tokio::test]
async fn cookies_are_kept_across_iterations_with_no_cookies_reset() {
    let server = TestServer::start().await.unwrap();
    for (option, kept) in [("", 0), ("noCookiesReset: true", 2)] {
        let yaml = format!(
            r#"
options: {{ iterations: 3, {option} }}
flows:
  default:
    - request: {{ url: 'URL/cookies/has/session', checks: {{ kept: {{ status: 200 }} }} }}
    - request: {{ url: 'URL/cookies/set/session/1' }}
"#
        );
        let result = run(&plan(&yaml, &server), RunControl::new()).await.unwrap();
        let Some(Value::Rate(r)) = value(&result, "checks", &[("check", "kept")]) else {
            panic!("no checks");
        };
        assert_eq!((r.trues, r.total), (kept, 3), "{option}");
    }
}

#[tokio::test]
async fn system_tags_limit_the_tags_samples_carry() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options: { iterations: 2, systemTags: [status, check], tags: { team: k9 } }
flows:
  default:
    - request: { url: 'URL/ok', name: home, checks: { ok: { status: 200 } } }
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(count(&result, "http_req_duration", &[("status", "200")]), 2);
    assert_eq!(count(&result, "http_req_duration", &[("team", "k9")]), 2);
    for tag in [("name", "home"), ("method", "GET"), ("scenario", "default")] {
        assert_eq!(count(&result, "http_req_duration", &[tag]), 0, "{tag:?}");
    }
    assert_eq!(count(&result, "checks", &[("check", "ok")]), 2);
    assert_eq!(count(&result, "iterations", &[("scenario", "default")]), 0);
    assert_eq!(count(&result, "iterations", &[]), 2);
}

#[test]
fn run_options_are_validated_and_listed() {
    const FLOW: &str = "\nflows: { default: [{ request: { url: 'http://h/' } }] }";
    let plan = load_plan(
        &format!(
            "options: {{ minIterationDuration: 2s, throw: true, noCookiesReset: true, summaryTimeUnit: ms }}{FLOW}"
        ),
        Format::Yaml,
    )
    .unwrap();
    let json = serde_json::to_value(&plan.options).unwrap();
    assert_eq!(json["minIterationDuration"], "2s");
    assert_eq!(json["throw"], true);
    assert_eq!(json["noCookiesReset"], true);
    assert_eq!(json["summaryTimeUnit"], "ms");
    let defaults = serde_json::to_value(&load_plan(FLOW, Format::Yaml).unwrap().options).unwrap();
    let tags = defaults["systemTags"].as_array().unwrap();
    assert!(tags.contains(&"expected_response".into()) && !tags.contains(&"vu".into()));
    assert_eq!(defaults["throw"], false);

    for (options, expected) in [
        ("summaryTimeUnit: h", "options.summaryTimeUnit"),
        ("systemTags: [status, colour]", "options.systemTags[1]"),
        ("systemTags: [iter]", "isn't supported"),
        ("minIterationDuration: soon", "minIterationDuration"),
        ("httpDebug: bodies", "options.httpDebug"),
    ] {
        let err = load_plan(&format!("options: {{ {options} }}{FLOW}"), Format::Yaml)
            .err()
            .unwrap_or_else(|| panic!("{options}: accepted"));
        assert!(err.to_string().contains(expected), "{options}: {err}");
    }
}
