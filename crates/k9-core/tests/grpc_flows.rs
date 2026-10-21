//! gRPC steps in declarative flows: protos from files or server reflection,
//! templated messages and metadata, checks and extractors.

use k9_core::engine::{EngineError, RunControl, RunResult, run};
use k9_core::{Format, Plan, load_plan};
use k9_metrics::Value;
use k9_testserver::TestServer;

const PROTO: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../k9-testserver/proto/k9/test/echo.proto"
);

/// `TARGET` in `yaml` becomes the test server's `host:port`, `PROTO` its proto.
fn plan(yaml: &str, server: &TestServer) -> Plan {
    let yaml = yaml
        .replace("TARGET", &server.http_addr().to_string())
        .replace("PROTO", PROTO);
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
        None => 0,
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
async fn a_flow_calls_grpc_methods() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options: { vus: 2, iterations: 4 }
flows:
  default:
    - grpc:
        target: TARGET
        plaintext: true
        protos: [PROTO]
        method: k9.test.Echo/Say
        message: { text: 'hello vu{{ __VU }}', delayMs: 1 }
        metadata: { x-vu: '{{ __VU }}' }
        name: say
        tags: { room: a }
        timeout: 5s
        checks:
          ok: { status: 0 }
          greeted: { bodyContains: hello }
          quick: { durationBelow: 1s }
        extract:
          who: { json: '$.words[1]' }
          by: { header: x-served-by }
          code: { header: grpc-status }
    - grpc:
        target: TARGET
        plaintext: true
        method: k9.test.Echo/Say
        message: { text: '{{ who }} by {{ by }}, code {{ code }}' }
        checks:
          remembered: { bodyContains: 'vu' }
          served: { bodyContains: k9-testserver }
          trailer: { bodyContains: 'code 0' }
    - grpc:
        target: TARGET
        plaintext: true
        protos: [PROTO]
        method: k9.test.Echo/Say
        message: { text: denied, status: 7 }
        checks: { denied: { status: 7 }, fine: { status: 0 }, says: { bodyContains: denied } }
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert_eq!(result.iterations_complete, 4);
    assert!(
        result.script_errors.is_empty() && result.request_errors.is_empty(),
        "{:?} {:?}",
        result.script_errors,
        result.request_errors
    );
    for name in [
        "ok",
        "greeted",
        "quick",
        "remembered",
        "served",
        "trailer",
        "denied",
    ] {
        assert_eq!(check(&result, name), (4, 4), "{name}");
    }
    assert_eq!(check(&result, "fine"), (0, 4));
    assert_eq!(
        check(&result, "says"),
        (0, 4),
        "a failed call has no message"
    );
    assert_eq!(count(&result, "grpc_req_duration", &[]), 12);
    let say = [
        ("name", "say"),
        ("room", "a"),
        ("status", "0"),
        ("method", "/k9.test.Echo/Say"),
        ("rpc_type", "unary"),
        ("scenario", "default"),
    ];
    assert_eq!(count(&result, "grpc_req_duration", &say), 4);
    assert_eq!(count(&result, "grpc_req_duration", &[("status", "7")]), 4);
    assert_eq!(count(&result, "http_req_duration", &[]), 0);
}

#[tokio::test]
async fn grpc_mistakes_are_reported_before_the_run_starts() {
    let server = TestServer::start().await.unwrap();
    let refused = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string();
    let say = "target: TARGET, plaintext: true, protos: [PROTO], method: k9.test.Echo/Say";
    for (step, expected) in [
        (
            "target: TARGET, plaintext: true, protos: [PROTO], method: k9.test.Echo/Shout"
                .to_owned(),
            "flows.default[0]: unknown gRPC method",
        ),
        (
            "target: TARGET, protos: [nowhere.proto], method: a.B/C".to_owned(),
            "flows.default[0]: loading protos",
        ),
        (
            format!("target: '{refused}', plaintext: true, method: k9.test.Echo/Say, timeout: 2s"),
            "flows.default[0]: server reflection",
        ),
        (format!("{say}, message: {{ txt: hi }}"), "txt"),
        (
            "target: '{{ host }}', protos: [PROTO], method: k9.test.Echo/Say".to_owned(),
            "target: templates",
        ),
        (
            format!("{say}, message: {{ text: '{{{{ nobody }}}}' }}"),
            "`{{ nobody }}`",
        ),
        (
            format!("{say}, metadata: {{ 'bad key': x }}"),
            "metadata `bad key`",
        ),
    ] {
        let yaml = format!(
            "options: {{ vus: 1, iterations: 1 }}\nflows:\n  default:\n    - grpc: {{ {step} }}\n"
        );
        let err = run(&plan(&yaml, &server), RunControl::new())
            .await
            .err()
            .unwrap_or_else(|| panic!("{step}: no error"));
        assert!(
            matches!(err, EngineError::InvalidRequest { .. }),
            "{step}: {err:?}"
        );
        assert!(err.to_string().contains(expected), "{step}: {err}");
    }
}

#[test]
fn proto_paths_are_relative_to_the_plan_file() {
    let yaml = r#"
flows:
  default:
    - grpc: { target: 'h:1', method: a.B/C, protos: [a.proto, /abs/b.proto] }
    - group:
        name: g
        steps:
          - grpc: { target: 'h:1', method: a.B/C, protos: [c.proto], importPaths: [defs] }
"#;
    let mut plan = load_plan(yaml, Format::Yaml).unwrap();
    plan.rebase_files(std::path::Path::new("/plans"));
    let json = serde_json::to_value(&plan.flows["default"]).unwrap();
    assert_eq!(
        json[0]["grpc"]["protos"],
        serde_json::json!(["/plans/a.proto", "/abs/b.proto"])
    );
    let inner = &json[1]["group"]["steps"][0]["grpc"];
    assert_eq!(
        inner["protos"],
        serde_json::json!(["c.proto"]),
        "named within the import paths"
    );
    assert_eq!(inner["importPaths"], serde_json::json!(["/plans/defs"]));
}

#[tokio::test]
async fn a_flow_streams_messages() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options: { vus: 2, iterations: 4 }
flows:
  default:
    - grpc:
        target: TARGET
        plaintext: true
        protos: [PROTO]
        method: k9.test.Echo/Chat
        messages:
          - { text: 'one vu{{ __VU }}' }
          - { text: two }
        checks:
          ok: { status: 0 }
          both: { bodyContains: '"two"' }
          quick: { durationBelow: 1s }
        extract: { second: { json: '$[1].text' }, me: { regex: 'one (vu\d+)' } }
    - grpc:
        target: TARGET
        plaintext: true
        protos: [PROTO]
        method: k9.test.Echo/Chat
        message: { text: '{{ second }} {{ me }}' }
        checks: { carried: { bodyContains: 'two vu' } }
    - grpc:
        target: TARGET
        plaintext: true
        protos: [PROTO]
        method: k9.test.Echo/Chat
        messages: [{ text: fine }, { text: broken, status: 13 }]
        checks: { failed: { status: 13 }, partial: { bodyContains: fine } }
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    assert!(
        result.script_errors.is_empty() && result.request_errors.is_empty(),
        "{:?} {:?}",
        result.script_errors,
        result.request_errors
    );
    for name in ["ok", "both", "quick", "carried", "failed", "partial"] {
        assert_eq!(check(&result, name), (4, 4), "{name}");
    }
    let counter = |name, filter: &[(&str, &str)]| match value(&result, name, filter) {
        Some(Value::Counter(c)) => c.sum as u64,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        counter("grpc_streams", &[("rpc_type", "bidi_streaming")]),
        12
    );
    assert_eq!(counter("grpc_streams_msgs_sent", &[]), 20);
    assert_eq!(counter("grpc_streams_msgs_received", &[]), 16);
    assert_eq!(count(&result, "grpc_req_duration", &[]), 0);
}

#[tokio::test]
async fn messages_are_for_streams() {
    let server = TestServer::start().await.unwrap();
    let yaml = "options: { iterations: 1 }\nflows:\n  default:\n    - grpc: { target: TARGET, plaintext: true, protos: [PROTO], method: k9.test.Echo/Say, messages: [{}] }\n";
    let err = run(&plan(yaml, &server), RunControl::new())
        .await
        .err()
        .expect("an error");
    assert!(err.to_string().contains("messages"), "{err}");
    let yaml = "options: { iterations: 1 }\nflows:\n  default:\n    - grpc: { target: TARGET, plaintext: true, protos: [PROTO], method: k9.test.Echo/Chat, message: {}, messages: [{}] }\n";
    let err = run(&plan(yaml, &server), RunControl::new())
        .await
        .err()
        .expect("an error");
    assert!(err.to_string().contains("message or messages"), "{err}");
}

#[tokio::test]
async fn steps_compress_and_limit_message_sizes() {
    let server = TestServer::start().await.unwrap();
    let big = "k9 ".repeat(2000);
    let yaml = format!(
        r#"
options: {{ iterations: 2 }}
flows:
  default:
    - grpc:
        target: TARGET
        plaintext: true
        protos: [PROTO]
        method: k9.test.Echo/Say
        message: {{ text: '{big}' }}
        compression: gzip
        maxSendSize: 10000
        checks: {{ zipped: {{ status: 0 }}, echoed: {{ bodyContains: '"length":"6000"' }} }}
    - grpc:
        target: TARGET
        plaintext: true
        protos: [PROTO]
        method: k9.test.Echo/Say
        message: {{ text: '{big}' }}
        maxReceiveSize: 1000
        checks: {{ refused: {{ status: 8 }} }}
    - grpc:
        target: TARGET
        plaintext: true
        protos: [PROTO]
        method: k9.test.Echo/Say
        message: {{ text: '{big}' }}
        maxSendSize: 100
        checks: {{ unsent: {{ status: 8 }} }}
    - grpc:
        target: TARGET
        plaintext: true
        protos: [PROTO]
        method: k9.test.Echo/Chat
        messages: [{{ text: '{big}' }}]
        compression: gzip
        maxReceiveSize: 1000
        checks: {{ stream_refused: {{ status: 8 }} }}
"#
    );
    let result = run(&plan(&yaml, &server), RunControl::new()).await.unwrap();
    for name in ["zipped", "echoed", "refused", "unsent", "stream_refused"] {
        assert_eq!(check(&result, name), (2, 2), "{name}");
    }
    assert_eq!(count(&result, "grpc_req_duration", &[("status", "8")]), 4);
}

#[tokio::test]
async fn unknown_compression_is_reported_before_the_run() {
    let server = TestServer::start().await.unwrap();
    let yaml = "options: { iterations: 1 }\nflows:\n  default:\n    - grpc: { target: TARGET, plaintext: true, protos: [PROTO], method: k9.test.Echo/Say, compression: br }\n";
    let err = run(&plan(yaml, &server), RunControl::new())
        .await
        .err()
        .expect("an error");
    assert!(err.to_string().contains("compression `br`"), "{err}");
}

#[tokio::test]
async fn stream_durations_are_recorded() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options: { iterations: 2 }
flows:
  default:
    - grpc:
        target: TARGET
        plaintext: true
        protos: [PROTO]
        method: k9.test.Echo/Chat
        messages: [{ text: slow, delayMs: 120 }]
        name: chat
"#;
    let result = run(&plan(yaml, &server), RunControl::new()).await.unwrap();
    let filter = [("name", "chat"), ("rpc_type", "bidi_streaming")];
    match value(&result, "grpc_stream_duration", &filter) {
        Some(Value::Trend(t)) => {
            assert_eq!(t.count, 2);
            assert!(t.min >= 120.0, "{}", t.min);
        }
        other => panic!("{other:?}"),
    }
}
