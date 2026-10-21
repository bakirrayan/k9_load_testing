//! What a plan takes to agents: `inline_files` puts
//! feeder rows, `tlsAuth` PEMs, compiled `.proto` files and multipart files
//! in the plan, so it runs where none of the files exist.

use k9_core::engine::{RunControl, run};
use k9_core::{Format, Plan, load_plan};
use k9_metrics::Value;
use k9_testserver::TestServer;

#[tokio::test]
async fn an_inlined_plan_runs_without_its_files() {
    let server = TestServer::start().await.unwrap();
    let dir = std::env::temp_dir().join(format!("k9-bundle-{}", std::process::id()));
    std::fs::create_dir_all(dir.join("defs")).unwrap();
    let proto = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../k9-testserver/proto/k9/test/echo.proto"
    );
    std::fs::copy(proto, dir.join("echo.proto")).unwrap();
    std::fs::copy(proto, dir.join("defs/echo.proto")).unwrap();
    std::fs::write(dir.join("upload.bin"), [0u8, 159, 146, 150]).unwrap();
    let yaml = format!(
        r#"
options: {{ iterations: 2 }}
flows:
  default:
    - grpc:
        target: '{target}'
        plaintext: true
        protos: [echo.proto]
        method: k9.test.Echo/Say
        message: {{ text: hi }}
        checks: {{ said: {{ status: 0 }} }}
    - grpc:
        target: '{target}'
        plaintext: true
        protos: [echo.proto]
        importPaths: [defs]
        method: k9.test.Echo/Say
        message: {{ text: again }}
        checks: {{ imported: {{ status: 0 }} }}
    - request:
        method: POST
        url: '{url}/multipart'
        multipart: {{ doc: {{ file: upload.bin, contentType: application/x-raw }} }}
        checks: {{ uploaded: {{ bodyContains: upload.bin }}, raw: {{ bodyContains: "\"size\":4" }} }}
"#,
        target = server.http_addr(),
        url = server.url("")
    );
    let mut plan = load_plan(&yaml, Format::Yaml).unwrap();
    plan.rebase_files(&dir);
    plan.inline_files().unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    let json = serde_json::to_string(&plan).unwrap();
    assert!(
        json.contains("descriptorSet") && json.contains("\"data\""),
        "{json}"
    );
    let shipped: Plan = serde_json::from_str(&json).unwrap();

    let result = run(&shipped, RunControl::new()).await.unwrap();
    assert!(
        result.request_errors.is_empty() && result.script_errors.is_empty(),
        "{:?} {:?}",
        result.request_errors,
        result.script_errors
    );
    let m = result.aggregator.metrics();
    let id = m.registry().get("checks").unwrap();
    for name in ["said", "imported", "uploaded", "raw"] {
        match result
            .aggregator
            .totals()
            .merged(id, &[("check", name)], m.interner())
        {
            Some(Value::Rate(r)) => assert_eq!((r.trues, r.total), (2, 2), "{name}"),
            other => panic!("{name}: {other:?}"),
        }
    }
}

#[test]
fn missing_files_are_named() {
    let yaml = "flows:\n  default:\n    - grpc: { target: 'h:1', method: a.B/C, protos: [/nowhere/x.proto] }\n";
    let mut plan = load_plan(yaml, Format::Yaml).unwrap();
    let err = plan.inline_files().unwrap_err();
    assert!(
        err.contains("flows.default[0]") && err.contains("x.proto"),
        "{err}"
    );
    let yaml = "flows:\n  default:\n    - request: { method: POST, url: 'http://h/', multipart: { f: { file: /nowhere/y.bin } } }\n";
    let mut plan = load_plan(yaml, Format::Yaml).unwrap();
    let err = plan.inline_files().unwrap_err();
    assert!(err.contains("y.bin"), "{err}");
}
