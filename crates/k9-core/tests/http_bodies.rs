//! Request bodies in declarative flows: compression and multipart forms
//! (static or templated), and compressed responses.

use k9_core::engine::{EngineError, RunControl, RunResult, run};
use k9_core::{Format, Plan, load_plan};
use k9_metrics::Value;
use k9_testserver::TestServer;

/// `BASE` becomes the test server's URL, `DIR` a directory holding `a.txt`.
fn plan(yaml: &str, server: &TestServer, dir: &std::path::Path) -> Plan {
    let yaml = yaml
        .replace("BASE", &server.url(""))
        .replace("DIR", &dir.display().to_string());
    load_plan(&yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"))
}

fn check(result: &RunResult, name: &str) -> (u64, u64) {
    let m = result.aggregator.metrics();
    let id = m.registry().get("checks").unwrap();
    match result
        .aggregator
        .totals()
        .merged(id, &[("check", name)], m.interner())
    {
        Some(Value::Rate(r)) => (r.trues, r.total),
        other => panic!("{name}: {other:?}"),
    }
}

fn files() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-bodies-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("a.txt"), "file contents").unwrap();
    dir
}

#[tokio::test]
async fn bodies_are_compressed_and_forms_sent() {
    let server = TestServer::start().await.unwrap();
    let yaml = r#"
options: { vus: 2, iterations: 4 }
flows:
  default:
    - request:
        method: POST
        url: BASE/echo
        body: static hello
        compression: gzip, br
        checks: { static: { bodyContains: static hello } }
    - request:
        method: POST
        url: BASE/echo
        body: 'hello vu{{ __VU }}'
        compression: zstd
        checks: { templated: { bodyContains: hello vu } }
    - request:
        url: BASE/compressed/br
        checks: { decoded: { bodyContains: '"compressed":"br"' } }
        extract: { accepted: { json: '$.acceptEncoding' } }
    - request:
        method: POST
        url: BASE/multipart
        multipart:
          user: 'vu{{ __VU }} asked {{ accepted }}'
          doc: { file: DIR/a.txt, contentType: text/plain }
        checks:
          user: { bodyContains: '"user":"vu' }
          accepted: { bodyContains: 'asked gzip, deflate' }
          file: { bodyContains: '"content":"file contents"' }
    - request:
        method: POST
        url: BASE/multipart
        compression: gzip
        multipart:
          plain: fixed
          doc: { file: DIR/a.txt, filename: renamed.txt }
        checks: { status: { status: 400 } }
"#;
    let result = run(&plan(yaml, &server, &files()), RunControl::new())
        .await
        .unwrap();
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
    for name in ["static", "templated", "decoded", "user", "accepted", "file"] {
        assert_eq!(check(&result, name), (4, 4), "{name}");
    }
    // The test server's multipart parser doesn't undo Content-Encoding: a
    // compressed form arrives compressed, which shows it was compressed.
    assert_eq!(check(&result, "status"), (4, 4));
}

#[tokio::test]
async fn body_mistakes_are_reported_before_the_run_starts() {
    let server = TestServer::start().await.unwrap();
    let dir = files();
    for (step, expected) in [
        (
            "method: POST, url: BASE/echo, body: x, compression: lzma",
            "compression: unknown compression `lzma`",
        ),
        (
            "method: POST, url: BASE/echo, body: x, multipart: { a: b }",
            "body or multipart",
        ),
        (
            "method: POST, url: BASE/echo, multipart: { f: { file: DIR/nope.txt } }",
            "multipart `f`: reading",
        ),
        (
            "method: POST, url: BASE/echo, multipart: { a: '{{ nobody }}' }",
            "multipart `a`",
        ),
    ] {
        let yaml = format!(
            "options: {{ iterations: 1 }}\nflows:\n  default:\n    - request: {{ {step} }}\n"
        );
        let err = run(&plan(&yaml, &server, &dir), RunControl::new())
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
fn multipart_files_are_relative_to_the_plan_file() {
    let yaml = "flows:\n  default:\n    - request: { method: POST, url: 'http://h/', multipart: { f: { file: a.txt }, g: { file: /abs/b.txt }, t: text } }\n";
    let mut plan = load_plan(yaml, Format::Yaml).unwrap();
    let dir = std::path::Path::new("/plans");
    plan.rebase_files(dir);
    let json = serde_json::to_value(&plan.flows["default"]).unwrap();
    let form = &json[0]["request"]["multipart"];
    // Joined with the OS's separator (`/plans\a.txt` on Windows).
    assert_eq!(form["f"]["file"], dir.join("a.txt").display().to_string());
    assert_eq!(form["g"]["file"], "/abs/b.txt");
    assert_eq!(form["t"], "text");
}
