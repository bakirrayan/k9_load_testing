//! `k9 convert`: a HAR recording into a plan, with values the server handed
//! out turned into extractors.

use std::path::PathBuf;
use std::process::{Command, Output};

use k9_testserver::TestServer;
use serde_json::json;

fn dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-convert-tests-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn k9(args: Vec<String>) -> Output {
    tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_k9"))
            .args(args)
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn header(name: &str, value: &str) -> serde_json::Value {
    json!({ "name": name, "value": value })
}

fn entry(
    page: &str,
    started: &str,
    request: serde_json::Value,
    status: u16,
    headers: Vec<serde_json::Value>,
    body: &str,
) -> serde_json::Value {
    json!({
        "pageref": page,
        "startedDateTime": started,
        "time": 20.0,
        "request": request,
        "response": {
            "status": status,
            "headers": headers,
            "content": { "mimeType": "application/json", "text": body },
        },
    })
}

/// A browser's recording of a login: the token it got (`tok-recorded`) is
/// sent back in later requests, and so is a request id from a header.
fn login_har(server: &TestServer) -> serde_json::Value {
    let get = |url: String, headers: Vec<serde_json::Value>| json!({ "method": "GET", "url": url, "headers": headers });
    json!({ "log": {
        "version": "1.2",
        "pages": [
            { "id": "page_1", "title": "Login" },
            { "id": "page_2", "title": "Profile" },
        ],
        "entries": [
            entry("page_1", "2026-10-01T10:00:00.000Z", json!({
                "method": "POST",
                "url": server.url("/login"),
                "headers": [
                    header(":authority", "localhost"),
                    header("Host", "localhost"),
                    header("Content-Length", "39"),
                    header("Content-Type", "application/json"),
                ],
                "postData": {
                    "mimeType": "application/json",
                    "text": r#"{"username":"alice","password":"secret"}"#,
                },
            }), 200, vec![header("x-request-id", "req-alice")],
                r#"{"token":"tok-recorded","user":{"name":"alice"}}"#),
            entry("page_1", "2026-10-01T10:00:00.050Z",
                get("https://www.google-analytics.com/collect?v=1".into(), vec![]),
                200, vec![], ""),
            entry("page_2", "2026-10-01T10:00:02.600Z", get(server.url("/me"), vec![
                header("Authorization", "Bearer tok-recorded"),
                header("X-Trace", "req-alice"),
            ]), 200, vec![], r#"{"name":"recorded"}"#),
            entry("page_2", "2026-10-01T10:00:02.700Z",
                get(server.url("/cookies/set/sid/abc?redirect=/ok"), vec![]),
                302, vec![header("Set-Cookie", "sid=abc; Path=/"), header("Location", "/ok")], ""),
            entry("page_2", "2026-10-01T10:00:02.750Z",
                get(server.url("/ok"), vec![header("Cookie", "sid=abc")]), 200, vec![], "ok"),
            entry("page_2", "2026-10-01T10:00:02.800Z",
                get(server.url("/cookies/has/sid"), vec![header("Cookie", "sid=abc")]),
                200, vec![], ""),
        ],
    }})
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recorded_login_converts_into_a_plan_that_runs() {
    let server = TestServer::start().await.unwrap();
    let har = dir().join("login.har");
    std::fs::write(&har, login_har(&server).to_string()).unwrap();
    let plan = dir().join("login.yaml");
    let out = k9(vec![
        "convert".into(),
        har.display().to_string(),
        "--skip".into(),
        "google-analytics.com".into(),
        "-O".into(),
        plan.display().to_string(),
    ])
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let yaml = std::fs::read_to_string(&plan).unwrap();
    // The token and the request id are taken from the responses they came in.
    assert!(yaml.contains("$.token"), "{yaml}");
    assert!(yaml.contains("Bearer {{ token }}"), "{yaml}");
    assert!(yaml.contains("{{ x_request_id }}"), "{yaml}");
    assert!(!yaml.contains("tok-recorded"), "{yaml}");
    // Pages become groups, the pause between them a sleep.
    assert!(yaml.contains("Login") && yaml.contains("Profile"), "{yaml}");
    assert!(yaml.contains("2.6s"), "{yaml}");
    // Skipped hosts, headers the client sets itself, the redirect k9 follows
    // and cookies its jar keeps are left out.
    assert!(!yaml.contains("google-analytics"), "{yaml}");
    assert!(!yaml.contains(":authority") && !yaml.contains("Content-Length"));
    assert!(!yaml.contains("sid=abc"), "{yaml}");
    assert_eq!(yaml.matches("/ok").count(), 1, "{yaml}");

    // It runs: /me is called with the token the server gave this run.
    let export = dir().join("summary.json");
    let out = k9(vec![
        "run".into(),
        plan.display().to_string(),
        "--summary-export".into(),
        export.display().to_string(),
    ])
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert_eq!(server.users().get("alice"), Some(&1));
    assert!(!server.users().contains_key("recorded"));
    let summary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&export).unwrap()).unwrap();
    assert_eq!(summary["metrics"]["checks"]["passes"], 4.0, "{summary}");
    assert_eq!(summary["metrics"]["checks"]["fails"], 0.0, "{summary}");
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_recordings_are_refused() {
    let bad = dir().join("bad.har");
    std::fs::write(&bad, r#"{"log": {"entries": "no"}}"#).unwrap();
    let out = k9(vec!["convert".into(), bad.display().to_string()]).await;
    assert_eq!(out.status.code(), Some(1), "{}", text(&out.stdout));
    assert!(
        text(&out.stderr).contains("bad.har"),
        "{}",
        text(&out.stderr)
    );
}
