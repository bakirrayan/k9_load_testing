//! `k9 new`: a starter plan or script, by tier. The
//! Python templates are run in tests/python/test_new.py.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use k9_testserver::TestServer;

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-new-tests-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn k9(cwd: &Path, args: &[&str]) -> Output {
    let (cwd, args) = (
        cwd.to_owned(),
        args.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
    );
    tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_k9"))
            .current_dir(cwd)
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

#[tokio::test(flavor = "multi_thread")]
async fn a_new_plan_runs_against_its_url() {
    let server = TestServer::start().await.unwrap();
    let dir = dir("plan");
    let url = server.url("/ok");
    let out = k9(&dir, &["new", "load.yaml", "--url", &url]).await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains("k9 run load.yaml"),
        "{}",
        text(&out.stdout)
    );
    let yaml = std::fs::read_to_string(dir.join("load.yaml")).unwrap();
    assert!(
        yaml.contains(&url) && yaml.contains("k9 run load.yaml"),
        "{yaml}"
    );

    let out = k9(&dir, &["inspect", "load.yaml"]).await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let inspection: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(inspection["tier"], "declarative");

    let out = k9(
        &dir,
        &[
            "run",
            "load.yaml",
            "--vus",
            "2",
            "--iterations",
            "4",
            "--summary-export",
            "s.json",
        ],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let summary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("s.json")).unwrap()).unwrap();
    assert_eq!(summary["metrics"]["checks"]["passes"], 4.0, "{summary}");
}

#[tokio::test(flavor = "multi_thread")]
async fn files_are_not_overwritten_without_force() {
    let dir = dir("force");
    // No file: an async script, `script.py`.
    let out = k9(&dir, &["new"]).await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let script = std::fs::read_to_string(dir.join("script.py")).unwrap();
    assert!(script.contains("async def default()"), "{script}");
    assert!(script.contains("http://localhost:8080/ok"), "{script}");

    std::fs::write(dir.join("script.py"), "mine").unwrap();
    let out = k9(&dir, &["new", "--template", "sync"]).await;
    assert_eq!(out.status.code(), Some(1));
    assert!(
        text(&out.stderr).contains("--force"),
        "{}",
        text(&out.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(dir.join("script.py")).unwrap(),
        "mine"
    );

    let out = k9(&dir, &["new", "--template", "sync", "--force"]).await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let script = std::fs::read_to_string(dir.join("script.py")).unwrap();
    assert!(script.contains("from k9.sync import"), "{script}");

    // `--template yaml` without a file: `plan.yaml`.
    let out = k9(&dir, &["new", "--template", "yaml"]).await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(dir.join("plan.yaml").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn mismatches_and_bad_urls_are_refused() {
    let dir = dir("bad");
    for (args, expected) in [
        (&["new", "x.yaml", "--template", "async"][..], "x.yaml"),
        (&["new", "x.py", "--template", "yaml"][..], "x.py"),
        (&["new", "x.txt"][..], "x.txt"),
        (&["new", "x.py", "--url", "localhost:8080"][..], "http"),
        (&["new", "x.py", "--url", "http://a.test/\"x"][..], "--url"),
    ] {
        let out = k9(&dir, args).await;
        assert_eq!(out.status.code(), Some(1), "{args:?}");
        assert!(
            text(&out.stderr).contains(expected),
            "{args:?}: {}",
            text(&out.stderr)
        );
        assert!(!dir.join("x.py").exists() && !dir.join("x.yaml").exists());
    }
}
