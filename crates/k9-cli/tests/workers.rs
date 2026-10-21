//! `k9 run --workers N`: the real binary starting worker processes of itself
//!. Python scripts are tested in tests/python.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use k9_testserver::TestServer;

fn write_plan(name: &str, yaml: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-worker-tests-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.yaml"));
    std::fs::write(&path, yaml).unwrap();
    path
}

fn k9() -> Command {
    Command::new(env!("CARGO_BIN_EXE_k9"))
}

async fn run(args: Vec<String>) -> Output {
    tokio::task::spawn_blocking(move || k9().args(args).output().unwrap())
        .await
        .unwrap()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn args(plan: &Path, extra: &[&str]) -> Vec<String> {
    let mut args = vec!["run".to_owned(), plan.display().to_string()];
    args.extend(extra.iter().map(|s| s.to_string()));
    args
}

#[tokio::test(flavor = "multi_thread")]
async fn workers_share_a_plan_and_the_totals_stay_exact() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "exact",
        &format!(
            "options:\n  scenarios:\n    s: {{ executor: shared-iterations, vus: 5, iterations: 101 }}\n  thresholds:\n    checks: ['rate==1']\nflows:\n  default:\n    - request: {{ url: '{}', checks: {{ ok: {{ status: 200 }} }} }}\n",
            server.url("/ok")
        ),
    );
    let export = plan.with_extension("json");
    let out = run(args(
        &plan,
        &[
            "--workers",
            "3",
            "--summary-export",
            export.to_str().unwrap(),
        ],
    ))
    .await;
    let stdout = text(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{stdout}{}", text(&out.stderr));
    assert!(stdout.contains("execution: local (3 workers)"), "{stdout}");
    assert!(
        stdout.contains("101 complete and 0 interrupted iterations"),
        "{stdout}"
    );
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&export).unwrap()).unwrap();
    let m = &json["metrics"];
    assert_eq!(m["http_reqs"]["count"], 101.0);
    assert_eq!(m["iterations"]["count"], 101.0);
    assert_eq!(m["checks"]["passes"], 101);
    assert_eq!(m["vus_max"]["value"], 5.0);
    assert_eq!(server.requests(), 101);
}

#[tokio::test(flavor = "multi_thread")]
async fn thresholds_judge_every_workers_samples() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "failing",
        &format!(
            "options:\n  vus: 4\n  iterations: 40\n  thresholds:\n    http_req_failed: ['rate<0.01']\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/error-rate/10")
        ),
    );
    let out = run(args(&plan, &["--workers", "2"])).await;
    assert_eq!(out.status.code(), Some(99), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains("http_req_failed................: 10.00%"),
        "{}",
        text(&out.stdout)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_on_fail_stops_every_worker() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "abort",
        &format!(
            "options:\n  vus: 4\n  duration: 1m\n  thresholds:\n    http_req_failed:\n      - {{ threshold: 'rate<0.5', abortOnFail: true }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n    - sleep: 50ms\n",
            server.url("/status/500")
        ),
    );
    let started = Instant::now();
    let out = run(args(&plan, &["--workers", "2"])).await;
    assert_eq!(out.status.code(), Some(99), "{}", text(&out.stderr));
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "{:?}",
        started.elapsed()
    );
    assert!(
        text(&out.stderr).contains("abortOnFail"),
        "{}",
        text(&out.stderr)
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_c_on_the_manager_stops_the_workers() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "ctrl-c",
        &format!(
            "options: {{ vus: 4, duration: 1m }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n    - sleep: 20ms\n",
            server.url("/ok")
        ),
    );
    let child = k9()
        .args(args(&plan, &["--workers", "2"]))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let started = Instant::now();
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert_eq!(out.status.code(), Some(105), "{}", text(&out.stderr));
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(
        text(&out.stdout).contains("http_reqs"),
        "the summary still prints"
    );
    // The workers are gone: nothing is still sending requests.
    let before = server.requests();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(server.requests(), before);
}

#[test]
fn workers_need_a_count_and_dont_mix_with_segments() {
    let plan = write_plan(
        "flags",
        "flows:\n  default:\n    - request: { url: 'http://127.0.0.1:1/' }\n",
    );
    let out = k9()
        .args(args(&plan, &["--workers", "0"]))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "{}", text(&out.stderr));
    let out = k9()
        .args(args(
            &plan,
            &["--workers", "2", "--execution-segment", "0:1/2"],
        ))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(104));
    assert!(
        text(&out.stderr).contains("--workers and --agents run every segment themselves"),
        "{}",
        text(&out.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn feeder_files_are_found_next_to_the_plan_and_per_vu_rows_hold_across_workers() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "feeder",
        &format!(
            r#"options:
  scenarios:
    s: {{ executor: per-vu-iterations, vus: 4, iterations: 2 }}
feeders:
  users: {{ file: users.json, mode: per-vu }}
flows:
  default:
    - request:
        method: POST
        url: '{base}/login'
        body: '{{"username": "{{{{ users.name }}}}", "password": "secret"}}'
        extract: {{ token: {{ json: $.token }} }}
    - request:
        url: '{base}/me'
        headers: {{ Authorization: 'Bearer {{{{ token }}}}' }}
"#,
            base = server.url("")
        ),
    );
    std::fs::write(
        plan.with_file_name("users.json"),
        r#"[{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]"#,
    )
    .unwrap();
    // A bare file name, run from the plan's directory.
    let dir = plan.parent().unwrap().to_owned();
    let args = args(Path::new(plan.file_name().unwrap()), &["--workers", "2"]);
    let out =
        tokio::task::spawn_blocking(move || k9().args(args).current_dir(dir).output().unwrap())
            .await
            .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let users = server.users();
    assert_eq!(
        users.into_iter().collect::<Vec<_>>(),
        [("a", 2), ("b", 2), ("c", 2), ("d", 2)].map(|(u, n)| (u.to_owned(), n))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn outputs_get_every_workers_samples() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "outputs",
        &format!(
            "options:\n  scenarios:\n    s: {{ executor: shared-iterations, vus: 4, iterations: 30 }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/ok")
        ),
    );
    let (json, csv) = (plan.with_extension("jsonl"), plan.with_extension("csv"));
    let out = run(args(
        &plan,
        &[
            "--workers",
            "3",
            "--out",
            &format!("json={}", json.display()),
            "--out",
            &format!("csv={}", csv.display()),
            "--report",
            plan.with_extension("html").to_str().unwrap(),
        ],
    ))
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let points = |metric: &str| {
        std::fs::read_to_string(&json)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
            .filter(|l| l["type"] == "Point" && l["metric"] == metric)
            .count()
    };
    assert_eq!(points("http_reqs"), 30);
    assert_eq!(points("iterations"), 30);
    assert_eq!(points("http_req_duration"), 30);
    // `vus` comes from the manager alone (once per second, and at the end).
    assert!(points("vus") <= 3, "{}", points("vus"));
    let report = std::fs::read_to_string(plan.with_extension("html")).unwrap();
    assert!(report.contains("3 worker processes"), "the report");
    assert!(
        report.contains("<polyline"),
        "charts from the workers' ticks"
    );
    let rows = std::fs::read_to_string(&csv).unwrap();
    assert_eq!(
        rows.lines()
            .filter(|l| l.starts_with("iterations,"))
            .count(),
        30
    );
}
