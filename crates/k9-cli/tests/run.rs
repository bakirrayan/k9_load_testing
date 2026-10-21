//! `k9 run`, end to end: the real binary against k9-testserver.

use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use k9_testserver::TestServer;

fn write_plan(name: &str, yaml: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-tests-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.yaml"));
    std::fs::write(&path, yaml).unwrap();
    path
}

fn k9() -> Command {
    Command::new(env!("CARGO_BIN_EXE_k9"))
}

/// Runs the binary off the async runtime so the test server keeps serving.
async fn k9_run(plan: PathBuf) -> Output {
    tokio::task::spawn_blocking(move || k9().arg("run").arg(plan).output().unwrap())
        .await
        .unwrap()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn run_prints_header_status_and_summary() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "summary",
        &format!(
            "options: {{ vus: 2, duration: 1s }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/delay/20")
        ),
    );
    let out = k9_run(plan.clone()).await;
    let stdout = text(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout:\n{stdout}\nstderr:\n{}",
        text(&out.stderr)
    );
    for expected in [
        "execution: local",
        &format!("script: {}", plan.display()),
        "scenarios: (100.00%) 1 scenario, 2 max VUs, 31s max duration (incl. graceful stop):",
        "* default: 2 looping VUs for 1s (gracefulStop: 30s)",
        "complete and 0 interrupted iterations",
        "http_req_duration.",
        "{ expected_response:true }",
        "http_reqs.",
        "iterations.",
        "vus_max.",
    ] {
        assert!(
            stdout.contains(expected),
            "missing {expected:?} in:\n{stdout}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_requests_are_reported_on_stderr() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let plan = write_plan(
        "errors",
        &format!(
            "options: {{ iterations: 2 }}\nflows:\n  default:\n    - request: {{ url: 'http://127.0.0.1:{port}/' }}\n"
        ),
    );
    let out = k9_run(plan).await;
    assert_eq!(out.status.code(), Some(0));
    let stderr = text(&out.stderr);
    assert!(stderr.contains("WARN") && stderr.contains("2×"), "{stderr}");
    assert!(stderr.contains("could not connect"), "{stderr}");
    assert!(
        text(&out.stdout).contains("100.00%"),
        "http_req_failed is 100%"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_plans_exit_104() {
    let bad = write_plan("bad", "options: { vu: 1 }\n");
    let out = k9_run(bad).await;
    assert_eq!(out.status.code(), Some(104));
    assert!(text(&out.stderr).contains("unknown field `vu`"));
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_c_stops_the_run_and_still_prints_the_summary() {
    use std::process::Stdio;

    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "ctrlc",
        &format!(
            "options: {{ vus: 2, duration: 30s }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/delay/5000")
        ),
    );
    let started = Instant::now();
    let out = tokio::task::spawn_blocking(move || {
        let child = k9()
            .arg("run")
            .arg(plan)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_millis(700));
        let status = Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
        child.wait_with_output().unwrap()
    })
    .await
    .unwrap();

    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        out.status.code(),
        Some(105),
        "k6's code for an external abort"
    );
    let stdout = text(&out.stdout);
    assert!(
        stdout.contains("0 complete and 2 interrupted iterations"),
        "{stdout}"
    );
    assert!(
        text(&out.stderr).contains("Ctrl-C again"),
        "{}",
        text(&out.stderr)
    );
}

// --- checks, thresholds and outputs --------------------------------

async fn k9_run_args(plan: PathBuf, args: &[&str]) -> Output {
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    tokio::task::spawn_blocking(move || k9().arg("run").arg(plan).args(&args).output().unwrap())
        .await
        .unwrap()
}

fn threshold_plan(server: &TestServer, name: &str, threshold: &str) -> PathBuf {
    write_plan(
        name,
        &format!(
            r#"
options:
  iterations: 3
  thresholds:
    http_req_duration: ["{threshold}"]
flows:
  default:
    - request:
        url: '{}'
        checks:
          "status is 200": {{ status: 200 }}
          "is teapot": {{ status: 418 }}
"#,
            server.url("/delay/20")
        ),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn crossed_thresholds_exit_99_and_are_marked() {
    let server = TestServer::start().await.unwrap();
    let out = k9_run_args(threshold_plan(&server, "crossed", "avg<1"), &[]).await;
    let (stdout, stderr) = (text(&out.stdout), text(&out.stderr));
    assert_eq!(out.status.code(), Some(99), "{stdout}\n{stderr}");
    assert!(stdout.contains("✗ http_req_duration."), "{stdout}");
    assert!(
        stderr.contains("thresholds on metrics 'http_req_duration' have been crossed"),
        "{stderr}"
    );

    let passing = k9_run_args(threshold_plan(&server, "passing", "avg<5000"), &[]).await;
    assert_eq!(passing.status.code(), Some(0));
    assert!(text(&passing.stdout).contains("✓ http_req_duration."));

    let skipped = k9_run_args(
        threshold_plan(&server, "skipped", "avg<1"),
        &["--no-thresholds"],
    )
    .await;
    assert_eq!(skipped.status.code(), Some(0));
    assert!(!text(&skipped.stdout).contains("✗ http_req_duration"));
}

#[tokio::test(flavor = "multi_thread")]
async fn checks_are_summarised_before_metrics() {
    let server = TestServer::start().await.unwrap();
    let out = k9_run_args(threshold_plan(&server, "checks", "avg<5000"), &[]).await;
    let stdout = text(&out.stdout);
    let pass = stdout.find("✓ status is 200").expect(&stdout);
    let fail = stdout.find("✗ is teapot").expect(&stdout);
    let metrics = stdout.find("checks.....").expect(&stdout);
    assert!(pass < metrics && fail < metrics, "{stdout}");
    assert!(stdout.contains("↳  0% — ✓ 0 / ✗ 3"), "{stdout}");
    assert!(stdout.contains("50.00%"), "checks rate:\n{stdout}");
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_on_fail_exits_99_early() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "abort",
        &format!(
            "options:\n  vus: 1\n  duration: 30s\n  thresholds:\n    http_req_failed: [{{ threshold: 'rate<0.5', abortOnFail: true }}]\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/status/500")
        ),
    );
    let started = Instant::now();
    let out = k9_run_args(plan, &[]).await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(out.status.code(), Some(99));
    assert!(
        text(&out.stderr).contains("abortOnFail"),
        "{}",
        text(&out.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn json_output_and_summary_export_are_written() {
    let server = TestServer::start().await.unwrap();
    let dir = std::env::temp_dir().join(format!("k9-cli-outputs-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let jsonl = dir.join("samples.jsonl");
    let summary = dir.join("summary.json");
    let plan = threshold_plan(&server, "outputs", "avg<5000");
    let out = k9_run_args(
        plan,
        &[
            "--out",
            &format!("json={}", jsonl.display()),
            "--summary-export",
            &summary.display().to_string(),
        ],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains(&format!("output: json ({})", jsonl.display())),
        "{}",
        text(&out.stdout)
    );

    let points = std::fs::read_to_string(&jsonl)
        .unwrap()
        .lines()
        .filter(|l| l.contains(r#""type":"Point""#) && l.contains(r#""metric":"http_reqs""#))
        .count();
    assert_eq!(points, 3);

    let export: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&summary).unwrap()).unwrap();
    assert_eq!(export["metrics"]["http_reqs"]["count"], 3.0);
    assert_eq!(
        export["metrics"]["http_req_duration"]["thresholds"]["avg<5000"], false,
        "k6: false = not crossed"
    );
    assert_eq!(export["root_group"]["checks"].as_array().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_or_incomplete_outputs_exit_104() {
    let plan = write_plan(
        "no-output",
        "options: { iterations: 1 }\nflows: { default: [] }\n",
    );
    for (arg, message) in [("carrier-pigeon", "unknown output"), ("json", "json=")] {
        let out = k9_run_args(plan.clone(), &["--out", arg]).await;
        assert_eq!(out.status.code(), Some(104), "{arg}");
        assert!(
            text(&out.stderr).contains(message),
            "{arg}: {}",
            text(&out.stderr)
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn execution_segments_split_a_run_into_parts_that_add_up() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "segments",
        &format!(
            "options:\n  scenarios:\n    s: {{ executor: shared-iterations, vus: 5, iterations: 101 }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/ok")
        ),
    );
    let mut total = 0.0;
    for (i, segment) in ["0:1/3", "1/3:2/3", "2/3:1"].into_iter().enumerate() {
        let export = plan.with_file_name(format!("segment-{i}.json"));
        let args: Vec<String> = [
            "run",
            plan.to_str().unwrap(),
            "--execution-segment",
            segment,
            "--execution-segment-sequence",
            "0,1/3,2/3,1",
            "--summary-export",
            export.to_str().unwrap(),
        ]
        .map(String::from)
        .to_vec();
        let out = tokio::task::spawn_blocking(move || k9().args(args).output().unwrap())
            .await
            .unwrap();
        let stdout = text(&out.stdout);
        assert_eq!(out.status.code(), Some(0), "{stdout}{}", text(&out.stderr));
        let max_vus = [2, 2, 1][i];
        assert!(
            stdout.contains(&format!(
                "scenarios: (33.33%) 1 scenario, {max_vus} max VUs"
            )),
            "{stdout}"
        );
        assert!(
            stdout.contains(&format!("execution: local (segment {segment})")),
            "{stdout}"
        );
        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&export).unwrap()).unwrap();
        total += json["metrics"]["http_reqs"]["count"].as_f64().unwrap();
    }
    assert_eq!(total, 101.0, "the parts add up to the whole test");
}

#[test]
fn a_segment_must_belong_to_its_sequence() {
    let plan = write_plan(
        "bad-segment",
        "flows:\n  default:\n    - request: { url: 'http://127.0.0.1:1/' }\n",
    );
    for (args, message) in [
        (
            vec!["--execution-segment", "1/2:1/4"],
            "invalid execution segment `1/2:1/4`",
        ),
        (
            vec![
                "--execution-segment",
                "1/4:1/2",
                "--execution-segment-sequence",
                "0,1/3,1",
            ],
            "execution segment 1/4:1/2 isn't one of the sequence 0,1/3,1 (its segments: 0:1/3, 1/3:1)",
        ),
        (
            vec!["--execution-segment-sequence", "0,1/2,1"],
            "--execution-segment-sequence needs --execution-segment",
        ),
    ] {
        let out = k9().arg("run").arg(&plan).args(&args).output().unwrap();
        assert_eq!(out.status.code(), Some(104), "{args:?}");
        assert!(
            text(&out.stderr).contains(message),
            "{args:?}: {}",
            text(&out.stderr)
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn summary_trend_stats_choose_the_trend_columns() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "trend-stats",
        &format!(
            "options:\n  iterations: 3\n  summaryTrendStats: [avg, p(99), count]\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/ok")
        ),
    );
    let export = plan.with_extension("json");
    let out = k9_run_args(
        plan.clone(),
        &["--summary-export", export.to_str().unwrap()],
    )
    .await;
    let stdout = text(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let line = stdout
        .lines()
        .find(|l| l.contains("http_req_duration"))
        .unwrap();
    assert!(
        line.contains("p(99)=") && line.contains("count=3"),
        "{line}"
    );
    assert!(!line.contains("p(90)=") && !line.contains("med="), "{line}");
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&export).unwrap()).unwrap();
    let dur = &json["metrics"]["http_req_duration"];
    assert!(dur["p(99)"].is_number() && dur["count"] == 3.0, "{dur}");
    assert!(dur.get("p(95)").is_none(), "{dur}");

    // The flag beats the plan; bad stats are refused.
    let out = k9_run_args(plan.clone(), &["--summary-trend-stats", "min,p(99.9)"]).await;
    let stdout = text(&out.stdout);
    let line = stdout
        .lines()
        .find(|l| l.contains("http_req_duration"))
        .unwrap();
    assert!(line.contains("min=") && line.contains("p(99.9)="), "{line}");
    assert!(!line.contains("avg="), "{line}");
    let out = k9_run_args(plan, &["--summary-trend-stats", "avg,p(101)"]).await;
    assert_eq!(out.status.code(), Some(104));
    assert!(
        text(&out.stderr).contains("p(101)"),
        "{}",
        text(&out.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_output_writes_k6s_columns() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "csv",
        &format!(
            "options:\n  iterations: 2\n  tags: {{ team: 'a,b' }}\nflows:\n  default:\n    - request: {{ url: '{}', checks: {{ ok: {{ status: 200 }} }} }}\n",
            server.url("/ok")
        ),
    );
    let csv = plan.with_extension("csv");
    let out = k9_run_args(plan, &["--out", &format!("csv={}", csv.display())]).await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains("output: csv ("),
        "{}",
        text(&out.stdout)
    );
    let body = std::fs::read_to_string(&csv).unwrap();
    let mut lines = body.lines();
    assert_eq!(
        lines.next().unwrap(),
        "metric_name,timestamp,metric_value,check,error,error_code,expected_response,group,method,name,proto,scenario,service,status,subproto,tls_version,url,extra_tags,metadata"
    );
    let rows: Vec<&str> = lines.collect();
    let reqs: Vec<&&str> = rows
        .iter()
        .filter(|r| r.starts_with("http_reqs,"))
        .collect();
    assert_eq!(reqs.len(), 2, "{body}");
    let url = server.url("/ok");
    let fields: Vec<&str> = reqs[0].split(',').collect();
    assert!(
        fields[1].parse::<u64>().unwrap() > 1_600_000_000,
        "unix seconds"
    );
    assert_eq!(fields[2], "1.000000");
    assert_eq!(fields[6], "true");
    assert_eq!(fields[8], "GET");
    assert_eq!(fields[13], "200");
    assert_eq!(fields[16], url);
    // A value with a comma is quoted.
    assert!(reqs[0].ends_with(",\"team=a,b\","), "{}", reqs[0]);
    let check = rows.iter().find(|r| r.starts_with("checks,")).unwrap();
    assert_eq!(check.split(',').nth(3), Some("ok"));
}

#[tokio::test(flavor = "multi_thread")]
async fn report_writes_a_self_contained_html_page() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "report",
        &format!(
            "options:\n  vus: 3\n  duration: 2500ms\n  thresholds:\n    http_req_duration: ['p(95)<1']\n    checks: ['rate==1']\nflows:\n  default:\n    - group:\n        name: api\n        steps:\n          - request: {{ url: '{}', checks: {{ 'is <ok>': {{ status: 200 }} }} }}\n    - sleep: 50ms\n",
            server.url("/delay/5")
        ),
    );
    let report = plan.with_extension("html");
    let out = k9_run_args(plan, &["--report", report.to_str().unwrap()]).await;
    assert_eq!(out.status.code(), Some(99), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains(&format!("report: {}", report.display())),
        "{}",
        text(&out.stdout)
    );
    let html = std::fs::read_to_string(&report).unwrap();
    assert!(html.starts_with("<!doctype html>"));
    assert!(html.contains("report.yaml"), "the script");
    assert!(html.contains("Thresholds crossed"));
    assert!(html.contains("p(95)&lt;1"), "thresholds, escaped");
    assert!(html.contains("rate==1"));
    assert!(html.contains("is &lt;ok&gt;"), "checks, escaped");
    assert!(html.contains("::api"), "check groups");
    assert!(html.contains("http_req_duration") && html.contains("iteration_duration"));
    assert!(html.matches("<svg").count() >= 4, "charts");
    assert!(
        html.contains("data:image/png;base64,"),
        "the logo is inline"
    );
    // Nothing to fetch: it opens offline.
    for external in ["<script src", "<link", "url(http", "src=\"http"] {
        assert!(!html.contains(external), "{external}");
    }
}

#[tokio::test]
async fn summary_time_unit_fixes_the_unit() {
    let server = TestServer::start().await.unwrap();
    let plan = threshold_plan(&server, "time-unit", "avg<5000");
    let out = k9_run_args(plan, &["--summary-time-unit", "us"]).await;
    let stdout = text(&out.stdout);
    let line = stdout
        .lines()
        .find(|l| l.contains("http_req_duration."))
        .expect(&stdout);
    assert!(line.contains("µs") && !line.contains("ms "), "{line}");
}

// --- output flags ---------------------------------------------------

fn echo_plan(server: &TestServer, name: &str) -> PathBuf {
    write_plan(
        name,
        &format!(
            "options: {{ iterations: 1 }}\nflows:\n  default:\n    - request: {{ method: POST, url: '{}', body: 'hello k9', headers: {{ X-Probe: yes }} }}\n",
            server.url("/echo?q=1")
        ),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn http_debug_prints_requests_and_responses() {
    let server = TestServer::start().await.unwrap();
    let out = k9_run_args(echo_plan(&server, "debug-full"), &["--http-debug=full"]).await;
    let stderr = text(&out.stderr);
    assert_eq!(out.status.code(), Some(0), "{stderr}");
    let request = stderr
        .find("Request:\nPOST /echo?q=1 HTTP/1.1\n")
        .expect(&stderr);
    let response = stderr.find("Response:\nHTTP/1.1 200 OK\n").expect(&stderr);
    assert!(request < response, "{stderr}");
    assert!(stderr.contains("x-probe: yes"), "{stderr}");
    assert_eq!(
        stderr.matches("hello k9").count(),
        2,
        "both bodies:\n{stderr}"
    );

    let out = k9_run_args(echo_plan(&server, "debug-headers"), &["--http-debug"]).await;
    let stderr = text(&out.stderr);
    assert!(stderr.contains("Response:\nHTTP/1.1 200 OK\n"), "{stderr}");
    assert!(!stderr.contains("hello k9"), "headers only:\n{stderr}");

    let out = k9_run_args(echo_plan(&server, "debug-off"), &[]).await;
    assert!(!text(&out.stderr).contains("Request:"));
}

#[tokio::test(flavor = "multi_thread")]
async fn no_summary_skips_the_summary_and_its_export() {
    let server = TestServer::start().await.unwrap();
    let export = std::env::temp_dir().join(format!("k9-no-summary-{}.json", std::process::id()));
    let _ = std::fs::remove_file(&export);
    let plan = echo_plan(&server, "no-summary");
    let out = k9_run_args(
        plan.clone(),
        &["--no-summary", "--summary-export", export.to_str().unwrap()],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let stdout = text(&out.stdout);
    assert!(!stdout.contains("http_reqs"), "{stdout}");
    assert!(!export.exists(), "no summary export either");

    let out = k9_run_args(plan, &["--quiet"]).await;
    assert!(
        text(&out.stdout).contains("http_reqs"),
        "--quiet keeps the summary"
    );
}
