//! `k9 run --agents N` and `k9 agent`: a test split across machines (here,
//! processes in other directories).

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use k9_testserver::TestServer;

const TOKEN: &str = "s3cret-agent-token";

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-agents-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Starts the manager and reads its header up to the agents' address.
// The caller waits for the child.
#[allow(clippy::zombie_processes)]
fn manager(args: &[&str]) -> (Child, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_k9"))
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        if stdout.read_line(&mut line).unwrap() == 0 {
            let output = child.wait_with_output().unwrap();
            panic!("no agents line: {}", text(&output.stderr));
        }
        if let Some((_, rest)) = line.split_once(" agents on ") {
            let address = rest.trim().trim_end_matches(')').to_owned();
            let rest = std::thread::spawn(move || {
                let mut all = String::new();
                while stdout.read_line(&mut all).unwrap_or(0) > 0 {}
                all
            });
            // The rest of stdout is collected when the manager exits.
            std::mem::forget(rest);
            return (child, address);
        }
    }
}

fn agent(manager: &str, extra: &[&str], cwd: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_k9"))
        .args(["agent", "--manager", manager, "--token", TOKEN, "--once"])
        .args(extra)
        .current_dir(cwd)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn finish(child: Child, within: Duration) -> Output {
    let started = Instant::now();
    let pid = child.id();
    let waiter = std::thread::spawn(move || child.wait_with_output().unwrap());
    while !waiter.is_finished() {
        if started.elapsed() > within {
            let _ = Command::new("kill").arg(pid.to_string()).status();
            panic!("process {pid} still running after {within:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    waiter.join().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn agents_split_a_plan_and_the_manager_merges_their_results() {
    let server = TestServer::start().await.unwrap();
    let work = dir("split");
    std::fs::write(work.join("codes.csv"), "code\n200\n201\n").unwrap();
    let plan = work.join("plan.yaml");
    std::fs::write(
        &plan,
        format!(
            "options:\n  scenarios:\n    s: {{ executor: shared-iterations, vus: 4, iterations: 40 }}\nfeeders:\n  codes: {{ file: codes.csv }}\nflows:\n  default:\n    - request: {{ url: '{}/status/{{{{ codes.code }}}}', checks: {{ ok: {{ status: [200, 201] }} }} }}\n",
            server.url("")
        ),
    )
    .unwrap();
    let export = work.join("summary.json");
    let elsewhere = dir("elsewhere");
    let before = server.requests();
    let output = tokio::task::spawn_blocking(move || {
        let (manager, address) = manager(&[
            "run",
            &plan.display().to_string(),
            "--agents",
            "2",
            "--listen",
            "127.0.0.1:0",
            "--agent-token",
            TOKEN,
            "--summary-export",
            &export.display().to_string(),
        ]);
        // The agents can't read the manager's files: the plan's feeder travels inline.
        std::fs::remove_file(work.join("codes.csv")).unwrap();
        let agents = [
            agent(&address, &[], &elsewhere),
            agent(&address, &[], &elsewhere),
        ];
        let output = finish(manager, Duration::from_secs(30));
        for agent in agents {
            let done = finish(agent, Duration::from_secs(20));
            assert_eq!(done.status.code(), Some(0), "{}", text(&done.stderr));
        }
        (output, std::fs::read_to_string(export).unwrap())
    })
    .await
    .unwrap();
    let (output, export) = output;
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let export: serde_json::Value = serde_json::from_str(&export).unwrap();
    assert_eq!(export["metrics"]["iterations"]["count"], 40.0);
    assert_eq!(export["metrics"]["checks"]["passes"], 40.0);
    assert_eq!(server.requests() - before, 40);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_token_is_refused_and_the_agent_says_so() {
    let work = dir("token");
    let plan = work.join("plan.yaml");
    std::fs::write(
        &plan,
        "options: { iterations: 1 }\nflows: { default: [] }\n",
    )
    .unwrap();
    tokio::task::spawn_blocking(move || {
        let (mut manager, address) = manager(&[
            "run",
            &plan.display().to_string(),
            "--agents",
            "1",
            "--listen",
            "127.0.0.1:0",
            "--agent-token",
            TOKEN,
        ]);
        let intruder = Command::new(env!("CARGO_BIN_EXE_k9"))
            .args(["agent", "--manager", &address, "--token", "guess", "--once"])
            .output()
            .unwrap();
        assert_eq!(intruder.status.code(), Some(104));
        assert!(
            text(&intruder.stderr).contains("refused this worker's token"),
            "{}",
            text(&intruder.stderr)
        );
        // The manager keeps waiting for a real agent.
        assert!(manager.try_wait().unwrap().is_none());
        let real = agent(&address, &[], &work);
        let output = finish(manager, Duration::from_secs(30));
        assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
        finish(real, Duration::from_secs(20));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agent_that_disappears_mid_run_fails_the_run() {
    let server = TestServer::start().await.unwrap();
    let work = dir("crash");
    let plan = work.join("plan.yaml");
    std::fs::write(
        &plan,
        format!(
            "options: {{ vus: 2, duration: 30s }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/delay/20")
        ),
    )
    .unwrap();
    let output = tokio::task::spawn_blocking(move || {
        let (manager, address) = manager(&[
            "run",
            &plan.display().to_string(),
            "--agents",
            "2",
            "--listen",
            "127.0.0.1:0",
            "--agent-token",
            TOKEN,
        ]);
        let mut doomed = agent(&address, &[], &work);
        let survivor = agent(&address, &[], &work);
        std::thread::sleep(Duration::from_millis(1500));
        doomed.kill().unwrap();
        let _ = doomed.wait();
        let output = finish(manager, Duration::from_secs(20));
        finish(survivor, Duration::from_secs(20));
        output
    })
    .await
    .unwrap();
    assert_eq!(output.status.code(), Some(103), "{}", text(&output.stdout));
    let stderr = text(&output.stderr);
    assert!(stderr.contains("disconnected"), "{stderr}");
}

#[tokio::test(flavor = "multi_thread")]
async fn agents_reach_a_manager_over_tls() {
    let work = dir("tls");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    std::fs::write(work.join("cert.pem"), cert.cert.pem()).unwrap();
    std::fs::write(work.join("key.pem"), cert.signing_key.serialize_pem()).unwrap();
    let plan = work.join("plan.yaml");
    std::fs::write(
        &plan,
        "options: { iterations: 4 }\nflows: { default: [] }\n",
    )
    .unwrap();
    let output = tokio::task::spawn_blocking(move || {
        let (manager, address) = manager(&[
            "run",
            &plan.display().to_string(),
            "--agents",
            "1",
            "--listen",
            "127.0.0.1:0",
            "--agent-token",
            TOKEN,
            "--tls-cert",
            &work.join("cert.pem").display().to_string(),
            "--tls-key",
            &work.join("key.pem").display().to_string(),
        ]);
        let port = address.rsplit(':').next().unwrap();
        let url = format!("https://localhost:{port}");
        // Without the CA, the agent can't trust the manager.
        let untrusting = Command::new(env!("CARGO_BIN_EXE_k9"))
            .args(["agent", "--manager", &url, "--token", TOKEN, "--once"])
            .output()
            .unwrap();
        assert_ne!(untrusting.status.code(), Some(0));
        assert!(
            text(&untrusting.stderr).contains("certificate"),
            "{}",
            text(&untrusting.stderr)
        );
        let ca = work.join("cert.pem").display().to_string();
        let trusting = agent(&url, &["--tls-ca", &ca], &work);
        let output = finish(manager, Duration::from_secs(30));
        let agent = finish(trusting, Duration::from_secs(20));
        assert_eq!(agent.status.code(), Some(0), "{}", text(&agent.stderr));
        output
    })
    .await
    .unwrap();
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
}

#[test]
fn agents_and_workers_dont_mix() {
    let work = dir("mix");
    let plan = work.join("plan.yaml");
    std::fs::write(
        &plan,
        "options: { iterations: 1 }\nflows: { default: [] }\n",
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_k9"))
        .args([
            "run",
            &plan.display().to_string(),
            "--agents",
            "2",
            "--workers",
            "2",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(104));
    assert!(
        text(&output.stderr).contains("--agents"),
        "{}",
        text(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn agents_get_proto_and_upload_files_with_the_plan() {
    let server = TestServer::start().await.unwrap();
    let work = dir("bundle");
    let proto = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../k9-testserver/proto/k9/test/echo.proto"
    );
    std::fs::copy(proto, work.join("echo.proto")).unwrap();
    std::fs::write(work.join("upload.txt"), "shipped upload").unwrap();
    let plan = work.join("plan.yaml");
    std::fs::write(
        &plan,
        format!(
            r#"
options:
  scenarios:
    s: {{ executor: shared-iterations, vus: 2, iterations: 6 }}
flows:
  default:
    - grpc:
        target: '{target}'
        plaintext: true
        protos: [echo.proto]
        method: k9.test.Echo/Say
        message: {{ text: hi }}
        checks: {{ said: {{ status: 0 }} }}
    - request:
        method: POST
        url: '{url}/multipart'
        multipart: {{ doc: {{ file: upload.txt }} }}
        checks: {{ uploaded: {{ bodyContains: shipped upload }} }}
"#,
            target = server.http_addr(),
            url = server.url("")
        ),
    )
    .unwrap();
    let export = work.join("summary.json");
    let elsewhere = dir("bundle-elsewhere");
    let output = tokio::task::spawn_blocking(move || {
        let (manager, address) = manager(&[
            "run",
            &plan.display().to_string(),
            "--agents",
            "2",
            "--listen",
            "127.0.0.1:0",
            "--agent-token",
            TOKEN,
            "--summary-export",
            &export.display().to_string(),
        ]);
        // The agents can't read the manager's files: they come with the plan.
        std::fs::remove_file(work.join("echo.proto")).unwrap();
        std::fs::remove_file(work.join("upload.txt")).unwrap();
        let agents = [
            agent(&address, &[], &elsewhere),
            agent(&address, &[], &elsewhere),
        ];
        let output = finish(manager, Duration::from_secs(30));
        for agent in agents {
            let done = finish(agent, Duration::from_secs(20));
            assert_eq!(done.status.code(), Some(0), "{}", text(&done.stderr));
        }
        (output, std::fs::read_to_string(export).unwrap_or_default())
    })
    .await
    .unwrap();
    let (output, export) = output;
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    let export: serde_json::Value = serde_json::from_str(&export).unwrap();
    let checks = export["root_group"]["checks"].as_array().unwrap();
    for name in ["said", "uploaded"] {
        let check = checks.iter().find(|c| c["name"] == name).expect(name);
        assert_eq!(
            (&check["passes"], &check["fails"]),
            (&6.into(), &0.into()),
            "{name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agent_runs_several_workers_and_shares_follow_weights() {
    let server = TestServer::start().await.unwrap();
    let work = dir("weighted");
    let plan = work.join("plan.yaml");
    std::fs::write(
        &plan,
        format!(
            "options:\n  scenarios:\n    s: {{ executor: shared-iterations, vus: 6, iterations: 30 }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/ok")
        ),
    )
    .unwrap();
    let export = work.join("summary.json");
    let elsewhere = dir("weighted-elsewhere");
    let before = server.requests();
    let (output, export) = tokio::task::spawn_blocking(move || {
        let (manager, address) = manager(&[
            "run",
            &plan.display().to_string(),
            "--agents",
            "2",
            "--listen",
            "127.0.0.1:0",
            "--agent-token",
            TOKEN,
            "--summary-export",
            &export.display().to_string(),
        ]);
        let agents = [
            agent(&address, &["--workers", "2"], &elsewhere),
            agent(&address, &["--weight", "2"], &elsewhere),
        ];
        let output = finish(manager, Duration::from_secs(30));
        for agent in agents {
            let done = finish(agent, Duration::from_secs(20));
            assert_eq!(done.status.code(), Some(0), "{}", text(&done.stderr));
        }
        (output, std::fs::read_to_string(export).unwrap_or_default())
    })
    .await
    .unwrap();
    let stderr = text(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "{stderr}");
    assert!(
        stderr.contains("with 2 workers: 1/3 of the test"),
        "{stderr}"
    );
    assert!(
        stderr.contains("with 1 worker: 2/3 of the test"),
        "{stderr}"
    );
    let export: serde_json::Value = serde_json::from_str(&export).unwrap();
    assert_eq!(export["metrics"]["iterations"]["count"], 30.0);
    assert_eq!(server.requests() - before, 30);
}
