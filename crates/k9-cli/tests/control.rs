//! `--address` (the control API, k6's `/v1/status`) and `--paused`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use k9_testserver::TestServer;
use serde_json::{Value, json};

fn write_plan(name: &str, yaml: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-control-tests-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.yaml"));
    std::fs::write(&path, yaml).unwrap();
    path
}

/// Starts `k9 run` and reads its header up to the control API's address.
// The caller waits for the child.
#[allow(clippy::zombie_processes)]
fn start(args: &[&str]) -> (Child, String) {
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
            panic!(
                "no control line: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        if let Some(rest) = line.trim().strip_prefix("control: http://") {
            let address = rest.split_whitespace().next().unwrap().to_owned();
            // Keep reading so k9 never blocks on a full pipe.
            std::thread::spawn(move || std::io::copy(&mut stdout, &mut std::io::sink()));
            return (child, address);
        }
    }
}

/// One request; returns the status code and the JSON body.
fn call(address: &str, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
    let mut stream = TcpStream::connect(address).unwrap();
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nhost: {address}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response[9..12].parse().unwrap();
    let body = &response[response.find("\r\n\r\n").unwrap() + 4..];
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

fn patch(address: &str, attributes: Value) -> (u16, Value) {
    call(
        address,
        "PATCH",
        "/v1/status",
        Some(json!({ "data": { "type": "status", "id": "default", "attributes": attributes } })),
    )
}

fn attributes(address: &str) -> Value {
    let (status, body) = call(address, "GET", "/v1/status", None);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["type"], "status");
    assert_eq!(body["data"]["id"], "default");
    body["data"]["attributes"].clone()
}

fn until(address: &str, what: &str, ok: impl Fn(&Value) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let attrs = attributes(address);
        if ok(&attrs) {
            return;
        }
        assert!(Instant::now() < deadline, "{what}: {attrs}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_paused_run_is_scaled_resumed_and_stopped_through_the_api() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "live",
        &format!(
            "options:\n  scenarios:\n    live: {{ executor: externally-controlled, vus: 1, maxVUs: 4, duration: 10m }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/delay/20")
        ),
    );
    let output = tokio::task::spawn_blocking(move || {
        let plan = plan.display().to_string();
        let (child, address) = start(&["run", &plan, "--paused", "--address", "127.0.0.1:0"]);

        let attrs = attributes(&address);
        assert_eq!(attrs["paused"], true);
        assert_eq!(attrs["running"], false);
        assert_eq!(attrs["stopped"], false);

        // Scale while paused, then resume.
        let (status, body) = patch(&address, json!({ "vus": 3 }));
        assert_eq!(status, 200, "{body}");
        let (status, body) = patch(&address, json!({ "paused": false }));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["attributes"]["paused"], false);
        until(&address, "running with 3 VUs", |a| {
            a["running"] == true && a["vus"] == 3 && a["vus-max"] == 4
        });

        // Live metrics, merged across tags, in k6's format.
        std::thread::sleep(Duration::from_millis(1100));
        let (status, body) = call(&address, "GET", "/v1/metrics", None);
        assert_eq!(status, 200, "{body}");
        let list = body["data"].as_array().unwrap();
        let reqs = list
            .iter()
            .find(|m| m["id"] == "http_reqs")
            .expect("http_reqs");
        assert_eq!(reqs["type"], "metrics");
        assert_eq!(reqs["attributes"]["type"], "counter");
        assert!(reqs["attributes"]["sample"]["count"].as_f64().unwrap() > 0.0);
        let (status, body) = call(&address, "GET", "/v1/metrics/http_req_duration", None);
        assert_eq!(status, 200, "{body}");
        let attrs = &body["data"]["attributes"];
        assert_eq!(
            (attrs["type"].as_str(), attrs["contains"].as_str()),
            (Some("trend"), Some("time"))
        );
        assert!(
            attrs["sample"]["p(95)"].as_f64().unwrap() >= 20.0,
            "{attrs}"
        );
        let (status, _) = call(&address, "GET", "/v1/metrics/nope", None);
        assert_eq!(status, 404);

        // Refusals come back as JSON:API errors.
        let (status, body) = patch(&address, json!({ "vus": 9 }));
        assert_eq!(status, 400);
        let detail = body["errors"][0]["detail"].as_str().unwrap();
        assert!(detail.contains("maxVUs is 4"), "{detail}");
        let (status, body) = patch(&address, json!({ "vus-max": 10 }));
        assert_eq!(status, 400, "{body}");
        let (status, _) = call(&address, "GET", "/v1/nothing", None);
        assert_eq!(status, 404);

        let (status, body) = patch(&address, json!({ "stopped": true }));
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["attributes"]["stopped"], true);
        child.wait_with_output().unwrap()
    })
    .await
    .unwrap();
    // Stopped from outside, like Ctrl-C.
    assert_eq!(
        output.status.code(),
        Some(105),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn other_executors_can_only_be_paused_before_they_start() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "vus",
        &format!(
            "options: {{ vus: 2, duration: 1s }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/ok")
        ),
    );
    let output = tokio::task::spawn_blocking(move || {
        let plan = plan.display().to_string();
        let (child, address) = start(&["run", &plan, "--address", "127.0.0.1:0"]);
        until(&address, "running", |a| a["running"] == true);
        let (status, body) = patch(&address, json!({ "paused": true }));
        assert_eq!(status, 400);
        let detail = body["errors"][0]["detail"].as_str().unwrap();
        assert!(detail.contains("externally-controlled"), "{detail}");
        child.wait_with_output().unwrap()
    })
    .await
    .unwrap();
    assert_eq!(output.status.code(), Some(0));
}

#[tokio::test]
async fn an_address_that_cant_be_used_is_a_config_error() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = taken.local_addr().unwrap().to_string();
    let plan = write_plan(
        "taken",
        "options: { iterations: 1 }\nflows: { default: [] }\n",
    );
    let output = Command::new(env!("CARGO_BIN_EXE_k9"))
        .args(["run", &plan.display().to_string(), "--address", &address])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(104));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(&address), "{stderr}");
}

fn k9(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_k9"))
        .args(args)
        .output()
        .unwrap()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn commands_control_a_running_test() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "commands",
        &format!(
            "options:\n  scenarios:\n    live: {{ executor: externally-controlled, vus: 1, maxVUs: 3, duration: 10m }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/delay/20")
        ),
    );
    let output = tokio::task::spawn_blocking(move || {
        let plan = plan.display().to_string();
        let (child, address) = start(&["run", &plan, "--paused", "--address", "127.0.0.1:0"]);
        let a = ["--address", address.as_str()];

        let out = k9(&[&["status"][..], &a].concat());
        assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
        let status = text(&out.stdout);
        assert!(
            status.contains("paused: true") && status.contains("running: false"),
            "{status}"
        );

        let out = k9(&[&["scale", "--vus", "2"][..], &a].concat());
        assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
        let out = k9(&[&["resume"][..], &a].concat());
        assert!(
            text(&out.stdout).contains("paused: false"),
            "{}",
            text(&out.stdout)
        );
        until(&address, "2 VUs", |a| a["vus"] == 2);

        let out = k9(&[&["scale", "--vus", "5"][..], &a].concat());
        assert_eq!(out.status.code(), Some(1));
        assert!(
            text(&out.stderr).contains("maxVUs is 3"),
            "{}",
            text(&out.stderr)
        );

        std::thread::sleep(Duration::from_millis(1100));
        let out = k9(&[&["stats"][..], &a].concat());
        assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
        let stats = text(&out.stdout);
        assert!(stats.contains("http_reqs (counter): count="), "{stats}");
        assert!(stats.contains("http_req_duration (trend): avg="), "{stats}");

        let out = k9(&[&["pause"][..], &a].concat());
        assert!(text(&out.stdout).contains("paused: true"));
        let out = k9(&[&["stop"][..], &a].concat());
        assert!(text(&out.stdout).contains("stopped: true"));
        child.wait_with_output().unwrap()
    })
    .await
    .unwrap();
    assert_eq!(output.status.code(), Some(105));

    // Nothing listening.
    let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = free.local_addr().unwrap().to_string();
    drop(free);
    let out = k9(&["status", "--address", &address]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        text(&out.stderr).contains(&address),
        "{}",
        text(&out.stderr)
    );
}

#[cfg(target_os = "linux")]
#[test]
fn a_low_open_file_limit_is_raised_or_reported() {
    let plan = write_plan(
        "fds",
        "options:\n  scenarios:\n    s: { executor: per-vu-iterations, vus: 300, iterations: 1 }\nflows: { default: [] }\n",
    );
    let run = |ulimit: &str| {
        Command::new("sh")
            .arg("-c")
            .arg(format!("ulimit {ulimit} && exec \"$0\" run \"$1\""))
            .arg(env!("CARGO_BIN_EXE_k9"))
            .arg(&plan)
            .output()
            .unwrap()
    };
    // A hard limit of 256 can't hold 300 VUs' sockets.
    let out = run("-n 256");
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("WARN the open-file limit (256) is too low for 300 VUs"),
        "{stderr}"
    );
    // A low soft limit under a high hard one is raised.
    let out = run("-S -n 256");
    assert_eq!(out.status.code(), Some(0));
    assert!(
        !text(&out.stderr).contains("open-file limit"),
        "{}",
        text(&out.stderr)
    );
}
