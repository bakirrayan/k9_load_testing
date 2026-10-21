//! `--out statsd` and `--out datadog`: samples as StatsD lines over UDP, like
//! k6's outputs.

use std::net::UdpSocket;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use k9_testserver::TestServer;

fn write_plan(name: &str, yaml: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-statsd-tests-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.yaml"));
    std::fs::write(&path, yaml).unwrap();
    path
}

async fn k9(args: Vec<String>, env: Vec<(&'static str, String)>) -> Output {
    tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_k9"))
            .args(args)
            .envs(env)
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A UDP receiver keeping every datagram.
struct Receiver {
    address: String,
    datagrams: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
}

impl Receiver {
    fn start() -> Receiver {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let address = socket.local_addr().unwrap().to_string();
        let datagrams = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (kept, stopping) = (datagrams.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut buf = [0u8; 65536];
            while !stopping.load(Ordering::Relaxed) {
                if let Ok(n) = socket.recv(&mut buf) {
                    kept.lock()
                        .unwrap()
                        .push(String::from_utf8(buf[..n].to_vec()).unwrap());
                }
            }
        });
        Receiver {
            address,
            datagrams,
            stop,
        }
    }

    /// Every datagram, once the last ones have arrived.
    fn finish(self) -> Vec<String> {
        std::thread::sleep(Duration::from_millis(200));
        self.stop.store(true, Ordering::Relaxed);
        self.datagrams.lock().unwrap().clone()
    }
}

/// A parsed StatsD line: name, value, type and DogStatsD tags.
#[derive(Debug)]
struct Line {
    name: String,
    value: f64,
    kind: String,
    tags: Vec<(String, String)>,
}

fn parse(datagrams: &[String]) -> Vec<Line> {
    datagrams
        .iter()
        .flat_map(|d| d.lines())
        .map(|line| {
            let (name, rest) = line.split_once(':').unwrap();
            let mut parts = rest.split('|');
            let value = parts.next().unwrap().parse().unwrap();
            let kind = parts.next().unwrap().to_owned();
            let tags = parts
                .find_map(|p| p.strip_prefix('#'))
                .map(|t| {
                    t.split(',')
                        .map(|kv| {
                            let (k, v) = kv.split_once(':').unwrap();
                            (k.to_owned(), v.to_owned())
                        })
                        .collect()
                })
                .unwrap_or_default();
            Line {
                name: name.to_owned(),
                value,
                kind,
                tags,
            }
        })
        .collect()
}

fn select<'a>(lines: &'a [Line], name: &str) -> Vec<&'a Line> {
    lines.iter().filter(|l| l.name == name).collect()
}

fn tag<'a>(line: &'a Line, key: &str) -> Option<&'a str> {
    line.tags
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

fn plan(server: &TestServer, name: &str) -> PathBuf {
    write_plan(
        name,
        &format!(
            "options:\n  scenarios:\n    s: {{ executor: shared-iterations, vus: 2, iterations: 8 }}\nflows:\n  default:\n    - request: {{ url: '{}', checks: {{ 'is ok': {{ status: 200 }} }} }}\n",
            server.url("/delay/20")
        ),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn samples_reach_statsd_like_k6s_output() {
    let server = TestServer::start().await.unwrap();
    let receiver = Receiver::start();
    let out = k9(
        vec![
            "run".into(),
            plan(&server, "statsd").display().to_string(),
            "--out".into(),
            "statsd".into(),
        ],
        vec![
            ("K6_STATSD_ADDR", receiver.address.clone()),
            ("K6_STATSD_NAMESPACE", "load.".into()),
            ("K9_STATSD_BUFFER_SIZE", "5".into()),
        ],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(text(&out.stdout).contains("output: statsd ("));
    let datagrams = receiver.finish();
    assert!(
        datagrams.iter().all(|d| d.lines().count() <= 5),
        "BUFFER_SIZE lines at most"
    );
    let lines = parse(&datagrams);
    // Counters as counts, trends as timings in ms, gauges as gauges.
    let reqs = select(&lines, "load.http_reqs");
    assert_eq!(reqs.len(), 8);
    assert!(reqs.iter().all(|l| l.kind == "c" && l.value == 1.0));
    assert!(reqs[0].tags.is_empty(), "tags are off by default");
    let durations = select(&lines, "load.http_req_duration");
    assert_eq!(durations.len(), 8);
    assert!(
        durations
            .iter()
            .all(|l| l.kind == "ms" && (15.0..500.0).contains(&l.value))
    );
    assert!(select(&lines, "load.vus").iter().all(|l| l.kind == "g"));
    // Checks are counted by name and outcome; other rates as 0/1 counts.
    let passed = select(&lines, "load.check.is ok.pass");
    assert_eq!(passed.len(), 8);
    assert!(passed.iter().all(|l| l.kind == "c" && l.value == 1.0));
    let failed = select(&lines, "load.http_req_failed");
    assert!(failed.iter().all(|l| l.kind == "c" && l.value == 0.0));
}

#[tokio::test(flavor = "multi_thread")]
async fn datadog_sends_tags_but_not_blocked_ones() {
    let server = TestServer::start().await.unwrap();
    let receiver = Receiver::start();
    let out = k9(
        vec![
            "run".into(),
            plan(&server, "datadog").display().to_string(),
            "--out".into(),
            format!("datadog={}", receiver.address),
        ],
        vec![("K6_DATADOG_TAG_BLACKLIST", "url,method".into())],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let lines = parse(&receiver.finish());
    let reqs = select(&lines, "k6.http_reqs");
    assert_eq!(reqs.len(), 8);
    assert_eq!(tag(reqs[0], "status"), Some("200"));
    assert_eq!(tag(reqs[0], "scenario"), Some("s"));
    assert_eq!(tag(reqs[0], "url"), None);
    assert_eq!(tag(reqs[0], "method"), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_settings_are_refused() {
    for (key, value, expected) in [
        ("K6_STATSD_BUFFER_SIZE", "0", "K6_STATSD_BUFFER_SIZE"),
        ("K6_STATSD_ENABLE_TAGS", "maybe", "K6_STATSD_ENABLE_TAGS"),
        ("K6_STATSD_ADDR", "nowhere", "nowhere"),
    ] {
        let plan = write_plan("bad", "flows:\n  default: []\n");
        let out = k9(
            vec![
                "run".into(),
                plan.display().to_string(),
                "--out".into(),
                "statsd".into(),
            ],
            vec![(key, value.into())],
        )
        .await;
        assert_eq!(out.status.code(), Some(104), "{key}");
        assert!(
            text(&out.stderr).contains(expected),
            "{key}: {}",
            text(&out.stderr)
        );
    }
}
