//! `--out prometheus-rw`: Prometheus remote write, named like k6's output so
//! its Grafana dashboards work.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, Output};

use k9_testserver::TestServer;

mod common;
use common::{fields, packed};

fn write_plan(name: &str, yaml: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-prom-tests-{}", std::process::id()));
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

/// A decoded time series: labels, and (value, timestamp ms) samples.
#[derive(Debug)]
struct Series {
    labels: BTreeMap<String, String>,
    samples: Vec<(f64, i64)>,
    histograms: Vec<Histogram>,
}

/// A decoded native histogram (positive buckets only).
#[derive(Debug)]
struct Histogram {
    count: u64,
    sum: f64,
    schema: i64,
    zero_count: u64,
    /// (bucket index, count).
    buckets: Vec<(i64, u64)>,
    time: i64,
}

fn zigzag(v: u64) -> i64 {
    (v >> 1) as i64 ^ -((v & 1) as i64)
}

fn decode_histogram(bytes: &[u8]) -> Histogram {
    let mut h = Histogram {
        count: 0,
        sum: 0.0,
        schema: 0,
        zero_count: 0,
        buckets: Vec::new(),
        time: 0,
    };
    let (mut spans, mut deltas) = (Vec::new(), Vec::new());
    for (n, field) in fields(bytes) {
        match n {
            1 => h.count = field.varint(),
            3 => h.sum = field.double(),
            4 => h.schema = zigzag(field.varint()),
            5 => assert!(field.double() > 0.0),
            6 => h.zero_count = field.varint(),
            11 => {
                let span = fields(field.bytes());
                let get = |n| {
                    span.iter()
                        .find(|(k, _)| *k == n)
                        .map_or(0, |(_, f)| f.varint())
                };
                spans.push((zigzag(get(1)), get(2)));
            }
            12 => deltas.extend(packed(field.bytes()).into_iter().map(zigzag)),
            15 => h.time = field.varint() as i64,
            8 | 9 => panic!("no negative samples were recorded"),
            other => panic!("unexpected histogram field {other}"),
        }
    }
    let (mut index, mut count, mut deltas) = (0i64, 0i64, deltas.into_iter());
    for (offset, length) in spans {
        index += offset;
        for _ in 0..length {
            count += deltas.next().unwrap();
            h.buckets.push((index, count as u64));
            index += 1;
        }
    }
    assert!(deltas.next().is_none(), "one delta per bucket");
    h
}

/// Decodes a remote-write body: snappy, then the `WriteRequest` protobuf.
fn decode(body: &[u8]) -> Vec<Series> {
    let raw = snap::raw::Decoder::new().decompress_vec(body).unwrap();
    fields(&raw)
        .into_iter()
        .filter(|(n, _)| *n == 1)
        .map(|(_, ts)| {
            let mut series = Series {
                labels: BTreeMap::new(),
                samples: Vec::new(),
                histograms: Vec::new(),
            };
            let mut last_label = String::new();
            for (n, part) in fields(ts.bytes()) {
                let inner = fields(part.bytes());
                match n {
                    1 => {
                        let name = inner[0].1.string();
                        assert!(name > last_label, "labels are sorted by name");
                        last_label = name.clone();
                        series.labels.insert(name, inner[1].1.string());
                    }
                    2 => {
                        let value = inner.iter().find(|(n, _)| *n == 1).unwrap().1.double();
                        let time = inner.iter().find(|(n, _)| *n == 2).unwrap().1.varint();
                        series.samples.push((value, time as i64));
                    }
                    4 => series.histograms.push(decode_histogram(part.bytes())),
                    _ => panic!("unexpected field {n}"),
                }
            }
            series
        })
        .collect()
}

/// The latest value of each series (by `__name__` and labels) over every push.
fn latest(server: &TestServer) -> Vec<Series> {
    let mut all: BTreeMap<Vec<(String, String)>, Series> = BTreeMap::new();
    for push in server.remote_writes() {
        for series in decode(&push.body) {
            let key: Vec<(String, String)> = series.labels.clone().into_iter().collect();
            all.insert(key, series);
        }
    }
    all.into_values().collect()
}

fn find<'a>(series: &'a [Series], name: &str, labels: &[(&str, &str)]) -> Option<&'a Series> {
    series.iter().find(|s| {
        s.labels.get("__name__").map(String::as_str) == Some(name)
            && labels
                .iter()
                .all(|(k, v)| s.labels.get(*k).map(String::as_str) == Some(*v))
    })
}

fn value(series: &[Series], name: &str, labels: &[(&str, &str)]) -> f64 {
    find(series, name, labels)
        .unwrap_or_else(|| {
            let names: Vec<_> = series.iter().map(|s| &s.labels["__name__"]).collect();
            panic!("no {name} {labels:?} in {names:?}")
        })
        .samples
        .last()
        .unwrap()
        .0
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_reach_prometheus_named_like_k6s_output() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "rw",
        &format!(
            "options:\n  scenarios:\n    s: {{ executor: shared-iterations, vus: 2, iterations: 8, tags: {{ empty: '' }} }}\nflows:\n  default:\n    - request: {{ url: '{}', checks: {{ ok: {{ status: 200 }} }} }}\n    - sleep: 250ms\n",
            server.url("/delay/20")
        ),
    );
    let out = k9(
        vec![
            "run".into(),
            plan.display().to_string(),
            "--out".into(),
            format!("prometheus-rw={}", server.url("/api/v1/write")),
        ],
        vec![
            ("K6_PROMETHEUS_RW_PUSH_INTERVAL", "1s".into()),
            (
                "K9_PROMETHEUS_RW_TREND_STATS",
                "p(99),p(99.9),avg,count,sum".into(),
            ),
            ("K6_PROMETHEUS_RW_USERNAME", "user".into()),
            ("K6_PROMETHEUS_RW_PASSWORD", "pass".into()),
            (
                "K6_PROMETHEUS_RW_LABELS",
                "testid=run-1,scenario=overridden".into(),
            ),
            ("K6_PROMETHEUS_RW_HEADERS_X-Tenant", "team-a".into()),
        ],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains("output: prometheus-rw ("),
        "{}",
        text(&out.stdout)
    );
    let pushes = server.remote_writes();
    assert!(pushes.len() >= 2, "one a second, and a last one at the end");
    let headers = &pushes[0].headers;
    assert_eq!(headers["content-encoding"], "snappy");
    assert_eq!(headers["content-type"], "application/x-protobuf");
    assert_eq!(headers["x-prometheus-remote-write-version"], "0.1.0");
    assert_eq!(headers["authorization"], "Basic dXNlcjpwYXNz");
    assert_eq!(headers["x-tenant"], "team-a");

    let series = latest(&server);
    let url = server.url("/delay/20");
    let req = [("url", url.as_str()), ("status", "200"), ("scenario", "s")];
    // Counters: cumulative, `_total`.
    assert_eq!(value(&series, "k6_http_reqs_total", &req), 8.0);
    assert_eq!(value(&series, "k6_iterations_total", &[]), 8.0);
    // Trends: one series per stat; times in seconds.
    let p99 = value(&series, "k6_http_req_duration_p99", &req);
    assert!((0.015..0.5).contains(&p99), "{p99}");
    assert!(value(&series, "k6_http_req_duration_p999", &req) >= p99);
    assert_eq!(value(&series, "k6_http_req_duration_count", &req), 8.0);
    let sum = value(&series, "k6_http_req_duration_sum", &req);
    let avg = value(&series, "k6_http_req_duration_avg", &req);
    assert!((sum / 8.0 - avg).abs() < 1e-9, "{sum} {avg}");
    // Rates: the ratio, `_rate`.
    assert_eq!(value(&series, "k6_checks_rate", &[("check", "ok")]), 1.0);
    assert_eq!(value(&series, "k6_http_req_failed_rate", &req), 0.0);
    // Gauges as they are.
    assert_eq!(value(&series, "k6_vus_max", &[]), 2.0);
    // Static labels, unless a tag has the name; empty tags are left out.
    let reqs = find(&series, "k6_http_reqs_total", &req).unwrap();
    assert_eq!(reqs.labels["testid"], "run-1");
    assert_eq!(reqs.labels["scenario"], "s");
    assert!(!reqs.labels.contains_key("empty"));
    // Timestamps are milliseconds, around now.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert!((now - reqs.samples[0].1).abs() < 60_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_endpoint_is_reported_but_doesnt_fail_the_run() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "rw-fail",
        &format!(
            "options: {{ iterations: 2 }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/ok")
        ),
    );
    let out = k9(
        vec![
            "run".into(),
            plan.display().to_string(),
            "--out".into(),
            "prometheus-rw".into(),
        ],
        vec![("K9_PROMETHEUS_RW_SERVER_URL", server.url("/status/500"))],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("prometheus-rw") && stderr.contains("500"),
        "{stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_settings_are_refused() {
    for (key, value, expected) in [
        (
            "K9_PROMETHEUS_RW_PUSH_INTERVAL",
            "soon",
            "K9_PROMETHEUS_RW_PUSH_INTERVAL",
        ),
        ("K6_PROMETHEUS_RW_TREND_STATS", "p(99),mean", "mean"),
        (
            "K6_PROMETHEUS_RW_LABELS",
            "novalue",
            "K6_PROMETHEUS_RW_LABELS",
        ),
    ] {
        let plan = write_plan("rw-bad", "flows:\n  default: []\n");
        let out = k9(
            vec![
                "run".into(),
                plan.display().to_string(),
                "--out".into(),
                "experimental-prometheus-rw".into(),
            ],
            vec![(key, value.into())],
        )
        .await;
        assert_eq!(out.status.code(), Some(104), "{key}");
        assert!(
            text(&out.stderr).contains(expected),
            "{}",
            text(&out.stderr)
        );
    }
}

/// Prometheus' staleness marker.
const STALE_NAN: u64 = 0x7ff0_0000_0000_0002;

#[tokio::test(flavor = "multi_thread")]
async fn trends_as_native_histograms_and_stale_markers() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "rw-native",
        &format!(
            "options:\n  scenarios:\n    s: {{ executor: shared-iterations, vus: 2, iterations: 8 }}\nflows:\n  default:\n    - request: {{ url: '{}' }}\n",
            server.url("/delay/20")
        ),
    );
    let out = k9(
        vec![
            "run".into(),
            plan.display().to_string(),
            "--out".into(),
            format!("prometheus-rw={}", server.url("/api/v1/write")),
        ],
        vec![
            ("K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM", "true".into()),
            ("K9_PROMETHEUS_RW_STALE_MARKERS", "true".into()),
        ],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let pushes = server.remote_writes();
    assert!(pushes.len() >= 2, "the totals, then the stale markers");
    let (last, before) = pushes.split_last().unwrap();

    let series: Vec<Series> = before.iter().flat_map(|p| decode(&p.body)).collect();
    let url = server.url("/delay/20");
    let req = [("url", url.as_str()), ("status", "200")];
    assert!(
        find(&series, "k6_http_req_duration_p99", &req).is_none(),
        "no stats for trends sent as histograms"
    );
    // Time trends are in seconds, with k6's `_seconds` suffix.
    let duration = series
        .iter()
        .rev()
        .find(|s| {
            s.labels["__name__"] == "k6_http_req_duration_seconds"
                && req
                    .iter()
                    .all(|(k, v)| s.labels.get(*k).map(String::as_str) == Some(*v))
        })
        .expect("a native histogram");
    assert!(duration.samples.is_empty());
    let h = duration.histograms.last().unwrap();
    assert_eq!(h.count, 8);
    assert_eq!(h.schema, 8, "k6's bucket factor, 1.00271");
    assert!((0.015 * 8.0..4.0).contains(&h.sum), "{}", h.sum);
    let in_buckets: u64 = h.buckets.iter().map(|(_, c)| c).sum();
    assert_eq!(in_buckets + h.zero_count, 8);
    for (index, _) in &h.buckets {
        // Bucket `i` holds values in (2^((i-1)/256), 2^(i/256)].
        let upper = 2f64.powf(*index as f64 / 256.0);
        assert!((0.015..0.6).contains(&upper), "{index}: {upper}");
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert!((now - h.time).abs() < 60_000);
    // Other metrics are as usual.
    assert_eq!(value(&series, "k6_http_reqs_total", &req), 8.0);

    // The last push marks every series stale.
    let stale = decode(&last.body);
    for name in [
        "k6_http_reqs_total",
        "k6_http_req_duration_seconds",
        "k6_vus",
    ] {
        let s = find(&stale, name, &[]).unwrap_or_else(|| panic!("{name} is marked stale"));
        assert!(s.histograms.is_empty());
        assert_eq!(s.samples[0].0.to_bits(), STALE_NAN, "{name}");
    }
    assert_eq!(
        stale.len(),
        latest_keys(before),
        "every series sent is marked stale"
    );
}

/// How many distinct series `pushes` carried.
fn latest_keys(pushes: &[k9_testserver::RemoteWrite]) -> usize {
    let mut keys = std::collections::BTreeSet::new();
    for push in pushes {
        for series in decode(&push.body) {
            keys.insert(series.labels);
        }
    }
    keys.len()
}
