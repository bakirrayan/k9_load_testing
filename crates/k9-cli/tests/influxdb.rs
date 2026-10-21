//! `--out influxdb` (k6's InfluxDB v1 output) and `--out xk6-influxdb`
//! (InfluxDB v2, k6's extension): every sample as a line-protocol point named
//! like k6's, so its InfluxDB dashboards work.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use k9_testserver::TestServer;

fn write_plan(name: &str, yaml: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-influx-tests-{}", std::process::id()));
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

/// A parsed line-protocol point.
#[derive(Debug)]
struct Point {
    measurement: String,
    tags: BTreeMap<String, String>,
    /// Raw field values (`1`, `200i`, `"text"`).
    fields: BTreeMap<String, String>,
    time: i64,
}

/// Splits on `sep` outside backslash escapes and double quotes.
fn split(text: &str, sep: char) -> Vec<String> {
    let (mut parts, mut part) = (Vec::new(), String::new());
    let (mut escaped, mut quoted) = (false, false);
    for c in text.chars() {
        if escaped {
            part.push(c);
            escaped = false;
        } else if c == '\\' {
            part.push(c);
            escaped = true;
        } else if c == '"' {
            part.push(c);
            quoted = !quoted;
        } else if c == sep && !quoted {
            parts.push(std::mem::take(&mut part));
        } else {
            part.push(c);
        }
    }
    parts.push(part);
    parts
}

fn unescape(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            out.extend(chars.next());
        } else {
            out.push(c);
        }
    }
    out
}

fn parse(body: &[u8]) -> Vec<Point> {
    let body = std::str::from_utf8(body).expect("line protocol is UTF-8");
    body.lines()
        .filter(|l| !l.is_empty())
        .map(|line| {
            let sections = split(line, ' ');
            assert_eq!(sections.len(), 3, "series, fields, time: {line}");
            let mut series = split(&sections[0], ',').into_iter();
            let measurement = unescape(&series.next().unwrap());
            let pair = |p: String| {
                let kv = split(&p, '=');
                assert_eq!(kv.len(), 2, "{p} in {line}");
                (unescape(&kv[0]), kv[1].clone())
            };
            let tags = series
                .map(pair)
                .map(|(k, v)| (k, unescape(&v)))
                .collect::<BTreeMap<_, _>>();
            let keys: Vec<_> = tags.keys().collect();
            let fields = split(&sections[1], ',').into_iter().map(pair).collect();
            assert!(keys.windows(2).all(|w| w[0] < w[1]), "tags are sorted");
            Point {
                measurement,
                tags,
                fields,
                time: sections[2].parse().unwrap(),
            }
        })
        .collect()
}

fn points(server: &TestServer, path: &str) -> Vec<Point> {
    server
        .pushes(path)
        .iter()
        .flat_map(|p| parse(&p.body))
        .collect()
}

fn select<'a>(points: &'a [Point], name: &str, tags: &[(&str, &str)]) -> Vec<&'a Point> {
    points
        .iter()
        .filter(|p| {
            p.measurement == name
                && tags
                    .iter()
                    .all(|(k, v)| p.tags.get(*k).map(String::as_str) == Some(*v))
        })
        .collect()
}

fn value(point: &Point) -> f64 {
    point.fields["value"].parse().unwrap()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn login_plan(server: &TestServer, name: &str) -> PathBuf {
    write_plan(
        name,
        &format!(
            "options:\n  scenarios:\n    s: {{ executor: shared-iterations, vus: 2, iterations: 8, tags: {{ empty: '', 'my tag': 'a,b=c' }} }}\nflows:\n  default:\n    - request: {{ url: '{}', checks: {{ ok: {{ status: 200 }} }} }}\n    - sleep: 250ms\n",
            server.url("/delay/20")
        ),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn every_sample_reaches_influxdb_v1_named_like_k6s_output() {
    let server = TestServer::start().await.unwrap();
    let plan = login_plan(&server, "v1");
    let out = k9(
        vec![
            "run".into(),
            plan.display().to_string(),
            "--out".into(),
            format!("influxdb={}", server.url("/k6db")),
        ],
        vec![
            ("K6_INFLUXDB_PUSH_INTERVAL", "1s".into()),
            ("K6_INFLUXDB_USERNAME", "user".into()),
            ("K9_INFLUXDB_PASSWORD", "pass".into()),
        ],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains("output: influxdb ("),
        "{}",
        text(&out.stdout)
    );
    // The database is created first, as k6 does.
    let queries = server.pushes("/query");
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].query, "q=CREATE%20DATABASE%20%22k6db%22");
    assert_eq!(queries[0].headers["authorization"], "Basic dXNlcjpwYXNz");

    let writes = server.pushes("/write");
    assert!(writes.len() >= 2, "one a second, and a last one at the end");
    assert_eq!(writes[0].query, "db=k6db&precision=ns");
    assert_eq!(writes[0].headers["authorization"], "Basic dXNlcjpwYXNz");
    assert!(writes[0].headers["content-type"].starts_with("text/plain"));

    let points = points(&server, "/write");
    let url = server.url("/delay/20");
    // Every sample is a point with a `value` field; `url` is a field (k6's
    // default tagsAsFields), the other tags are tags.
    let reqs = select(
        &points,
        "http_reqs",
        &[("status", "200"), ("scenario", "s")],
    );
    assert_eq!(reqs.len(), 8);
    assert!(reqs.iter().all(|p| value(p) == 1.0));
    assert_eq!(reqs[0].fields["url"], format!("\"{url}\""));
    assert!(!reqs[0].tags.contains_key("url"));
    assert!(
        !reqs[0].tags.contains_key("empty"),
        "empty tags are left out"
    );
    assert_eq!(reqs[0].tags["my tag"], "a,b=c", "escaped and read back");
    // Trends in milliseconds, as samples.
    let durations = select(&points, "http_req_duration", &[("status", "200")]);
    assert_eq!(durations.len(), 8);
    assert!(durations.iter().all(|p| (15.0..500.0).contains(&value(p))));
    let checks = select(&points, "checks", &[("check", "ok")]);
    assert_eq!(checks.len(), 8);
    assert!(checks.iter().all(|p| value(p) == 1.0));
    let iterations: f64 = select(&points, "iterations", &[])
        .iter()
        .map(|p| value(p))
        .sum();
    assert_eq!(iterations, 8.0);
    // Nanosecond timestamps, around now.
    assert!((now_ns() - reqs[0].time).abs() < 60_000_000_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn influxdb_v2_takes_a_token_bucket_and_typed_fields() {
    let server = TestServer::start().await.unwrap();
    let plan = login_plan(&server, "v2");
    let out = k9(
        vec![
            "run".into(),
            plan.display().to_string(),
            "--out".into(),
            "xk6-influxdb".into(),
        ],
        vec![
            ("K6_INFLUXDB_ADDR", server.url("/")),
            ("K6_INFLUXDB_ORGANIZATION", "my org".into()),
            ("K6_INFLUXDB_BUCKET", "k6".into()),
            ("K6_INFLUXDB_TOKEN", "t0k".into()),
            ("K6_INFLUXDB_PRECISION", "ms".into()),
            ("K6_INFLUXDB_TAGS_AS_FIELDS", "url,status:int".into()),
        ],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(server.pushes("/query").is_empty(), "v2 creates nothing");
    let writes = server.pushes("/api/v2/write");
    assert!(!writes.is_empty());
    assert_eq!(writes[0].query, "org=my%20org&bucket=k6&precision=ms");
    assert_eq!(writes[0].headers["authorization"], "Token t0k");

    let points = points(&server, "/api/v2/write");
    let reqs = select(&points, "http_reqs", &[("scenario", "s")]);
    assert_eq!(reqs.len(), 8);
    assert_eq!(reqs[0].fields["status"], "200i");
    assert!(!reqs[0].tags.contains_key("status"));
    // Millisecond timestamps.
    assert!((now_ns() / 1_000_000 - reqs[0].time).abs() < 60_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_settings_are_refused() {
    for (out_spec, key, value, expected) in [
        (
            "xk6-influxdb",
            "K6_INFLUXDB_TOKEN",
            "t",
            "K6_INFLUXDB_BUCKET",
        ),
        (
            "influxdb",
            "K6_INFLUXDB_TAGS_AS_FIELDS",
            "vu:date",
            "K6_INFLUXDB_TAGS_AS_FIELDS",
        ),
        (
            "influxdb",
            "K9_INFLUXDB_PRECISION",
            "1d",
            "K9_INFLUXDB_PRECISION",
        ),
        ("influxdb=localhost:8086", "K6_INFLUXDB_DB", "x", "http"),
    ] {
        let plan = write_plan("bad", "flows:\n  default: []\n");
        let out = k9(
            vec![
                "run".into(),
                plan.display().to_string(),
                "--out".into(),
                out_spec.into(),
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
