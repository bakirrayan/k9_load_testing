//! `--out opentelemetry`: OTLP metrics over HTTP, like k6's OpenTelemetry
//! output.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, Output};

use k9_testserver::TestServer;

mod common;
use common::{Field, fields};

fn write_plan(name: &str, yaml: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-otlp-tests-{}", std::process::id()));
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

#[derive(Debug, Default)]
struct Point {
    attrs: BTreeMap<String, String>,
    start: u64,
    time: u64,
    /// `as_double`, or `as_int` as a float.
    value: Option<f64>,
    count: u64,
    sum: f64,
    buckets: Vec<u64>,
    bounds: Vec<f64>,
    min: f64,
    max: f64,
}

#[derive(Debug, Default)]
struct Metric {
    name: String,
    unit: String,
    /// `sum`, `gauge` or `histogram`.
    kind: &'static str,
    temporality: u64,
    monotonic: bool,
    points: Vec<Point>,
}

struct Export {
    resource: BTreeMap<String, String>,
    metrics: Vec<Metric>,
}

fn attributes(list: &[(u64, Field<'_>)], field: u64) -> BTreeMap<String, String> {
    list.iter()
        .filter(|(n, _)| *n == field)
        .map(|(_, kv)| {
            let kv = fields(kv.bytes());
            let value = fields(kv[1].1.bytes());
            (kv[0].1.string(), value[0].1.string())
        })
        .collect()
}

fn fixed(field: &Field<'_>) -> u64 {
    match field {
        Field::Fixed64(bits) => *bits,
        other => panic!("{other:?}"),
    }
}

fn point(bytes: &[u8], histogram: bool) -> Point {
    let parts = fields(bytes);
    let mut p = Point {
        attrs: attributes(&parts, if histogram { 9 } else { 7 }),
        ..Point::default()
    };
    for (n, f) in &parts {
        match (histogram, n) {
            (_, 2) => p.start = fixed(f),
            (_, 3) => p.time = fixed(f),
            (false, 4) => p.value = Some(f.double()),
            (false, 6) => p.value = Some(fixed(f) as i64 as f64),
            (true, 4) => p.count = fixed(f),
            (true, 5) => p.sum = f.double(),
            (true, 6) => {
                p.buckets = f
                    .bytes()
                    .chunks(8)
                    .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                    .collect()
            }
            (true, 7) => {
                p.bounds = f
                    .bytes()
                    .chunks(8)
                    .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
                    .collect()
            }
            (true, 11) => p.min = f.double(),
            (true, 12) => p.max = f.double(),
            _ => {}
        }
    }
    p
}

fn decode(body: &[u8]) -> Export {
    let request = fields(body);
    assert_eq!(request.len(), 1, "one ResourceMetrics");
    let resource_metrics = fields(request[0].1.bytes());
    let mut export = Export {
        resource: BTreeMap::new(),
        metrics: Vec::new(),
    };
    for (n, part) in &resource_metrics {
        match n {
            1 => export.resource = attributes(&fields(part.bytes()), 1),
            2 => {
                for (n, metric) in fields(part.bytes()) {
                    if n != 2 {
                        continue;
                    }
                    let mut m = Metric::default();
                    for (n, f) in fields(metric.bytes()) {
                        match n {
                            1 => m.name = f.string(),
                            3 => m.unit = f.string(),
                            5 | 7 | 9 => {
                                m.kind = match n {
                                    5 => "gauge",
                                    7 => "sum",
                                    _ => "histogram",
                                };
                                for (n, g) in fields(f.bytes()) {
                                    match n {
                                        1 => m.points.push(point(g.bytes(), m.kind == "histogram")),
                                        2 => m.temporality = g.varint(),
                                        3 => m.monotonic = g.varint() == 1,
                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    export.metrics.push(m);
                }
            }
            _ => {}
        }
    }
    export
}

fn metric<'a>(export: &'a Export, name: &str) -> &'a Metric {
    export
        .metrics
        .iter()
        .find(|m| m.name == name)
        .unwrap_or_else(|| {
            let names: Vec<_> = export.metrics.iter().map(|m| &m.name).collect();
            panic!("no {name} in {names:?}")
        })
}

fn with<'a>(m: &'a Metric, attrs: &[(&str, &str)]) -> &'a Point {
    m.points
        .iter()
        .find(|p| {
            attrs
                .iter()
                .all(|(k, v)| p.attrs.get(*k).map(String::as_str) == Some(*v))
        })
        .unwrap_or_else(|| panic!("{}: no point with {attrs:?}: {:?}", m.name, m.points))
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_reach_an_otlp_collector_like_k6s_output() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "otlp",
        &format!(
            "options:\n  scenarios:\n    s: {{ executor: shared-iterations, vus: 2, iterations: 8 }}\nflows:\n  default:\n    - request: {{ url: '{}', checks: {{ ok: {{ status: 200 }} }} }}\n    - sleep: 250ms\n",
            server.url("/delay/20")
        ),
    );
    let endpoint = server.url("").trim_start_matches("http://").to_owned();
    let out = k9(
        vec![
            "run".into(),
            plan.display().to_string(),
            "--out".into(),
            "experimental-opentelemetry".into(),
        ],
        vec![
            ("K6_OTEL_EXPORTER_PROTOCOL", "http/protobuf".into()),
            ("K6_OTEL_HTTP_EXPORTER_ENDPOINT", endpoint),
            ("K6_OTEL_HTTP_EXPORTER_INSECURE", "true".into()),
            ("K6_OTEL_EXPORT_INTERVAL", "1s".into()),
            ("K9_OTEL_METRIC_PREFIX", "k6_".into()),
            ("K6_OTEL_SERVICE_NAME", "checkout-load".into()),
            ("K6_OTEL_HTTP_EXPORTER_USERNAME", "user".into()),
            ("K6_OTEL_HTTP_EXPORTER_PASSWORD", "pass".into()),
            ("K6_OTEL_HEADERS", "x-tenant=team-a".into()),
        ],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains("output: opentelemetry ("),
        "{}",
        text(&out.stdout)
    );
    let pushes = server.otlp_pushes();
    assert!(pushes.len() >= 2, "one a second, and a last one at the end");
    let headers = &pushes[0].headers;
    assert_eq!(headers["content-type"], "application/x-protobuf");
    assert_eq!(headers["authorization"], "Basic dXNlcjpwYXNz");
    assert_eq!(headers["x-tenant"], "team-a");

    let first = decode(&pushes[0].body);
    let last = decode(&pushes.last().unwrap().body);
    assert_eq!(last.resource["service.name"], "checkout-load");
    assert_eq!(last.resource["service.version"], env!("CARGO_PKG_VERSION"));

    let url = server.url("/delay/20");
    let req = [("url", url.as_str()), ("status", "200"), ("scenario", "s")];
    // Counters: cumulative, monotonic sums.
    let reqs = metric(&last, "k6_http_reqs");
    assert_eq!(
        (reqs.kind, reqs.temporality, reqs.monotonic),
        ("sum", 2, true)
    );
    let point = with(reqs, &req);
    assert_eq!(point.value, Some(8.0));
    assert!(point.start < point.time);
    let first_start = with(metric(&first, "k6_http_reqs"), &req).start;
    assert_eq!(point.start, first_start, "one start time for the whole run");
    // Trends: cumulative histograms, in their unit, with the SDK's default buckets.
    let duration = metric(&last, "k6_http_req_duration");
    assert_eq!((duration.kind, duration.unit.as_str()), ("histogram", "ms"));
    assert_eq!(duration.temporality, 2);
    let h = with(duration, &req);
    assert_eq!(h.count, 8);
    assert_eq!(h.buckets.iter().sum::<u64>(), 8);
    assert_eq!(
        h.bounds,
        [
            0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0,
            7500.0, 10000.0
        ]
    );
    assert_eq!(h.buckets.len(), 16);
    assert!(
        h.min >= 20.0 && h.max >= h.min && h.sum >= 8.0 * 20.0,
        "{h:?}"
    );
    // Rates: one integer counter, split by `condition`.
    let checks = metric(&last, "k6_checks");
    assert_eq!(checks.kind, "sum");
    assert_eq!(
        with(checks, &[("check", "ok"), ("condition", "nonzero")]).value,
        Some(8.0)
    );
    assert!(
        !checks
            .points
            .iter()
            .any(|p| p.attrs.get("condition").map(String::as_str) == Some("zero"))
    );
    // Gauges, and data units.
    assert_eq!(metric(&last, "k6_vus_max").kind, "gauge");
    assert_eq!(metric(&last, "k6_data_sent").unit, "By");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_url_can_be_given_directly_and_grpc_is_refused() {
    let server = TestServer::start().await.unwrap();
    let plan = write_plan(
        "otlp-url",
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
            format!("opentelemetry={}", server.url("/v1/metrics")),
        ],
        vec![],
    )
    .await;
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    let last = decode(&server.otlp_pushes().last().unwrap().body);
    assert_eq!(metric(&last, "http_reqs").points[0].value, Some(2.0));
    assert_eq!(last.resource["service.name"], "k9");

    for (key, value, expected) in [
        ("K6_OTEL_EXPORTER_PROTOCOL", "grpc", "grpc"),
        (
            "K6_OTEL_EXPORT_INTERVAL",
            "often",
            "K6_OTEL_EXPORT_INTERVAL",
        ),
        ("K6_OTEL_HEADERS", "novalue", "K6_OTEL_HEADERS"),
    ] {
        let out = k9(
            vec![
                "run".into(),
                plan.display().to_string(),
                "--out".into(),
                "opentelemetry".into(),
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
