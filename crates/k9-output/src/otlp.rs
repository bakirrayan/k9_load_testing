//! `--out opentelemetry[=URL]`: OTLP metrics over HTTP (protobuf), like k6's
//! OpenTelemetry output, configured with its `K6_OTEL_*` settings. Every
//! push carries each series' value since the start of the run (cumulative
//! temporality, as the OpenTelemetry SDKs default to):
//!
//! - counters: monotonic sums; gauges: gauges;
//! - rates: one integer counter per metric, with a `condition` attribute
//!   (`zero` or `nonzero`);
//! - trends: histograms with the SDKs' default bucket bounds, in the metric's
//!   unit (`ms` for times, `By` for data).
//!
//! Tags become attributes (empty ones are left out); `service.name` and
//! `service.version` describe the resource. gRPC isn't supported yet.

use std::collections::HashMap;
use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use k9_metrics::{MetricId, MetricKind, Metrics, Sample, SeriesKey, TagSetId, ValueType};

use crate::Output;
use crate::prometheus::basic_auth;
use crate::push::{
    Sender, Settings, bytes_field, check_headers, fixed64_field, pairs, varint_field,
};

/// The OpenTelemetry SDKs' default explicit bucket bounds.
const BOUNDS: [f64; 15] = [
    0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0,
    10000.0,
];

#[derive(Debug, Clone, PartialEq)]
pub struct OtlpConfig {
    pub url: String,
    pub export_interval: Duration,
    pub metric_prefix: String,
    pub service_name: String,
    pub service_version: String,
    pub insecure_skip_tls_verify: bool,
    pub headers: Vec<(String, String)>,
}

impl OtlpConfig {
    /// k6's settings: `K6_OTEL_*` variables, each of which can also be given
    /// as `K9_OTEL_*` (which wins). `url` (from `--out opentelemetry=URL`)
    /// beats the endpoint settings.
    pub fn from_env(
        url: Option<&str>,
        vars: impl IntoIterator<Item = (String, String)>,
    ) -> Result<OtlpConfig, String> {
        let settings = Settings::from_vars("OTEL", vars);
        if let Some((name, protocol)) = settings.get("EXPORTER_PROTOCOL")
            && protocol != "http/protobuf"
        {
            return Err(format!(
                "{name}: {protocol:?} isn't supported yet; k9 sends OTLP as http/protobuf"
            ));
        }
        let url = match url.filter(|u| !u.is_empty()) {
            Some(url) => url.to_owned(),
            None => {
                let endpoint = settings
                    .get("HTTP_EXPORTER_ENDPOINT")
                    .map_or("localhost:4318", |(_, v)| v);
                let path = settings
                    .get("HTTP_EXPORTER_URL_PATH")
                    .map_or("/v1/metrics", |(_, v)| v);
                let insecure = settings.bool("HTTP_EXPORTER_INSECURE")?.unwrap_or(false);
                let scheme = if insecure { "http" } else { "https" };
                format!("{scheme}://{endpoint}{path}")
            }
        };
        let mut headers = Vec::new();
        if let Some((_, user)) = settings.get("HTTP_EXPORTER_USERNAME") {
            let password = settings
                .get("HTTP_EXPORTER_PASSWORD")
                .map_or("", |(_, p)| p);
            headers.push(basic_auth(user, password));
        }
        if let Some((name, value)) = settings.get("HEADERS") {
            headers.extend(pairs(name, value, '=')?);
        }
        check_headers("opentelemetry", &headers)?;
        Ok(OtlpConfig {
            url,
            export_interval: settings
                .duration("EXPORT_INTERVAL")?
                .unwrap_or(Duration::from_secs(10)),
            metric_prefix: settings
                .get("METRIC_PREFIX")
                .map_or(String::new(), |(_, v)| v.to_owned()),
            service_name: settings
                .get("SERVICE_NAME")
                .map_or("k9".to_owned(), |(_, v)| v.to_owned()),
            service_version: settings
                .get("SERVICE_VERSION")
                .map_or(env!("CARGO_PKG_VERSION").to_owned(), |(_, v)| v.to_owned()),
            insecure_skip_tls_verify: settings.bool("TLS_INSECURE_SKIP_VERIFY")?.unwrap_or(false),
            headers,
        })
    }
}

/// A series' value since the start of the run.
enum Total {
    Counter(f64),
    Gauge(f64),
    Rate { nonzero: u64, zero: u64 },
    Histogram(Histogram),
}

struct Histogram {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    buckets: [u64; BOUNDS.len() + 1],
}

impl Histogram {
    fn record(&mut self, v: f64) {
        if self.count == 0 {
            (self.min, self.max) = (v, v);
        } else {
            self.min = self.min.min(v);
            self.max = self.max.max(v);
        }
        self.count += 1;
        self.sum += v;
        // Bucket i holds (bounds[i-1], bounds[i]]; the last one the rest.
        let i = BOUNDS.partition_point(|b| *b < v);
        self.buckets[i] += 1;
    }
}

pub struct OtlpOutput {
    config: OtlpConfig,
    totals: HashMap<SeriesKey, Total>,
    /// When the run's cumulative values started, in Unix nanoseconds.
    start: u64,
    sender: Sender,
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

impl OtlpOutput {
    pub fn start(config: OtlpConfig) -> io::Result<OtlpOutput> {
        let mut headers = vec![(
            "Content-Type".to_owned(),
            "application/x-protobuf".to_owned(),
        )];
        headers.extend(config.headers.iter().cloned());
        let sender = Sender::start(
            "opentelemetry",
            &config.url,
            &headers,
            config.insecure_skip_tls_verify,
            config.export_interval,
            None,
        )?;
        Ok(OtlpOutput {
            config,
            totals: HashMap::new(),
            start: now_nanos(),
            sender,
        })
    }

    /// The current totals as an `ExportMetricsServiceRequest`.
    fn encode(&self, metrics: &Metrics) -> Option<Vec<u8>> {
        if self.totals.is_empty() {
            return None;
        }
        let now = now_nanos();
        let registry = metrics.registry();
        let mut by_metric: HashMap<MetricId, Vec<(TagSetId, &Total)>> = HashMap::new();
        for (key, total) in &self.totals {
            by_metric
                .entry(key.metric)
                .or_default()
                .push((key.tags, total));
        }
        let mut ids: Vec<&MetricId> = by_metric.keys().collect();
        ids.sort();
        let mut scope = Vec::new();
        let mut info = Vec::new();
        bytes_field(&mut info, 1, b"k9");
        bytes_field(&mut info, 2, env!("CARGO_PKG_VERSION").as_bytes());
        bytes_field(&mut scope, 1, &info);
        for id in ids {
            let def = registry.def(*id);
            let mut metric = Vec::new();
            let name = format!("{}{}", self.config.metric_prefix, def.name);
            bytes_field(&mut metric, 1, name.as_bytes());
            let unit = match def.value_type {
                ValueType::Time => "ms",
                ValueType::Data => "By",
                ValueType::Default => "",
            };
            if !unit.is_empty() {
                bytes_field(&mut metric, 3, unit.as_bytes());
            }
            let mut data = Vec::new();
            let mut field = 7; // sum
            for (tags, total) in &by_metric[id] {
                let set = metrics.interner().get(*tags);
                let mut attrs: Vec<(&str, &str)> =
                    set.iter().filter(|(_, v)| !v.is_empty()).collect();
                match total {
                    Total::Counter(sum) => {
                        number_point(
                            &mut data,
                            &attrs,
                            Some(self.start),
                            now,
                            Number::Double(*sum),
                        );
                    }
                    Total::Gauge(value) => {
                        field = 5;
                        number_point(&mut data, &attrs, None, now, Number::Double(*value));
                    }
                    Total::Rate { nonzero, zero } => {
                        for (condition, count) in [("nonzero", nonzero), ("zero", zero)] {
                            if *count > 0 {
                                attrs.push(("condition", condition));
                                attrs.sort();
                                let value = Number::Int(*count as i64);
                                number_point(&mut data, &attrs, Some(self.start), now, value);
                                attrs.retain(|(k, _)| *k != "condition");
                            }
                        }
                    }
                    Total::Histogram(h) => {
                        field = 9;
                        histogram_point(&mut data, &attrs, self.start, now, h);
                    }
                }
            }
            if field != 5 {
                varint_field(&mut data, 2, 2); // CUMULATIVE
            }
            if field == 7 {
                varint_field(&mut data, 3, 1); // is_monotonic
            }
            bytes_field(&mut metric, field, &data);
            bytes_field(&mut scope, 2, &metric);
        }
        let mut resource = Vec::new();
        key_value(&mut resource, 1, "service.name", &self.config.service_name);
        key_value(
            &mut resource,
            1,
            "service.version",
            &self.config.service_version,
        );
        let mut resource_metrics = Vec::new();
        bytes_field(&mut resource_metrics, 1, &resource);
        bytes_field(&mut resource_metrics, 2, &scope);
        let mut request = Vec::new();
        bytes_field(&mut request, 1, &resource_metrics);
        Some(request)
    }
}

impl Output for OtlpOutput {
    fn description(&self) -> String {
        format!("opentelemetry ({})", self.config.url)
    }

    fn add_samples(&mut self, samples: &[Sample], metrics: &Metrics) -> io::Result<()> {
        let registry = metrics.registry();
        for sample in samples {
            let key = SeriesKey {
                metric: sample.metric,
                tags: sample.tags,
            };
            let total =
                self.totals
                    .entry(key)
                    .or_insert_with(|| match registry.def(sample.metric).kind {
                        MetricKind::Counter => Total::Counter(0.0),
                        MetricKind::Gauge => Total::Gauge(0.0),
                        MetricKind::Rate => Total::Rate {
                            nonzero: 0,
                            zero: 0,
                        },
                        MetricKind::Trend => Total::Histogram(Histogram {
                            count: 0,
                            sum: 0.0,
                            min: 0.0,
                            max: 0.0,
                            buckets: [0; BOUNDS.len() + 1],
                        }),
                    });
            match total {
                Total::Counter(sum) => *sum += sample.value,
                Total::Gauge(value) => *value = sample.value,
                Total::Rate { nonzero, zero } => {
                    if sample.value != 0.0 {
                        *nonzero += 1;
                    } else {
                        *zero += 1;
                    }
                }
                Total::Histogram(h) => h.record(sample.value),
            }
        }
        if self.sender.due() {
            let body = self.encode(metrics);
            let _ = self.sender.push(body, false);
        }
        Ok(())
    }

    fn stop(&mut self, metrics: &Metrics) -> io::Result<()> {
        let body = self.encode(metrics);
        let _ = self.sender.push(body, true);
        self.sender.finish()
    }
}

// The OTLP messages used (opentelemetry-proto, metrics/v1 and common/v1):
// ResourceMetrics { Resource resource = 1; repeated ScopeMetrics scope_metrics = 2; }
// Resource { repeated KeyValue attributes = 1; }
// ScopeMetrics { InstrumentationScope scope = 1; repeated Metric metrics = 2; }
// Metric { string name = 1; string unit = 3; Gauge gauge = 5; Sum sum = 7; Histogram histogram = 9; }
// Sum { repeated NumberDataPoint data_points = 1; temporality = 2; bool is_monotonic = 3; }
// NumberDataPoint { attributes = 7; fixed64 start_time_unix_nano = 2; fixed64 time_unix_nano = 3;
//   double as_double = 4; sfixed64 as_int = 6; }
// HistogramDataPoint { attributes = 9; start = 2; time = 3; fixed64 count = 4; double sum = 5;
//   repeated fixed64 bucket_counts = 6; repeated double explicit_bounds = 7; double min = 11; double max = 12; }
// KeyValue { string key = 1; AnyValue value = 2; }  AnyValue { string string_value = 1; }

enum Number {
    Double(f64),
    Int(i64),
}

fn key_value(out: &mut Vec<u8>, field: u64, key: &str, value: &str) {
    let mut any = Vec::new();
    bytes_field(&mut any, 1, value.as_bytes());
    let mut kv = Vec::new();
    bytes_field(&mut kv, 1, key.as_bytes());
    bytes_field(&mut kv, 2, &any);
    bytes_field(out, field, &kv);
}

fn number_point(
    out: &mut Vec<u8>,
    attrs: &[(&str, &str)],
    start: Option<u64>,
    time: u64,
    value: Number,
) {
    let mut point = Vec::new();
    for (k, v) in attrs {
        key_value(&mut point, 7, k, v);
    }
    if let Some(start) = start {
        fixed64_field(&mut point, 2, start);
    }
    fixed64_field(&mut point, 3, time);
    match value {
        Number::Double(v) => fixed64_field(&mut point, 4, v.to_bits()),
        Number::Int(v) => fixed64_field(&mut point, 6, v as u64),
    }
    bytes_field(out, 1, &point);
}

fn histogram_point(
    out: &mut Vec<u8>,
    attrs: &[(&str, &str)],
    start: u64,
    time: u64,
    h: &Histogram,
) {
    let mut point = Vec::new();
    for (k, v) in attrs {
        key_value(&mut point, 9, k, v);
    }
    fixed64_field(&mut point, 2, start);
    fixed64_field(&mut point, 3, time);
    fixed64_field(&mut point, 4, h.count);
    fixed64_field(&mut point, 5, h.sum.to_bits());
    let buckets: Vec<u8> = h.buckets.iter().flat_map(|c| c.to_le_bytes()).collect();
    bytes_field(&mut point, 6, &buckets);
    let bounds: Vec<u8> = BOUNDS
        .iter()
        .flat_map(|b| b.to_bits().to_le_bytes())
        .collect();
    bytes_field(&mut point, 7, &bounds);
    fixed64_field(&mut point, 11, h.min.to_bits());
    fixed64_field(&mut point, 12, h.max.to_bits());
    bytes_field(out, 1, &point);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn the_url_is_built_like_k6s() {
        let config = OtlpConfig::from_env(None, env(&[])).unwrap();
        assert_eq!(config.url, "https://localhost:4318/v1/metrics");
        assert_eq!(config.export_interval, Duration::from_secs(10));
        assert_eq!(config.metric_prefix, "");
        let config = OtlpConfig::from_env(
            None,
            env(&[
                ("K6_OTEL_HTTP_EXPORTER_ENDPOINT", "collector:4318"),
                ("K6_OTEL_HTTP_EXPORTER_URL_PATH", "/otlp/v1/metrics"),
                ("K6_OTEL_HTTP_EXPORTER_INSECURE", "true"),
            ]),
        )
        .unwrap();
        assert_eq!(config.url, "http://collector:4318/otlp/v1/metrics");
        let config = OtlpConfig::from_env(Some("http://flag/"), env(&[])).unwrap();
        assert_eq!(config.url, "http://flag/");
    }

    #[test]
    fn histogram_buckets_follow_the_bounds() {
        let mut h = Histogram {
            count: 0,
            sum: 0.0,
            min: 0.0,
            max: 0.0,
            buckets: [0; BOUNDS.len() + 1],
        };
        for v in [0.0, 5.0, 5.1, 20000.0] {
            h.record(v);
        }
        assert_eq!(h.buckets[0], 1, "≤ 0");
        assert_eq!(h.buckets[1], 1, "(0, 5]");
        assert_eq!(h.buckets[2], 1, "(5, 10]");
        assert_eq!(h.buckets[15], 1, "> 10000");
        assert_eq!((h.min, h.max, h.count), (0.0, 20000.0, 4));
    }
}
