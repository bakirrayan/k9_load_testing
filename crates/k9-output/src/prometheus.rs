//! `--out prometheus-rw[=URL]`: Prometheus remote write, laid out like k6's
//! `experimental-prometheus-rw` output so its Grafana dashboards work as they
//! are. Every push carries each series' value since the start of the run:
//!
//! - counters: `k6_<name>_total`; gauges: `k6_<name>`; rates: `k6_<name>_rate`
//!   (the ratio);
//! - trends: `k6_<name>_<stat>` per configured stat (`p(99)` → `_p99`,
//!   `p(99.9)` → `_p999`), with times in seconds.
//!
//! Tags become labels (empty ones are left out), plus static labels.
//!
//! With `TREND_AS_NATIVE_HISTOGRAM`, trends are native histograms instead
//! (`k6_<name>_seconds` for times, schema 8 as k6's bucket factor gives).
//! With `STALE_MARKERS`, a last push marks every series stale.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use k9_metrics::{MetricKind, Metrics, Sample, SeriesKey, TrendValue, ValueType};

use crate::push::{
    Sender, Settings, bytes_field, check_headers, fixed64_field, pairs, varint, varint_field,
};
use crate::{Output, TrendStat};

const DEFAULT_URL: &str = "http://localhost:9090/api/v1/write";
const PREFIX: &str = "k6_";
/// Prometheus' staleness marker.
const STALE_NAN: u64 = 0x7ff0_0000_0000_0002;
/// Buckets grow by 2^(2^-8) ≈ 1.00271 (k6's bucket factor).
const SCHEMA: i32 = 8;
/// client_golang's default: values up to 2^-128 count as zero.
const ZERO_THRESHOLD: f64 = 2.938735877055719e-39;

/// A trend value to send: one of the summary's stats, or the sum.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stat {
    Summary(TrendStat),
    Sum,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrometheusConfig {
    pub url: String,
    pub push_interval: Duration,
    trend_stats: Vec<Stat>,
    pub insecure_skip_tls_verify: bool,
    /// Extra request headers, including authorization.
    pub headers: Vec<(String, String)>,
    /// Labels on every series, unless a tag has the same name.
    pub labels: Vec<(String, String)>,
    pub trend_as_native_histogram: bool,
    pub stale_markers: bool,
}

impl PrometheusConfig {
    /// k6's settings: `K6_PROMETHEUS_RW_*` variables, each of which can also
    /// be given as `K9_PROMETHEUS_RW_*` (which wins). `url` (from
    /// `--out prometheus-rw=URL`) beats both.
    pub fn from_env(
        url: Option<&str>,
        vars: impl IntoIterator<Item = (String, String)>,
    ) -> Result<PrometheusConfig, String> {
        let settings = Settings::from_vars("PROMETHEUS_RW", vars);
        let mut config = PrometheusConfig {
            url: url
                .filter(|u| !u.is_empty())
                .map(str::to_owned)
                .or_else(|| settings.get("SERVER_URL").map(|(_, v)| v.to_owned()))
                .unwrap_or_else(|| DEFAULT_URL.to_owned()),
            push_interval: settings
                .duration("PUSH_INTERVAL")?
                .unwrap_or(Duration::from_secs(5)),
            trend_stats: vec![Stat::Summary(TrendStat::Percentile(99.0))],
            insecure_skip_tls_verify: settings.bool("INSECURE_SKIP_TLS_VERIFY")?.unwrap_or(false),
            headers: Vec::new(),
            labels: Vec::new(),
            trend_as_native_histogram: settings.bool("TREND_AS_NATIVE_HISTOGRAM")?.unwrap_or(false),
            stale_markers: settings.bool("STALE_MARKERS")?.unwrap_or(false),
        };
        if let Some((_, value)) = settings.get("TREND_STATS") {
            config.trend_stats = value
                .split(',')
                .map(|s| match s.trim() {
                    "sum" => Ok(Stat::Sum),
                    other => TrendStat::parse(other).map(Stat::Summary),
                })
                .collect::<Result<_, _>>()?;
        }
        match (
            settings.get("USERNAME"),
            settings.get("PASSWORD"),
            settings.get("BEARER_TOKEN"),
        ) {
            (_, _, Some((_, token))) => {
                config
                    .headers
                    .push(("Authorization".into(), format!("Bearer {token}")));
            }
            (Some((_, user)), password, None) => {
                config
                    .headers
                    .push(basic_auth(user, password.map_or("", |(_, p)| p)));
            }
            _ => {}
        }
        if let Some((name, value)) = settings.get("HTTP_HEADERS") {
            config.headers.extend(pairs(name, value, ':')?);
        }
        config.headers.extend(settings.prefixed("HEADERS_"));
        check_headers("prometheus-rw", &config.headers)?;
        if let Some((name, value)) = settings.get("LABELS") {
            config.labels = pairs(name, value, '=')?
                .into_iter()
                .map(|(k, v)| (label_name(&k), v))
                .collect();
        }
        Ok(config)
    }
}

pub(crate) fn basic_auth(user: &str, password: &str) -> (String, String) {
    let credentials = format!("{user}:{password}");
    (
        "Authorization".into(),
        format!("Basic {}", crate::base64(credentials.as_bytes())),
    )
}

/// A valid Prometheus label name: other characters become `_`.
fn label_name(name: &str) -> String {
    let mut out: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.starts_with(|c: char| c.is_ascii_digit()) || out.is_empty() {
        out.insert(0, '_');
    }
    out
}

/// A series' value since the start of the run.
enum Total {
    Counter(f64),
    Gauge(f64),
    Rate { trues: u64, total: u64 },
    Trend(TrendValue),
    Native(Native),
}

/// A native histogram's state since the start of the run.
#[derive(Default)]
struct Native {
    count: u64,
    sum: f64,
    zero: u64,
    positive: BTreeMap<i32, u64>,
    negative: BTreeMap<i32, u64>,
}

impl Native {
    fn record(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        self.count += 1;
        self.sum += value;
        if value.abs() <= ZERO_THRESHOLD {
            self.zero += 1;
        } else {
            let buckets = if value > 0.0 {
                &mut self.positive
            } else {
                &mut self.negative
            };
            *buckets.entry(bucket(value.abs())).or_default() += 1;
        }
    }
}

/// The index `i` of the bucket (2^((i-1)/2^SCHEMA), 2^(i/2^SCHEMA)] holding
/// `value` (> 0).
fn bucket(value: f64) -> i32 {
    let scale = f64::from(1 << SCHEMA);
    let upper = |i: i32| (f64::from(i) / scale).exp2();
    let mut i = (value.log2() * scale).ceil() as i32;
    while upper(i - 1) >= value {
        i -= 1;
    }
    while upper(i) < value {
        i += 1;
    }
    i
}

pub struct PrometheusOutput {
    config: PrometheusConfig,
    totals: HashMap<SeriesKey, Total>,
    sender: Sender,
}

impl PrometheusOutput {
    pub fn start(config: PrometheusConfig) -> io::Result<PrometheusOutput> {
        let mut headers: Vec<(String, String)> = [
            ("Content-Encoding", "snappy"),
            ("Content-Type", "application/x-protobuf"),
            ("X-Prometheus-Remote-Write-Version", "0.1.0"),
        ]
        .iter()
        .map(|(n, v)| (n.to_string(), v.to_string()))
        .collect();
        headers.extend(config.headers.iter().cloned());
        let sender = Sender::start(
            "prometheus-rw",
            &config.url,
            &headers,
            config.insecure_skip_tls_verify,
            config.push_interval,
            None,
        )?;
        Ok(PrometheusOutput {
            config,
            totals: HashMap::new(),
            sender,
        })
    }

    /// The current totals as a snappy-compressed `WriteRequest`, or (if
    /// `stale`) a staleness marker for each of their series.
    fn encode(&self, metrics: &Metrics, stale: bool) -> Option<Vec<u8>> {
        if self.totals.is_empty() {
            return None;
        }
        let registry = metrics.registry();
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mut request = Vec::new();
        for (key, total) in &self.totals {
            let def = registry.def(key.metric);
            let tags = metrics.interner().get(key.tags);
            let mut labels: Vec<(String, String)> = tags
                .iter()
                .filter(|(_, v)| !v.is_empty())
                .map(|(k, v)| (label_name(k), v.to_owned()))
                .collect();
            for (k, v) in &self.config.labels {
                if !labels.iter().any(|(existing, _)| existing == k) {
                    labels.push((k.clone(), v.clone()));
                }
            }
            let time = def.value_type == ValueType::Time;
            let named = |suffix: &str| {
                let mut all = labels.clone();
                all.push((
                    "__name__".to_owned(),
                    format!("{PREFIX}{}{suffix}", def.name),
                ));
                all.sort();
                all
            };
            let mut series = |suffix: &str, value: f64| {
                let value = if stale {
                    f64::from_bits(STALE_NAN)
                } else {
                    value
                };
                write_series(&mut request, &named(suffix), Some(value), None, millis);
            };
            match total {
                Total::Counter(sum) => series("_total", *sum),
                Total::Gauge(value) => series("", *value),
                Total::Rate { trues, total } => series(
                    "_rate",
                    if *total == 0 {
                        0.0
                    } else {
                        *trues as f64 / *total as f64
                    },
                ),
                Total::Native(native) => {
                    let suffix = if time { "_seconds" } else { "" };
                    if stale {
                        series(suffix, 0.0);
                    } else {
                        let histogram = encode_histogram(native, millis);
                        write_series(&mut request, &named(suffix), None, Some(&histogram), millis);
                    }
                }
                Total::Trend(t) => {
                    let seconds = |v: f64| if time { v / 1e3 } else { v };
                    for stat in &self.config.trend_stats {
                        match stat {
                            Stat::Sum => series("_sum", seconds(t.sum)),
                            Stat::Summary(TrendStat::Count) => series("_count", t.count as f64),
                            Stat::Summary(s) => {
                                let suffix = s.to_string().replace(['(', ')', '.'], "");
                                series(&format!("_{suffix}"), seconds(s.value(t)));
                            }
                        }
                    }
                }
            }
        }
        Some(
            snap::raw::Encoder::new()
                .compress_vec(&request)
                .expect("snappy compresses any input"),
        )
    }
}

impl Output for PrometheusOutput {
    fn description(&self) -> String {
        format!("prometheus-rw ({})", self.config.url)
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
                        MetricKind::Rate => Total::Rate { trues: 0, total: 0 },
                        MetricKind::Trend if self.config.trend_as_native_histogram => {
                            Total::Native(Native::default())
                        }
                        MetricKind::Trend => Total::Trend(TrendValue::new()),
                    });
            match total {
                Total::Counter(sum) => *sum += sample.value,
                Total::Gauge(value) => *value = sample.value,
                Total::Rate { trues, total } => {
                    *trues += u64::from(sample.value != 0.0);
                    *total += 1;
                }
                Total::Trend(t) => t.record(sample.value),
                Total::Native(native) => {
                    let time = registry.def(sample.metric).value_type == ValueType::Time;
                    native.record(if time {
                        sample.value / 1e3
                    } else {
                        sample.value
                    });
                }
            }
        }
        if self.sender.due() {
            let body = self.encode(metrics, false);
            let _ = self.sender.push(body, false);
        }
        Ok(())
    }

    fn stop(&mut self, metrics: &Metrics) -> io::Result<()> {
        let body = self.encode(metrics, false);
        let _ = self.sender.push(body, true);
        if self.config.stale_markers {
            let body = self.encode(metrics, true);
            let _ = self.sender.push(body, true);
        }
        self.sender.finish()
    }
}

// The `WriteRequest` protobuf: repeated TimeSeries timeseries = 1;
// TimeSeries { repeated Label labels = 1; repeated Sample samples = 2; }
// Label { string name = 1; string value = 2; }
// Sample { double value = 1; int64 timestamp = 2; }
// TimeSeries.histograms = 4: Histogram { uint64 count_int = 1; double sum = 3;
//   sint32 schema = 4; double zero_threshold = 5; uint64 zero_count_int = 6;
//   negative_spans = 8; packed sint64 negative_deltas = 9; positive_spans = 11;
//   packed sint64 positive_deltas = 12; int64 timestamp = 15; }
// BucketSpan { sint32 offset = 1; uint32 length = 2; }

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn encode_histogram(native: &Native, millis: i64) -> Vec<u8> {
    let mut out = Vec::new();
    varint_field(&mut out, 1, native.count);
    fixed64_field(&mut out, 3, native.sum.to_bits());
    varint_field(&mut out, 4, zigzag(i64::from(SCHEMA)));
    fixed64_field(&mut out, 5, ZERO_THRESHOLD.to_bits());
    varint_field(&mut out, 6, native.zero);
    for (buckets, spans_field, deltas_field) in
        [(&native.negative, 8, 9), (&native.positive, 11, 12)]
    {
        if buckets.is_empty() {
            continue;
        }
        // Spans of consecutive buckets: the first offset is the first index,
        // later ones the gap since the previous span.
        let mut spans: Vec<(i32, u32)> = Vec::new();
        let mut next = None;
        for &index in buckets.keys() {
            match (next, spans.last_mut()) {
                (Some(n), Some(span)) if n == index => span.1 += 1,
                (Some(n), _) => spans.push((index - n, 1)),
                (None, _) => spans.push((index, 1)),
            }
            next = Some(index + 1);
        }
        for (offset, length) in spans {
            let mut span = Vec::new();
            varint_field(&mut span, 1, zigzag(i64::from(offset)));
            varint_field(&mut span, 2, u64::from(length));
            bytes_field(&mut out, spans_field, &span);
        }
        // Each count as a delta from the previous bucket's.
        let mut deltas = Vec::new();
        let mut previous = 0i64;
        for &count in buckets.values() {
            varint(&mut deltas, zigzag(count as i64 - previous));
            previous = count as i64;
        }
        bytes_field(&mut out, deltas_field, &deltas);
    }
    varint_field(&mut out, 15, millis as u64);
    out
}

/// A series with a float sample or a histogram.
fn write_series(
    out: &mut Vec<u8>,
    labels: &[(String, String)],
    value: Option<f64>,
    histogram: Option<&[u8]>,
    millis: i64,
) {
    let mut series = Vec::new();
    for (name, label_value) in labels {
        let mut label = Vec::new();
        bytes_field(&mut label, 1, name.as_bytes());
        bytes_field(&mut label, 2, label_value.as_bytes());
        bytes_field(&mut series, 1, &label);
    }
    if let Some(value) = value {
        let mut sample = Vec::new();
        fixed64_field(&mut sample, 1, value.to_bits());
        varint_field(&mut sample, 2, millis as u64);
        bytes_field(&mut series, 2, &sample);
    }
    if let Some(histogram) = histogram {
        bytes_field(&mut series, 4, histogram);
    }
    bytes_field(out, 1, &series);
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
    fn the_flag_wins_over_settings_and_defaults_match_k6() {
        let vars = env(&[
            ("K6_PROMETHEUS_RW_SERVER_URL", "http://k6/"),
            ("K9_PROMETHEUS_RW_SERVER_URL", "http://k9/"),
        ]);
        let config = PrometheusConfig::from_env(None, vars.clone()).unwrap();
        assert_eq!(config.url, "http://k9/");
        let config = PrometheusConfig::from_env(Some("http://flag/"), vars).unwrap();
        assert_eq!(config.url, "http://flag/");
        let config = PrometheusConfig::from_env(None, env(&[])).unwrap();
        assert_eq!(config.url, DEFAULT_URL);
        assert_eq!(config.push_interval, Duration::from_secs(5));
        assert_eq!(
            config.trend_stats,
            [Stat::Summary(TrendStat::Percentile(99.0))]
        );
    }

    #[test]
    fn auth_headers_and_labels() {
        let config = PrometheusConfig::from_env(
            None,
            env(&[
                ("K6_PROMETHEUS_RW_BEARER_TOKEN", "t0k"),
                ("K6_PROMETHEUS_RW_HTTP_HEADERS", "X-A:1, X-B:2"),
                ("K6_PROMETHEUS_RW_LABELS", "test-id=7"),
            ]),
        )
        .unwrap();
        assert!(
            config
                .headers
                .contains(&("Authorization".into(), "Bearer t0k".into()))
        );
        assert!(config.headers.contains(&("X-B".into(), "2".into())));
        assert_eq!(config.labels, [("test_id".to_owned(), "7".to_owned())]);
    }

    #[test]
    fn buckets_are_upper_inclusive() {
        assert_eq!(bucket(1.0), 0);
        assert_eq!(bucket(2.0), 256);
        assert_eq!(bucket(0.5), -256);
        assert_eq!(bucket(1.001), 1);
        let i = bucket(0.02);
        assert!((f64::from(i - 1) / 256.0).exp2() < 0.02);
        assert!((f64::from(i) / 256.0).exp2() >= 0.02);
    }

    #[test]
    fn spans_and_deltas() {
        let mut native = Native::default();
        for v in [1.0, 1.0, 1.001, 2.0, 0.0] {
            native.record(v);
        }
        let encoded = encode_histogram(&native, 0);
        // Buckets 0 (2), 1 (1), then 256 (1): spans (0, 2) and (254, 1);
        // deltas 2, -1, 0.
        let span = |offset: i64, length: u64| {
            let mut span = Vec::new();
            varint_field(&mut span, 1, zigzag(offset));
            varint_field(&mut span, 2, length);
            let mut field = Vec::new();
            bytes_field(&mut field, 11, &span);
            field
        };
        let contains = |needle: &[u8]| encoded.windows(needle.len()).any(|w| w == needle);
        assert!(contains(&span(0, 2)));
        assert!(contains(&span(254, 1)));
        assert!(contains(&[12 << 3 | 2, 3, 4, 1, 0]));
        assert_eq!(native.zero, 1);
    }

    #[test]
    fn label_names_are_made_valid() {
        assert_eq!(label_name("expected_response"), "expected_response");
        assert_eq!(label_name("my-tag.x"), "my_tag_x");
        assert_eq!(label_name("1st"), "_1st");
    }
}
