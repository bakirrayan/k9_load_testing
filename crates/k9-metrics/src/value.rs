//! Aggregated metric values and snapshots.

use std::collections::HashMap;

use hdrhistogram::Histogram;

use crate::registry::MetricId;
use crate::tags::{Interner, TagSetId};

/// Trend values are stored in HDR histograms as integers in units of this size
/// (for time metrics, which are in milliseconds, that's 1 µs). Percentiles are
/// accurate to 3 significant digits; count, sum, min and max are exact.
pub const TREND_RESOLUTION: f64 = 0.001;

/// One time series: a metric plus a tag set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SeriesKey {
    pub metric: MetricId,
    pub tags: TagSetId,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Counter(CounterValue),
    Gauge(GaugeValue),
    Rate(RateValue),
    Trend(TrendValue),
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CounterValue {
    pub sum: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GaugeValue {
    /// The most recently recorded value.
    pub value: f64,
    pub min: f64,
    pub max: f64,
    /// Global recording order, used to find the latest value across threads.
    pub(crate) seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RateValue {
    /// Samples that were non-zero.
    pub trues: u64,
    pub total: u64,
}

impl RateValue {
    /// Share of non-zero samples in `0.0..=1.0` (0 when empty).
    pub fn rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.trues as f64 / self.total as f64
        }
    }
}

#[derive(Debug, Clone)]
pub struct TrendValue {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    /// Negative samples, which were counted as 0 in the histogram (min, max,
    /// sum and avg still use the real values).
    pub clamped: u64,
    pub(crate) hist: Histogram<u64>,
}

impl PartialEq for TrendValue {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count
            && self.sum == other.sum
            && self.min == other.min
            && self.max == other.max
            && self.clamped == other.clamped
            && self.hist == other.hist
    }
}

impl Default for TrendValue {
    fn default() -> Self {
        TrendValue::new()
    }
}

impl TrendValue {
    pub fn new() -> TrendValue {
        TrendValue {
            count: 0,
            sum: 0.0,
            min: 0.0,
            max: 0.0,
            clamped: 0,
            // Auto-resizing, 3 significant digits.
            hist: Histogram::new(3).expect("3 significant digits is a valid precision"),
        }
    }

    pub fn from_samples(samples: &[f64]) -> TrendValue {
        let mut trend = TrendValue::new();
        for &sample in samples {
            trend.record(sample);
        }
        trend
    }

    pub fn record(&mut self, value: f64) {
        if self.count == 0 {
            self.min = value;
            self.max = value;
        } else {
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
        self.count += 1;
        self.sum += value;
        let scaled = if value < 0.0 {
            self.clamped += 1;
            0
        } else {
            (value / TREND_RESOLUTION).round() as u64
        };
        // `record` grows the histogram as needed. Don't use `saturating_record`
        // for normal recording: it clamps to the current range instead of growing.
        if self.hist.record(scaled).is_err() {
            self.hist.saturating_record(scaled); // only if growing fails (huge values)
        }
    }

    pub fn merge(&mut self, other: &TrendValue) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            self.min = other.min;
            self.max = other.max;
        } else {
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
        }
        self.count += other.count;
        self.sum += other.sum;
        self.clamped += other.clamped;
        self.hist
            .add(&other.hist)
            .expect("auto-resizing histograms can absorb any other histogram");
    }

    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    pub fn med(&self) -> f64 {
        self.percentile(50.0)
    }

    /// The `p`th percentile (`p` in 0–100), e.g. `percentile(95.0)` for k6's `p(95)`.
    /// Exact at 0 and 100 (min and max); otherwise to 3 significant digits.
    pub fn percentile(&self, p: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        if p <= 0.0 {
            return self.min;
        }
        if p >= 100.0 {
            return self.max;
        }
        let bucket = self.hist.value_at_quantile(p / 100.0);
        let value = self.hist.median_equivalent(bucket) as f64 * TREND_RESOLUTION;
        // A bucket's midpoint can fall outside the observed range.
        value.clamp(self.min, self.max)
    }
}

/// Per-tick numbers kept for time-series outputs (histograms aren't kept per tick).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Stats {
    Counter {
        sum: f64,
    },
    Gauge {
        value: f64,
    },
    Rate {
        trues: u64,
        total: u64,
    },
    Trend {
        count: u64,
        avg: f64,
        min: f64,
        med: f64,
        max: f64,
        p90: f64,
        p95: f64,
        p99: f64,
    },
}

impl Value {
    /// A value with no samples, for reporting a series that never recorded.
    pub fn empty(kind: crate::registry::MetricKind) -> Value {
        use crate::registry::MetricKind;
        match kind {
            MetricKind::Counter => Value::Counter(CounterValue::default()),
            MetricKind::Gauge => Value::Gauge(GaugeValue {
                value: 0.0,
                min: 0.0,
                max: 0.0,
                seq: 0,
            }),
            MetricKind::Rate => Value::Rate(RateValue::default()),
            MetricKind::Trend => Value::Trend(TrendValue::new()),
        }
    }

    /// Folds `other` (same metric kind) into `self`.
    pub fn merge(&mut self, other: &Value) {
        match (self, other) {
            (Value::Counter(a), Value::Counter(b)) => a.sum += b.sum,
            (Value::Gauge(a), Value::Gauge(b)) => {
                if b.seq > a.seq {
                    a.value = b.value;
                    a.seq = b.seq;
                }
                a.min = a.min.min(b.min);
                a.max = a.max.max(b.max);
            }
            (Value::Rate(a), Value::Rate(b)) => {
                a.trues += b.trues;
                a.total += b.total;
            }
            (Value::Trend(a), Value::Trend(b)) => a.merge(b),
            (a, b) => debug_assert!(false, "merging different metric kinds: {a:?} and {b:?}"),
        }
    }

    pub fn stats(&self) -> Stats {
        match self {
            Value::Counter(c) => Stats::Counter { sum: c.sum },
            Value::Gauge(g) => Stats::Gauge { value: g.value },
            Value::Rate(r) => Stats::Rate {
                trues: r.trues,
                total: r.total,
            },
            Value::Trend(t) => Stats::Trend {
                count: t.count,
                avg: t.avg(),
                min: t.min,
                med: t.med(),
                max: t.max,
                p90: t.percentile(90.0),
                p95: t.percentile(95.0),
                p99: t.percentile(99.0),
            },
        }
    }
}

/// Values per series.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Snapshot {
    series: HashMap<SeriesKey, Value>,
}

impl Snapshot {
    pub fn new() -> Snapshot {
        Snapshot::default()
    }

    pub fn get(&self, metric: MetricId, tags: TagSetId) -> Option<&Value> {
        self.series.get(&SeriesKey { metric, tags })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SeriesKey, &Value)> {
        self.series.iter()
    }

    pub fn len(&self) -> usize {
        self.series.len()
    }

    pub fn is_empty(&self) -> bool {
        self.series.is_empty()
    }

    pub(crate) fn merge_value(&mut self, key: SeriesKey, value: &Value) {
        match self.series.get_mut(&key) {
            Some(existing) => existing.merge(value),
            None => {
                self.series.insert(key, value.clone());
            }
        }
    }

    pub(crate) fn insert(&mut self, key: SeriesKey, value: Value) {
        self.series.insert(key, value);
    }

    /// Like [`Snapshot::merge`], but moves values instead of cloning them.
    pub(crate) fn merge_owned(&mut self, other: Snapshot) {
        for (key, value) in other.series {
            match self.series.get_mut(&key) {
                Some(existing) => existing.merge(&value),
                None => {
                    self.series.insert(key, value);
                }
            }
        }
    }

    /// Folds every series of `other` into `self`.
    pub fn merge(&mut self, other: &Snapshot) {
        for (key, value) in &other.series {
            self.merge_value(*key, value);
        }
    }

    /// All series of `metric` whose tags match `filter`, merged into one value,
    /// e.g. `http_req_duration` overall, or `{scenario:checkout}` only.
    pub fn merged(
        &self,
        metric: MetricId,
        filter: &[(&str, &str)],
        interner: &Interner,
    ) -> Option<Value> {
        let mut merged: Option<Value> = None;
        for (key, value) in &self.series {
            if key.metric != metric || !interner.get(key.tags).matches(filter) {
                continue;
            }
            match &mut merged {
                Some(m) => m.merge(value),
                None => merged = Some(value.clone()),
            }
        }
        merged
    }
}
