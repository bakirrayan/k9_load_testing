//! Snapshots that cross process boundaries. Metric ids and tag-set ids are
//! local to a process, so a [`PortableSnapshot`] names metrics and tags by
//! value; trend histograms travel as their recorded buckets, which rebuild an
//! identical histogram on the other side, so merging stays exact.

use serde::{Deserialize, Serialize};

use crate::registry::{MetricKind, Registry};
use crate::tags::Interner;
use crate::value::{CounterValue, GaugeValue, RateValue, SeriesKey, Snapshot, TrendValue, Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortableSnapshot {
    pub series: Vec<PortableSeries>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortableSeries {
    pub metric: String,
    /// Sorted by key.
    pub tags: Vec<(String, String)>,
    pub value: PortableValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PortableValue {
    Counter {
        sum: f64,
    },
    Gauge {
        value: f64,
        min: f64,
        max: f64,
    },
    Rate {
        trues: u64,
        total: u64,
    },
    Trend {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
        clamped: u64,
        /// (bucket value, count) for every bucket with samples, in the
        /// histogram's integer units.
        buckets: Vec<(u64, u64)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PortableError {
    #[error("the worker sent samples for unknown metric `{0}`")]
    UnknownMetric(String),
    #[error("the worker sent a {sent} for `{metric}`, which is a {here} here")]
    KindMismatch {
        metric: String,
        sent: MetricKind,
        here: MetricKind,
    },
}

impl Snapshot {
    /// This snapshot with metrics and tags by value, ready to send.
    pub fn to_portable(&self, registry: &Registry, interner: &Interner) -> PortableSnapshot {
        let series = self
            .iter()
            .map(|(key, value)| PortableSeries {
                metric: registry.def(key.metric).name.clone(),
                tags: interner
                    .get(key.tags)
                    .iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect(),
                value: portable_value(value),
            })
            .collect();
        PortableSnapshot { series }
    }
}

fn portable_value(value: &Value) -> PortableValue {
    match value {
        Value::Counter(c) => PortableValue::Counter { sum: c.sum },
        Value::Gauge(g) => PortableValue::Gauge {
            value: g.value,
            min: g.min,
            max: g.max,
        },
        Value::Rate(r) => PortableValue::Rate {
            trues: r.trues,
            total: r.total,
        },
        Value::Trend(t) => PortableValue::Trend {
            count: t.count,
            sum: t.sum,
            min: t.min,
            max: t.max,
            clamped: t.clamped,
            buckets: t
                .hist
                .iter_recorded()
                .map(|b| (b.value_iterated_to(), b.count_at_value()))
                .collect(),
        },
    }
}

impl PortableValue {
    fn kind(&self) -> MetricKind {
        match self {
            PortableValue::Counter { .. } => MetricKind::Counter,
            PortableValue::Gauge { .. } => MetricKind::Gauge,
            PortableValue::Rate { .. } => MetricKind::Rate,
            PortableValue::Trend { .. } => MetricKind::Trend,
        }
    }
}

impl PortableSnapshot {
    /// The snapshot in this process's registry and interner. When snapshots
    /// from several workers are merged, a gauge keeps the value from the one
    /// with the highest `order` (e.g. the order they arrived in).
    pub fn into_snapshot(
        self,
        registry: &Registry,
        interner: &Interner,
        order: u64,
    ) -> Result<Snapshot, PortableError> {
        let mut snapshot = Snapshot::new();
        for series in self.series {
            let metric = registry
                .get(&series.metric)
                .ok_or_else(|| PortableError::UnknownMetric(series.metric.clone()))?;
            let here = registry.def(metric).kind;
            let sent = series.value.kind();
            if here != sent {
                return Err(PortableError::KindMismatch {
                    metric: series.metric,
                    sent,
                    here,
                });
            }
            let tags: Vec<(&str, &str)> = series
                .tags
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let key = SeriesKey {
                metric,
                tags: interner.intern(&tags),
            };
            snapshot.merge_value(key, &local_value(series.value, order));
        }
        Ok(snapshot)
    }
}

fn local_value(value: PortableValue, order: u64) -> Value {
    match value {
        PortableValue::Counter { sum } => Value::Counter(CounterValue { sum }),
        PortableValue::Gauge { value, min, max } => Value::Gauge(GaugeValue {
            value,
            min,
            max,
            seq: order,
        }),
        PortableValue::Rate { trues, total } => Value::Rate(RateValue { trues, total }),
        PortableValue::Trend {
            count,
            sum,
            min,
            max,
            clamped,
            buckets,
        } => {
            let mut hist = TrendValue::new().hist;
            for (bucket, n) in buckets {
                // A bucket's value records back into the same bucket.
                if hist.record_n(bucket, n).is_err() {
                    hist.saturating_record_n(bucket, n);
                }
            }
            Value::Trend(TrendValue {
                count,
                sum,
                min,
                max,
                clamped,
                hist,
            })
        }
    }
}
