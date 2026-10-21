//! Recording. Each OS thread records into its own shard (an uncontended mutex);
//! the aggregator swaps shards out for empty ones.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, SystemTime};

use crate::registry::{MetricId, MetricKind, Registry};
use crate::tags::{Interner, TagSetId};
use crate::value::{CounterValue, GaugeValue, RateValue, SeriesKey, Snapshot, TrendValue, Value};

/// Distinguishes `Metrics` instances in the thread-local shard table.
static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// This thread's shard for each live `Metrics` instance.
    static SHARDS: RefCell<Vec<(u64, Weak<Mutex<Shard>>)>> = const { RefCell::new(Vec::new()) };
}

/// One recorded value with its timestamp, kept only when raw samples are
/// enabled ([`Metrics::with_samples`]) for streaming outputs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    pub metric: MetricId,
    pub tags: TagSetId,
    pub value: f64,
    pub time: SystemTime,
}

/// The recording side of a run's metrics. Share it via `Arc`.
pub struct Metrics {
    instance: u64,
    capture_samples: bool,
    registry: Registry,
    interner: Interner,
    /// Global order of gauge samples, so the latest value wins across threads.
    gauge_seq: AtomicU64,
    shards: Mutex<Vec<Arc<Mutex<Shard>>>>,
}

/// What one thread recorded since the last [`Metrics::take`].
#[derive(Default)]
struct Shard {
    series: HashMap<SeriesKey, Acc>,
    samples: Vec<Sample>,
}

/// Per-series accumulator. Trend samples stay raw until aggregation so that
/// threads never hold histograms.
enum Acc {
    Counter(f64),
    Gauge(GaugeValue),
    Rate(RateValue),
    Trend(Vec<f64>),
}

impl Acc {
    fn new(kind: MetricKind) -> Acc {
        match kind {
            MetricKind::Counter => Acc::Counter(0.0),
            MetricKind::Gauge => Acc::Gauge(GaugeValue {
                value: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
                seq: 0,
            }),
            MetricKind::Rate => Acc::Rate(RateValue::default()),
            MetricKind::Trend => Acc::Trend(Vec::new()),
        }
    }

    fn record(&mut self, value: f64, seq: u64) {
        match self {
            Acc::Counter(sum) => *sum += value,
            Acc::Gauge(g) => {
                if seq >= g.seq {
                    g.value = value;
                    g.seq = seq;
                }
                g.min = g.min.min(value);
                g.max = g.max.max(value);
            }
            Acc::Rate(r) => {
                r.trues += u64::from(value != 0.0);
                r.total += 1;
            }
            Acc::Trend(samples) => samples.push(value),
        }
    }

    /// Folds another thread's accumulator for the same series into this one.
    fn absorb(&mut self, other: Acc) {
        match (self, other) {
            (Acc::Trend(a), Acc::Trend(b)) => a.extend(b),
            (Acc::Counter(a), Acc::Counter(b)) => *a += b,
            (a, b) => {
                let mut value = a.to_value();
                value.merge(&b.into_value());
                *a = Acc::from_value(value);
            }
        }
    }

    fn into_value(self) -> Value {
        match self {
            Acc::Counter(sum) => Value::Counter(CounterValue { sum }),
            Acc::Gauge(g) => Value::Gauge(g),
            Acc::Rate(r) => Value::Rate(r),
            Acc::Trend(samples) => Value::Trend(TrendValue::from_samples(&samples)),
        }
    }

    /// Only used for gauges and rates, which are small and `Copy`.
    fn to_value(&self) -> Value {
        match self {
            Acc::Counter(sum) => Value::Counter(CounterValue { sum: *sum }),
            Acc::Gauge(g) => Value::Gauge(*g),
            Acc::Rate(r) => Value::Rate(*r),
            Acc::Trend(samples) => Value::Trend(TrendValue::from_samples(samples)),
        }
    }

    fn from_value(value: Value) -> Acc {
        match value {
            Value::Counter(c) => Acc::Counter(c.sum),
            Value::Gauge(g) => Acc::Gauge(g),
            Value::Rate(r) => Acc::Rate(r),
            Value::Trend(_) => unreachable!("trends are absorbed as raw samples"),
        }
    }
}

impl Metrics {
    pub fn new(registry: Registry) -> Metrics {
        Metrics::build(registry, false)
    }

    /// Like [`Metrics::new`], but also keeps every raw sample with a timestamp,
    /// for outputs that stream samples (JSON Lines, Prometheus, OTLP).
    pub fn with_samples(registry: Registry) -> Metrics {
        Metrics::build(registry, true)
    }

    fn build(registry: Registry, capture_samples: bool) -> Metrics {
        Metrics {
            instance: NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed),
            capture_samples,
            registry,
            interner: Interner::new(),
            gauge_seq: AtomicU64::new(1),
            shards: Mutex::new(Vec::new()),
        }
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn interner(&self) -> &Interner {
        &self.interner
    }

    /// Records one sample: counters add it, gauges set it, rates count it as
    /// true when non-zero, trends add it to the distribution.
    pub fn record(&self, metric: MetricId, tags: TagSetId, value: f64) {
        let kind = self.registry.def(metric).kind;
        let seq = if kind == MetricKind::Gauge {
            self.gauge_seq.fetch_add(1, Ordering::Relaxed)
        } else {
            0
        };
        let key = SeriesKey { metric, tags };
        let time = self.capture_samples.then(SystemTime::now);
        self.with_local_shard(|shard| {
            shard
                .series
                .entry(key)
                .or_insert_with(|| Acc::new(kind))
                .record(value, seq);
            if let Some(time) = time {
                shard.samples.push(Sample {
                    metric,
                    tags,
                    value,
                    time,
                });
            }
        });
    }

    /// Records a duration in milliseconds, the unit of k6's time metrics.
    pub fn record_duration(&self, metric: MetricId, tags: TagSetId, duration: Duration) {
        self.record(metric, tags, duration.as_secs_f64() * 1e3);
    }

    /// Takes everything recorded since the last call, across all threads.
    /// Recording threads are only blocked for the moment their shard is swapped.
    pub fn take(&self) -> Snapshot {
        self.take_with_samples().0
    }

    /// Like [`Metrics::take`], plus the raw samples (empty unless enabled),
    /// sorted by time.
    pub fn take_with_samples(&self) -> (Snapshot, Vec<Sample>) {
        let shards: Vec<_> = lock(&self.shards).clone();
        let mut merged: HashMap<SeriesKey, Acc> = HashMap::new();
        let mut samples = Vec::new();
        for shard in shards {
            let taken = std::mem::take(&mut *lock(&shard));
            samples.extend(taken.samples);
            for (key, acc) in taken.series {
                match merged.get_mut(&key) {
                    Some(existing) => existing.absorb(acc),
                    None => {
                        merged.insert(key, acc);
                    }
                }
            }
        }
        let mut snapshot = Snapshot::new();
        for (key, acc) in merged {
            snapshot.insert(key, acc.into_value());
        }
        samples.sort_by_key(|s| s.time);
        (snapshot, samples)
    }

    fn with_local_shard(&self, f: impl FnOnce(&mut Shard)) {
        SHARDS.with_borrow_mut(|local| {
            let found = local
                .iter()
                .find(|(instance, _)| *instance == self.instance)
                .and_then(|(_, shard)| shard.upgrade());
            let shard = found.unwrap_or_else(|| {
                // First sample from this thread: create and register its shard,
                // dropping entries for `Metrics` instances that no longer exist.
                local.retain(|(_, shard)| shard.strong_count() > 0);
                let shard = Arc::new(Mutex::new(Shard::default()));
                lock(&self.shards).push(shard.clone());
                local.push((self.instance, Arc::downgrade(&shard)));
                shard
            });
            f(&mut lock(&shard));
        });
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Shards hold plain data; a panic mid-record can't break their invariants.
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
