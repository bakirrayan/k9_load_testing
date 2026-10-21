//! Running totals and a per-tick time series.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::metrics::{Metrics, Sample};
use crate::registry::MetricId;
use crate::value::{SeriesKey, Snapshot, Stats, Value};

/// One aggregation tick: what was recorded since the previous tick.
#[derive(Debug, Clone, PartialEq)]
pub struct Tick {
    /// Time since the run started.
    pub elapsed: Duration,
    pub stats: HashMap<SeriesKey, Stats>,
    /// Each metric merged across its tag sets (e.g. `http_req_duration`'s
    /// p(95) over every request), for charts.
    pub metrics: HashMap<MetricId, Stats>,
}

/// Owns the run's totals. During a run, the engine calls [`Aggregator::tick`]
/// once per second; the live UI, streaming outputs and thresholds read from it.
pub struct Aggregator {
    metrics: Arc<Metrics>,
    totals: Snapshot,
    ticks: Vec<Tick>,
    samples: Vec<Sample>,
    /// Absorbed from elsewhere (worker processes) since the last tick.
    pending: Snapshot,
}

impl Aggregator {
    pub fn new(metrics: Arc<Metrics>) -> Aggregator {
        Aggregator {
            metrics,
            totals: Snapshot::new(),
            ticks: Vec::new(),
            samples: Vec::new(),
            pending: Snapshot::new(),
        }
    }

    /// Collects everything recorded since the last tick into the totals and
    /// appends a [`Tick`] to the time series. Returns the new tick.
    pub fn tick(&mut self, elapsed: Duration) -> &Tick {
        self.tick_inner(elapsed, false);
        self.ticks.last().expect("just pushed")
    }

    /// Like [`Aggregator::tick`], also returning what the tick collected
    /// (e.g. for a worker to send to its manager).
    pub fn tick_with_delta(&mut self, elapsed: Duration) -> Snapshot {
        self.tick_inner(elapsed, true)
            .expect("asked to keep the delta")
    }

    /// Adds values recorded elsewhere (e.g. by worker processes, via
    /// [`crate::PortableSnapshot`]); they count from the next tick.
    pub fn absorb(&mut self, snapshot: Snapshot) {
        self.pending.merge_owned(snapshot);
    }

    fn tick_inner(&mut self, elapsed: Duration, keep_delta: bool) -> Option<Snapshot> {
        let (mut delta, samples) = self.metrics.take_with_samples();
        delta.merge_owned(std::mem::take(&mut self.pending));
        self.samples.extend(samples);
        let stats = delta
            .iter()
            .map(|(key, value)| (*key, value.stats()))
            .collect();
        let mut by_metric: HashMap<MetricId, Value> = HashMap::new();
        for (key, value) in delta.iter() {
            match by_metric.get_mut(&key.metric) {
                Some(merged) => merged.merge(value),
                None => {
                    by_metric.insert(key.metric, value.clone());
                }
            }
        }
        let metrics = by_metric
            .into_iter()
            .map(|(metric, value)| (metric, value.stats()))
            .collect();
        let kept = keep_delta.then(|| delta.clone());
        self.totals.merge_owned(delta);
        self.ticks.push(Tick {
            elapsed,
            stats,
            metrics,
        });
        kept
    }

    /// Raw samples collected by ticks since the last drain (only when the
    /// metrics were created with [`Metrics::with_samples`]).
    pub fn drain_samples(&mut self) -> Vec<Sample> {
        std::mem::take(&mut self.samples)
    }

    pub fn totals(&self) -> &Snapshot {
        &self.totals
    }

    pub fn ticks(&self) -> &[Tick] {
        &self.ticks
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}
