//! K9 metrics: what gets measured, how it's recorded on the hot path, and how
//! it's aggregated.
//!
//! - [`Registry`]: metric definitions, including k6's built-ins, fixed before a run.
//! - [`Interner`]: tag sets are stored once and referred to by [`TagSetId`].
//! - [`Metrics`]: recording. Each thread writes into its own shard, so VUs on
//!   different threads never contend. Counters, gauges and rates are folded in
//!   place; trend samples are buffered raw (8 bytes each) and only turned into
//!   HDR histograms by the aggregator, so no thread holds large histograms.
//! - [`Aggregator`]: swaps the shards out (once per second during a run),
//!   merges them into running totals and keeps a per-tick time series.
//!
//! - [`PortableSnapshot`]: snapshots by name, for sending between processes.
//! - [`threshold`]: k6's threshold language, parsed and evaluated against values.

mod aggregator;
mod metrics;
mod portable;
mod registry;
mod samples;
mod tags;
pub mod threshold;
mod value;

pub use aggregator::{Aggregator, Tick};
pub use metrics::{Metrics, Sample};
pub use portable::{PortableError, PortableSeries, PortableSnapshot, PortableValue};
pub use registry::{Builtins, MetricDef, MetricError, MetricId, MetricKind, Registry, ValueType};
pub use samples::{PortableSample, PortableSamples, SampleDecoder, SampleEncoder};
pub use tags::{Interner, TagSet, TagSetId};
pub use value::{
    CounterValue, GaugeValue, RateValue, SeriesKey, Snapshot, Stats, TREND_RESOLUTION, TrendValue,
    Value,
};
