//! Reporting: the k6-style end-of-test summary, and outputs that stream
//! samples during the run (`--out`: JSON, CSV, Prometheus remote write, OTLP,
//! InfluxDB and StatsD), and the HTML report.

mod csv;
mod export;
pub mod format;
mod influxdb;
mod json;
mod otlp;
mod prometheus;
mod push;
mod report;
mod stats;
mod statsd;
mod summary;

use std::io;

use k9_metrics::{Metrics, Sample};

pub use csv::CsvOutput;
pub use export::{SummaryState, summary_data, summary_export};
pub use format::TimeUnit;
pub use influxdb::{InfluxConfig, InfluxOutput, InfluxVersion};
pub use json::JsonOutput;
pub use otlp::{OtlpConfig, OtlpOutput};
pub use prometheus::{PrometheusConfig, PrometheusOutput};
use report::base64;
pub use report::{ReportInfo, html_report};
pub use stats::TrendStat;
pub use statsd::{StatsdConfig, StatsdFlavor, StatsdOutput};
pub use summary::{
    RunHeader, ScenarioLine, SummaryData, ThresholdResult, render_checks, render_header,
    render_metrics, status_line,
};

/// A destination for raw samples, fed once per aggregation tick.
pub trait Output: Send {
    /// Shown in the run header, e.g. `json (results.jsonl)`.
    fn description(&self) -> String;
    fn add_samples(&mut self, samples: &[Sample], metrics: &Metrics) -> io::Result<()>;
    /// Flushes and closes; called once after the last samples.
    fn stop(&mut self, metrics: &Metrics) -> io::Result<()>;
}
