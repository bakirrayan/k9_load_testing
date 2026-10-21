//! `--out json=path`: every sample as a JSON line, in k6's format.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use k9_metrics::{MetricId, MetricKind, Metrics, Sample, ValueType};
use serde_json::json;

use crate::{Output, format};

pub struct JsonOutput {
    path: PathBuf,
    writer: BufWriter<File>,
    /// Metrics whose `"type":"Metric"` line has been written.
    announced: HashSet<MetricId>,
    /// Threshold expressions by metric name, listed in each metric's first line.
    thresholds: HashMap<String, Vec<String>>,
}

impl JsonOutput {
    pub fn create(path: &Path, thresholds: HashMap<String, Vec<String>>) -> io::Result<JsonOutput> {
        Ok(JsonOutput {
            path: path.to_owned(),
            writer: BufWriter::new(File::create(path)?),
            announced: HashSet::new(),
            thresholds,
        })
    }
}

impl Output for JsonOutput {
    fn description(&self) -> String {
        format!("json ({})", self.path.display())
    }

    fn add_samples(&mut self, samples: &[Sample], metrics: &Metrics) -> io::Result<()> {
        let registry = metrics.registry();
        for sample in samples {
            let def = registry.def(sample.metric);
            if self.announced.insert(sample.metric) {
                let line = json!({
                    "type": "Metric",
                    "data": {
                        "name": def.name,
                        "type": match def.kind {
                            MetricKind::Counter => "counter",
                            MetricKind::Gauge => "gauge",
                            MetricKind::Rate => "rate",
                            MetricKind::Trend => "trend",
                        },
                        "contains": match def.value_type {
                            ValueType::Default => "default",
                            ValueType::Time => "time",
                            ValueType::Data => "data",
                        },
                        "thresholds": self.thresholds.get(&def.name).cloned().unwrap_or_default(),
                        "submetrics": null,
                    },
                    "metric": def.name,
                });
                writeln!(self.writer, "{line}")?;
            }
            let tags: serde_json::Map<String, serde_json::Value> = metrics
                .interner()
                .get(sample.tags)
                .iter()
                .map(|(k, v)| (k.to_owned(), v.into()))
                .collect();
            let line = json!({
                "type": "Point",
                "data": {
                    "time": format::rfc3339(sample.time),
                    "value": sample.value,
                    "tags": tags,
                },
                "metric": def.name,
            });
            writeln!(self.writer, "{line}")?;
        }
        Ok(())
    }

    fn stop(&mut self, _metrics: &Metrics) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use k9_metrics::{Registry, TagSetId};

    use super::*;

    #[test]
    fn writes_k6_metric_and_point_lines() {
        let mut registry = Registry::new();
        let reqs = registry
            .register("http_reqs", MetricKind::Counter, ValueType::Default)
            .unwrap();
        let dur = registry
            .register("http_req_duration", MetricKind::Trend, ValueType::Time)
            .unwrap();
        let metrics = Metrics::new(registry);
        let tags = metrics
            .interner()
            .intern(&[("status", "200"), ("method", "GET")]);
        let t0 = SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_500);
        let sample = |metric, value, ms| Sample {
            metric,
            tags,
            value,
            time: t0 + Duration::from_millis(ms),
        };

        let path = std::env::temp_dir().join(format!("k9-json-{}.jsonl", std::process::id()));
        let mut out = JsonOutput::create(
            &path,
            HashMap::from([("http_req_duration".to_owned(), vec!["p(95)<500".to_owned()])]),
        )
        .unwrap();
        out.add_samples(&[sample(reqs, 1.0, 0), sample(dur, 101.5, 0)], &metrics)
            .unwrap();
        out.add_samples(
            &[Sample {
                tags: TagSetId::EMPTY,
                ..sample(reqs, 1.0, 1000)
            }],
            &metrics,
        )
        .unwrap();
        out.stop(&metrics).unwrap();

        let lines: Vec<serde_json::Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let _ = std::fs::remove_file(&path);
        assert_eq!(lines.len(), 5, "2 metric lines + 3 points: {lines:#?}");

        assert_eq!(lines[0]["type"], "Metric");
        assert_eq!(lines[0]["metric"], "http_reqs");
        assert_eq!(lines[0]["data"]["type"], "counter");
        assert_eq!(lines[0]["data"]["contains"], "default");
        assert_eq!(lines[0]["data"]["thresholds"], serde_json::json!([]));

        assert_eq!(lines[1]["type"], "Point");
        assert_eq!(lines[1]["metric"], "http_reqs");
        assert_eq!(lines[1]["data"]["value"], 1.0);
        assert_eq!(lines[1]["data"]["time"], "2023-11-14T22:13:20.500000000Z");
        assert_eq!(
            lines[1]["data"]["tags"],
            serde_json::json!({"method": "GET", "status": "200"})
        );

        assert_eq!(lines[2]["data"]["type"], "trend");
        assert_eq!(lines[2]["data"]["contains"], "time");
        assert_eq!(
            lines[2]["data"]["thresholds"],
            serde_json::json!(["p(95)<500"])
        );
        assert_eq!(lines[3]["data"]["value"], 101.5);

        assert_eq!(
            lines[4]["type"], "Point",
            "no second Metric line for http_reqs"
        );
        assert_eq!(lines[4]["data"]["tags"], serde_json::json!({}));
        assert_eq!(lines[4]["data"]["time"], "2023-11-14T22:13:21.500000000Z");
    }
}
