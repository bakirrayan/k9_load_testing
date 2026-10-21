//! `--out csv=path`: every sample as a CSV row, in k6's layout.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use k9_metrics::{Metrics, Sample};

use crate::Output;

/// k6's tag columns, between `metric_value` and `extra_tags`.
const TAG_COLUMNS: [&str; 14] = [
    "check",
    "error",
    "error_code",
    "expected_response",
    "group",
    "method",
    "name",
    "proto",
    "scenario",
    "service",
    "status",
    "subproto",
    "tls_version",
    "url",
];

pub struct CsvOutput {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl CsvOutput {
    pub fn create(path: &Path) -> io::Result<CsvOutput> {
        let mut writer = BufWriter::new(File::create(path)?);
        writeln!(
            writer,
            "metric_name,timestamp,metric_value,{},extra_tags,metadata",
            TAG_COLUMNS.join(",")
        )?;
        Ok(CsvOutput {
            path: path.to_owned(),
            writer,
        })
    }
}

impl Output for CsvOutput {
    fn description(&self) -> String {
        format!("csv ({})", self.path.display())
    }

    fn add_samples(&mut self, samples: &[Sample], metrics: &Metrics) -> io::Result<()> {
        let registry = metrics.registry();
        let mut row = String::new();
        for sample in samples {
            row.clear();
            let tags = metrics.interner().get(sample.tags);
            let secs = sample
                .time
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            field(&mut row, &registry.def(sample.metric).name);
            row.push_str(&format!(",{secs},{:.6}", sample.value));
            for column in TAG_COLUMNS {
                row.push(',');
                if let Some((_, value)) = tags.iter().find(|(k, _)| *k == column) {
                    field(&mut row, value);
                }
            }
            let extra: Vec<String> = tags
                .iter()
                .filter(|(k, _)| !TAG_COLUMNS.contains(k))
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            row.push(',');
            field(&mut row, &extra.join("&"));
            row.push(',');
            writeln!(self.writer, "{row}")?;
        }
        Ok(())
    }

    fn stop(&mut self, _metrics: &Metrics) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Appends a CSV field, quoted if it needs to be.
fn field(row: &mut String, value: &str) {
    if value.contains([',', '"', '\n', '\r']) {
        row.push('"');
        row.push_str(&value.replace('"', "\"\""));
        row.push('"');
    } else {
        row.push_str(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_are_quoted_only_when_needed() {
        let mut row = String::new();
        for value in ["plain", "a,b", "say \"hi\"", "two\nlines", ""] {
            field(&mut row, value);
            row.push('|');
        }
        assert_eq!(row, "plain|\"a,b\"|\"say \"\"hi\"\"\"|\"two\nlines\"||");
    }
}
