//! Feeders: rows of test data (inline, CSV or JSON), loaded before the run.

use std::sync::atomic::{AtomicUsize, Ordering};

use indexmap::IndexMap;

use crate::Segment;
use crate::plan::{Feeder, FeederMode};

pub(crate) struct CFeeder {
    pub name: String,
    pub columns: Vec<String>,
    /// Values by row, then column; a missing value is empty.
    pub rows: Vec<Vec<String>>,
    mode: FeederMode,
    /// For [`FeederMode::Sequential`]: the rows this segment uses.
    owned: Vec<usize>,
    cursor: AtomicUsize,
}

impl CFeeder {
    pub fn load(name: &str, spec: &Feeder, segment: &Segment) -> Result<CFeeder, String> {
        let objects = match (&spec.file, spec.rows.is_empty()) {
            (Some(file), true) => read_file(file)?,
            (None, _) => Table::Objects(spec.rows.clone()),
            (Some(_), false) => return Err("give either `file` or `rows`".to_owned()),
        };
        let (columns, rows) = match objects {
            Table::Csv(columns, rows) => (columns, rows),
            Table::Objects(objects) => table(objects),
        };
        if rows.is_empty() {
            return Err("no rows".to_owned());
        }
        // Workers take turns (as they do with VU slots), so each row is used
        // by one worker; with fewer rows than workers, they share them all.
        let mut owned: Vec<usize> = (0..rows.len())
            .filter(|&i| segment.owns(i as u64))
            .collect();
        if owned.is_empty() {
            owned = (0..rows.len()).collect();
        }
        Ok(CFeeder {
            name: name.to_owned(),
            columns,
            rows,
            mode: spec.mode,
            owned,
            cursor: AtomicUsize::new(0),
        })
    }

    /// A row for an iteration of VU `vu`.
    pub fn pick(&self, vu: u64) -> usize {
        match self.mode {
            FeederMode::Sequential => {
                let n = self.cursor.fetch_add(1, Ordering::Relaxed);
                self.owned[n % self.owned.len()]
            }
            FeederMode::Random => fastrand::usize(..self.rows.len()),
            FeederMode::PerVu => (vu.saturating_sub(1) % self.rows.len() as u64) as usize,
        }
    }
}

/// A feeder file's rows as objects (CSV values as strings, in column order).
pub fn rows(file: &str) -> Result<Vec<IndexMap<String, serde_json::Value>>, String> {
    Ok(match read_file(file)? {
        Table::Objects(objects) => objects,
        Table::Csv(columns, records) => records
            .into_iter()
            .map(|record| {
                columns
                    .iter()
                    .cloned()
                    .zip(record.into_iter().map(serde_json::Value::String))
                    .collect()
            })
            .collect(),
    })
}

enum Table {
    Csv(Vec<String>, Vec<Vec<String>>),
    Objects(Vec<IndexMap<String, serde_json::Value>>),
}

fn read_file(file: &str) -> Result<Table, String> {
    let text = std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}"))?;
    let lower = file.to_ascii_lowercase();
    if lower.ends_with(".csv") {
        let mut records = parse_csv(&text).map_err(|e| format!("{file}: {e}"))?;
        if records.is_empty() {
            return Ok(Table::Csv(Vec::new(), Vec::new()));
        }
        let columns = records.remove(0);
        for (i, record) in records.iter().enumerate() {
            if record.len() != columns.len() {
                return Err(format!(
                    "{file}: record {} has {} fields, the header has {}",
                    i + 2,
                    record.len(),
                    columns.len()
                ));
            }
        }
        Ok(Table::Csv(columns, records))
    } else if lower.ends_with(".json") {
        serde_json::from_str::<Vec<IndexMap<String, serde_json::Value>>>(&text)
            .map(Table::Objects)
            .map_err(|e| format!("{file}: expected an array of objects: {e}"))
    } else {
        Err(format!("{file}: use a .csv or .json file"))
    }
}

/// Columns in order of first appearance; strings as they are, other JSON
/// values as JSON text, `null` and missing keys as empty.
fn table(objects: Vec<IndexMap<String, serde_json::Value>>) -> (Vec<String>, Vec<Vec<String>>) {
    let mut columns: IndexMap<String, ()> = IndexMap::new();
    for object in &objects {
        for key in object.keys() {
            columns.entry(key.clone()).or_default();
        }
    }
    let rows = objects
        .iter()
        .map(|object| {
            columns
                .keys()
                .map(|c| match object.get(c) {
                    None | Some(serde_json::Value::Null) => String::new(),
                    Some(serde_json::Value::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                })
                .collect()
        })
        .collect();
    (columns.into_keys().collect(), rows)
}

/// RFC 4180 CSV: quoted fields may hold commas, newlines and `""` (a quote).
/// Blank lines are skipped.
fn parse_csv(text: &str) -> Result<Vec<Vec<String>>, String> {
    let mut records = Vec::new();
    let mut record = Vec::new();
    let mut field = String::new();
    let mut chars = text
        .strip_prefix('\u{feff}')
        .unwrap_or(text)
        .chars()
        .peekable();
    let mut line = 1;
    let mut quoted_field = false;
    while let Some(c) = chars.next() {
        match c {
            '"' if field.is_empty() && !quoted_field => {
                quoted_field = true;
                loop {
                    match chars.next() {
                        Some('"') if chars.peek() == Some(&'"') => {
                            chars.next();
                            field.push('"');
                        }
                        Some('"') => break,
                        Some(c) => {
                            line += usize::from(c == '\n');
                            field.push(c);
                        }
                        None => return Err(format!("line {line}: unclosed quote")),
                    }
                }
            }
            ',' => {
                record.push(std::mem::take(&mut field));
                quoted_field = false;
            }
            '\r' if chars.peek() == Some(&'\n') => {}
            '\n' => {
                line += 1;
                if !record.is_empty() || !field.is_empty() || quoted_field {
                    record.push(std::mem::take(&mut field));
                    records.push(std::mem::take(&mut record));
                }
                quoted_field = false;
            }
            c if quoted_field => {
                return Err(format!("line {line}: `{c}` after a closing quote"));
            }
            c => field.push(c),
        }
    }
    if !record.is_empty() || !field.is_empty() || quoted_field {
        record.push(field);
        records.push(record);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_handles_quotes_newlines_and_blank_lines() {
        let records =
            parse_csv("a,b\r\n\"x,1\",\"say \"\"hi\"\"\"\n\n\"multi\nline\",\n,z").unwrap();
        assert_eq!(
            records,
            vec![
                vec!["a", "b"],
                vec!["x,1", "say \"hi\""],
                vec!["multi\nline", ""],
                vec!["", "z"],
            ]
        );
        assert!(parse_csv("\"open").unwrap_err().contains("unclosed"));
        assert!(
            parse_csv("\"a\"b")
                .unwrap_err()
                .contains("after a closing quote")
        );
    }

    #[test]
    fn objects_become_columns_in_order() {
        let objects: Vec<IndexMap<String, serde_json::Value>> =
            serde_json::from_str(r#"[{"a": "x", "b": 1}, {"c": true, "a": null}]"#).unwrap();
        let (columns, rows) = table(objects);
        assert_eq!(columns, ["a", "b", "c"]);
        assert_eq!(rows, vec![vec!["x", "1", ""], vec!["", "", "true"]]);
    }

    fn feeder(n: usize, mode: FeederMode, segment: &Segment) -> CFeeder {
        let spec = Feeder {
            file: None,
            rows: (0..n)
                .map(|i| IndexMap::from([("i".to_owned(), serde_json::json!(i))]))
                .collect(),
            mode,
        };
        CFeeder::load("f", &spec, segment).unwrap()
    }

    #[test]
    fn modes_pick_rows() {
        let full = Segment::full();
        let seq = feeder(3, FeederMode::Sequential, &full);
        assert_eq!(
            (0..5).map(|_| seq.pick(1)).collect::<Vec<_>>(),
            [0, 1, 2, 0, 1]
        );
        let per_vu = feeder(3, FeederMode::PerVu, &full);
        assert_eq!([1, 2, 3, 4, 1].map(|vu| per_vu.pick(vu)), [0, 1, 2, 0, 0]);
        let random = feeder(3, FeederMode::Random, &full);
        assert!((0..50).all(|_| random.pick(1) < 3));
    }

    #[test]
    fn segments_split_sequential_rows() {
        let mut seen = Vec::new();
        for segment in Segment::split(3) {
            let f = feeder(7, FeederMode::Sequential, &segment);
            let mut mine: Vec<usize> = f.owned.clone();
            assert_eq!(f.pick(1), mine[0]);
            seen.append(&mut mine);
        }
        seen.sort();
        assert_eq!(seen, (0..7).collect::<Vec<_>>());
        // Fewer rows than segments: each segment uses them all.
        let f = feeder(1, FeederMode::Sequential, &Segment::split(3)[2]);
        assert_eq!(f.pick(1), 0);
    }
}
