//! The end-of-test summary, laid out like k6's.

use std::collections::BTreeMap;
use std::time::Duration;

use k9_metrics::{Interner, Registry, Snapshot, Value, ValueType};

use crate::{TrendStat, format};

/// What the header describes: the run before it starts.
#[derive(Debug, Clone)]
pub struct RunHeader {
    pub script: String,
    pub scenarios: Vec<ScenarioLine>,
    pub max_vus: u32,
    /// Longest possible run, including graceful stops.
    pub max_duration: Duration,
    /// Output descriptions, e.g. `json (results.jsonl)`; none prints `-`.
    pub outputs: Vec<String>,
    /// The execution segment this instance runs, e.g. `1/3:2/3`, and its share
    /// of the test (0–1); `None` for the whole test.
    pub segment: Option<(String, f64)>,
    /// Worker processes running the test (`--workers`), if any.
    pub workers: Option<u32>,
    /// Agents running the test (`--agents`): how many, and where they connect.
    pub agents: Option<(u32, String)>,
    /// Where the control API listens, e.g. `http://127.0.0.1:6565`, if it does.
    pub control: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ScenarioLine {
    pub name: String,
    /// e.g. `10 looping VUs for 30s (gracefulStop: 30s)`.
    pub description: String,
}

/// What the metrics block reports: the run's totals.
pub struct SummaryData<'a> {
    pub totals: &'a Snapshot,
    pub registry: &'a Registry,
    pub interner: &'a Interner,
    /// How long the run took; per-second rates divide by this.
    pub duration: Duration,
    /// Extra lines under a metric for a tag-filtered subset, e.g.
    /// `http_req_duration` + `expected_response:true`.
    pub submetrics: Vec<(String, Vec<(String, String)>)>,
    /// Threshold results; metrics (and sub-metrics) with thresholds get a
    /// ✓ or ✗ in front, and a threshold's tag filter gets its own line.
    pub thresholds: Vec<ThresholdResult>,
    /// Which values trends show (k6's `summaryTrendStats`).
    pub trend_stats: Vec<TrendStat>,
    /// One unit for every time value (k6's `summaryTimeUnit`).
    pub time_unit: Option<format::TimeUnit>,
}

/// One evaluated threshold expression, for the summary.
#[derive(Debug, Clone, PartialEq)]
pub struct ThresholdResult {
    pub metric: String,
    pub tags: Vec<(String, String)>,
    /// As written, e.g. `p(95)<500`.
    pub expression: String,
    pub passed: bool,
}

impl<'a> SummaryData<'a> {
    /// Summary data with k6's default sub-metric
    /// (`http_req_duration{expected_response:true}`).
    pub fn new(
        totals: &'a Snapshot,
        registry: &'a Registry,
        interner: &'a Interner,
        duration: Duration,
    ) -> SummaryData<'a> {
        SummaryData {
            totals,
            registry,
            interner,
            duration,
            submetrics: vec![(
                "http_req_duration".to_owned(),
                vec![("expected_response".to_owned(), "true".to_owned())],
            )],
            thresholds: Vec::new(),
            trend_stats: TrendStat::DEFAULT.to_vec(),
            time_unit: None,
        }
    }
}

/// The checks section: each check's pass rate, grouped like k6 (`█ group`).
/// Empty when no checks ran.
pub fn render_checks(data: &SummaryData<'_>) -> String {
    let groups = check_groups(data);
    if groups.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    render_check_group(&groups, "", 0, &mut out);
    out
}

/// Check results by group path (`""` is the root) → check name → (passes, total),
/// summed across other tags. Every ancestor group is present so nesting renders.
/// Empty when no checks ran.
pub(crate) type CheckGroups = BTreeMap<String, BTreeMap<String, (u64, u64)>>;

pub(crate) fn check_groups(data: &SummaryData<'_>) -> CheckGroups {
    let mut groups = CheckGroups::new();
    let Some(checks) = data.registry.get("checks") else {
        return groups;
    };
    for (key, value) in data.totals.iter() {
        let Value::Rate(rate) = value else { continue };
        if key.metric != checks {
            continue;
        }
        let tags = data.interner.get(key.tags);
        let Some(name) = tags.get("check") else {
            continue;
        };
        let group = tags.get("group").unwrap_or("").to_owned();
        let entry = groups
            .entry(group)
            .or_default()
            .entry(name.to_owned())
            .or_default();
        entry.0 += rate.trues;
        entry.1 += rate.total;
    }
    if groups.is_empty() {
        return groups;
    }
    let paths: Vec<String> = groups.keys().cloned().collect();
    for path in paths {
        let mut prefix = String::new();
        for part in path.split("::").filter(|p| !p.is_empty()) {
            prefix = format!("{prefix}::{part}");
            groups.entry(prefix.clone()).or_default();
        }
    }
    groups.entry(String::new()).or_default();
    groups
}

/// Direct child group paths of `path`.
pub(crate) fn child_groups<'g>(groups: &'g CheckGroups, path: &str) -> Vec<(&'g str, &'g str)> {
    let prefix = format!("{path}::");
    groups
        .keys()
        .filter_map(|child| {
            let name = child.strip_prefix(&prefix)?;
            (!name.contains("::")).then_some((child.as_str(), name))
        })
        .collect()
}

fn render_check_group(groups: &CheckGroups, path: &str, depth: usize, out: &mut String) {
    let indent = " ".repeat(5 + 2 * depth);
    let checks = &groups[path];
    if !checks.is_empty() {
        out.push('\n');
        for (name, (passes, total)) in checks {
            let passed = passes == total;
            out.push_str(&format!(
                "{indent}{} {name}\n",
                if passed { "✓" } else { "✗" }
            ));
            if !passed {
                let pct = (*passes as f64 / *total as f64 * 100.0).floor();
                out.push_str(&format!(
                    "{indent} ↳  {pct}% — ✓ {passes} / ✗ {}\n",
                    total - passes
                ));
            }
        }
    }
    for (child, name) in child_groups(groups, path) {
        out.push_str(&format!("\n{indent}█ {name}\n"));
        render_check_group(groups, child, depth + 1, out);
    }
}

pub fn render_header(header: &RunHeader) -> String {
    let n = header.scenarios.len();
    let (execution, share) = match (&header.segment, header.workers) {
        (Some((segment, share)), _) => (format!("local (segment {segment})"), *share),
        (None, Some(n)) => (format!("local ({n} workers)"), 1.0),
        (None, None) if header.agents.is_some() => {
            let (n, address) = header.agents.as_ref().expect("checked");
            (format!("distributed ({n} agents on {address})"), 1.0)
        }
        (None, None) => ("local".to_owned(), 1.0),
    };
    let mut out = format!(
        "\n     execution: {execution}\n        script: {}\n        output: {}\n{}\n     \
         scenarios: ({:.2}%) {n} scenario{}, {} max VUs, {} max duration (incl. graceful stop):\n",
        header.script,
        if header.outputs.is_empty() {
            "-".to_owned()
        } else {
            header.outputs.join(", ")
        },
        match &header.control {
            Some(control) => format!("       control: {control}\n"),
            None => String::new(),
        },
        share * 100.0,
        if n == 1 { "" } else { "s" },
        header.max_vus,
        format::duration_ms(header.max_duration.as_secs_f64() * 1e3),
    );
    for s in &header.scenarios {
        out.push_str(&format!("              * {}: {}\n", s.name, s.description));
    }
    out
}

/// The metrics block: one line per metric that has data, sorted by name.
pub fn render_metrics(data: &SummaryData<'_>) -> String {
    let mut metrics: Vec<_> = data.registry.iter().collect();
    metrics.sort_by(|a, b| a.1.name.cmp(&b.1.name));

    // Whether all thresholds on (metric, tags) passed; None if there are none.
    let mark = |metric: &str, tags: &[(String, String)]| {
        let mut matching = data
            .thresholds
            .iter()
            .filter(|t| t.metric == metric && t.tags == tags)
            .peekable();
        matching.peek()?;
        Some(matching.all(|t| t.passed))
    };

    let mut rows = Vec::new();
    for (id, def) in metrics {
        let own_mark = mark(&def.name, &[]);
        let value = match data.totals.merged(id, &[], data.interner) {
            Some(value) => value,
            None if own_mark.is_some() => Value::empty(def.kind),
            None => continue,
        };
        let mut row = Row::new(def.name.clone(), &value, def.value_type, data);
        row.mark = own_mark;
        rows.push(row);

        // Sub-metric lines: the defaults, plus every tag-filtered threshold.
        let mut filters: Vec<&Vec<(String, String)>> = data
            .submetrics
            .iter()
            .filter(|(metric, _)| *metric == def.name)
            .map(|(_, tags)| tags)
            .collect();
        for t in &data.thresholds {
            if t.metric == def.name && !t.tags.is_empty() && !filters.contains(&&t.tags) {
                filters.push(&t.tags);
            }
        }
        for tags in filters {
            let filter: Vec<(&str, &str)> =
                tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            let sub_mark = mark(&def.name, tags);
            let sub = match data.totals.merged(id, &filter, data.interner) {
                Some(sub) => sub,
                None if sub_mark.is_some() => Value::empty(def.kind),
                None => continue,
            };
            let label = format!(
                "  {{ {} }}",
                tags.iter()
                    .map(|(k, v)| format!("{k}:{v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let mut row = Row::new(label, &sub, def.value_type, data);
            row.mark = sub_mark;
            rows.push(row);
        }
    }

    let label_width = rows
        .iter()
        .map(|r| r.label.chars().count())
        .max()
        .unwrap_or(0)
        + 3;
    let plain_widths = column_widths(rows.iter().filter(|r| !r.trend));
    let trend_widths = column_widths(rows.iter().filter(|r| r.trend));

    let mut out = String::new();
    for row in &rows {
        let widths = if row.trend {
            &trend_widths
        } else {
            &plain_widths
        };
        let dots = ".".repeat(label_width - row.label.chars().count());
        let cells: Vec<String> = row
            .cells
            .iter()
            .zip(widths)
            .map(|(cell, w)| format!("{cell:<w$}"))
            .collect();
        let prefix = match row.mark {
            Some(true) => "   ✓ ",
            Some(false) => "   ✗ ",
            None => "     ",
        };
        let line = format!("{prefix}{}{dots}: {}", row.label, cells.join(" "));
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}

struct Row {
    label: String,
    /// Threshold result for this line, if it has thresholds.
    mark: Option<bool>,
    cells: Vec<String>,
    /// Trend rows align their columns with each other, not with other kinds.
    trend: bool,
}

impl Row {
    fn new(label: String, value: &Value, value_type: ValueType, data: &SummaryData<'_>) -> Row {
        let (duration, trend_stats) = (data.duration, &data.trend_stats[..]);
        let fmt = |v: f64| match value_type {
            ValueType::Time => format::duration_in(v, data.time_unit),
            ValueType::Data => format::bytes(v),
            ValueType::Default => format::number(v),
        };
        let (cells, trend) = match value {
            Value::Counter(c) => {
                let secs = duration.as_secs_f64();
                let per_sec = if secs > 0.0 { c.sum / secs } else { 0.0 };
                (vec![fmt(c.sum), format!("{}/s", fmt(per_sec))], false)
            }
            Value::Gauge(g) => (
                vec![
                    fmt(g.value),
                    format!("min={}", fmt(g.min)),
                    format!("max={}", fmt(g.max)),
                ],
                false,
            ),
            Value::Rate(r) => (
                vec![
                    format::percent(r.rate()),
                    format!("✓ {}", r.trues),
                    format!("✗ {}", r.total - r.trues),
                ],
                false,
            ),
            Value::Trend(t) => (
                trend_stats
                    .iter()
                    .map(|stat| match stat {
                        TrendStat::Count => format!("count={}", t.count),
                        _ => format!("{stat}={}", fmt(stat.value(t))),
                    })
                    .collect(),
                true,
            ),
        };
        Row {
            label,
            mark: None,
            cells,
            trend,
        }
    }
}

fn column_widths<'r>(rows: impl Iterator<Item = &'r Row>) -> Vec<usize> {
    let mut widths = Vec::new();
    for row in rows {
        for (i, cell) in row.cells.iter().enumerate() {
            let w = cell.chars().count();
            if i == widths.len() {
                widths.push(w);
            } else {
                widths[i] = widths[i].max(w);
            }
        }
    }
    widths
}

/// `running (10.0s), 00/10 VUs, 300 complete and 0 interrupted iterations`
pub fn status_line(
    elapsed: Duration,
    active_vus: u32,
    max_vus: u32,
    complete: u64,
    interrupted: u64,
) -> String {
    let secs = elapsed.as_secs_f64();
    let minutes = (secs / 60.0).floor();
    format!(
        "running ({:02}m{:04.1}s), {active_vus:02}/{max_vus:02} VUs, {complete} complete and {interrupted} interrupted iterations",
        minutes as u64,
        secs - minutes * 60.0,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use k9_metrics::{Aggregator, Metrics};

    use super::*;

    fn sample_totals() -> Aggregator {
        let (registry, b) = Registry::with_builtins();
        let metrics = Arc::new(Metrics::new(registry));
        let tags = metrics.interner();
        let ok = tags.intern(&[("expected_response", "true"), ("status", "200")]);
        let bad = tags.intern(&[("expected_response", "false"), ("status", "500")]);
        for ms in [100.0, 110.0, 120.0] {
            metrics.record(b.http_req_duration, ok, ms);
            metrics.record(b.http_reqs, ok, 1.0);
            metrics.record(b.http_req_failed, ok, 0.0);
        }
        metrics.record(b.http_req_duration, bad, 500.0);
        metrics.record(b.http_reqs, bad, 1.0);
        metrics.record(b.http_req_failed, bad, 1.0);
        metrics.record(b.data_received, TagSetId::EMPTY, 2_300_000.0);
        metrics.record(b.vus, TagSetId::EMPTY, 10.0);
        metrics.record(b.iterations, TagSetId::EMPTY, 4.0);
        let mut agg = Aggregator::new(metrics);
        agg.tick(Duration::from_secs(10));
        agg
    }

    use k9_metrics::TagSetId;

    fn render(agg: &Aggregator) -> String {
        let m = agg.metrics();
        render_metrics(&SummaryData::new(
            agg.totals(),
            m.registry(),
            m.interner(),
            Duration::from_secs(10),
        ))
    }

    #[test]
    fn one_time_unit_for_every_trend_value() {
        let agg = sample_totals();
        let m = agg.metrics();
        let mut data = SummaryData::new(
            agg.totals(),
            m.registry(),
            m.interner(),
            Duration::from_secs(10),
        );
        data.time_unit = Some(format::TimeUnit::S);
        let out = render_metrics(&data);
        let duration = line(&out, "http_req_duration");
        assert!(
            duration.contains("avg=0.21s") && duration.contains("max=0.5s"),
            "{duration}"
        );
        data.time_unit = Some(format::TimeUnit::Us);
        let out = render_metrics(&data);
        assert!(
            line(&out, "http_req_duration").contains("min=100000µs"),
            "{out}"
        );
    }

    fn line<'a>(out: &'a str, name: &str) -> &'a str {
        out.lines()
            .find(|l| l.trim_start().starts_with(&format!("{name}.")))
            .unwrap_or_else(|| panic!("no line for {name} in:\n{out}"))
    }

    #[test]
    fn counters_show_total_and_rate() {
        let out = render(&sample_totals());
        let reqs = line(&out, "http_reqs");
        assert!(reqs.contains(": 4 ") && reqs.ends_with("0.4/s"), "{reqs}");
        let data = line(&out, "data_received");
        assert!(
            data.contains("2.3 MB") && data.ends_with("230 kB/s"),
            "{data}"
        );
    }

    #[test]
    fn rates_show_percent_and_counts() {
        let out = render(&sample_totals());
        let failed = line(&out, "http_req_failed");
        assert!(failed.contains("25.00%"), "{failed}");
        assert!(failed.contains("✓ 1") && failed.contains("✗ 3"), "{failed}");
    }

    #[test]
    fn trends_show_k6_columns_and_default_submetric() {
        let out = render(&sample_totals());
        let dur = line(&out, "http_req_duration");
        for part in ["avg=207.5ms", "min=100ms", "max=500ms", "p(90)=", "p(95)="] {
            assert!(dur.contains(part), "missing {part} in {dur}");
        }
        let sub = out
            .lines()
            .find(|l| l.contains("{ expected_response:true }"))
            .expect("sub-metric line");
        assert!(
            sub.contains("avg=110ms") && sub.contains("max=120ms"),
            "{sub}"
        );
        let dur_pos = out.find("http_req_duration.").unwrap();
        let sub_pos = out.find("{ expected_response:true }").unwrap();
        assert!(sub_pos > dur_pos, "sub-metric follows its metric");
    }

    #[test]
    fn gauges_show_value_min_max() {
        let out = render(&sample_totals());
        let vus = line(&out, "vus");
        assert!(
            vus.contains(": 10 ") && vus.contains("min=10") && vus.contains("max=10"),
            "{vus}"
        );
    }

    #[test]
    fn only_recorded_metrics_are_listed_in_name_order_with_aligned_colons() {
        let out = render(&sample_totals());
        assert!(!out.contains("http_req_blocked"), "never recorded:\n{out}");
        let names: Vec<&str> = out
            .lines()
            .filter(|l| !l.contains('{'))
            .map(|l| l.trim_start().split('.').next().unwrap())
            .collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        // The value column starts after the dot leader (labels may contain ':').
        // Marks replace indentation, so the value column stays aligned
        // (counted in characters: ✓ is several bytes).
        let columns: Vec<usize> = out
            .lines()
            .map(|l| l[..l.find(".: ").unwrap()].chars().count())
            .collect();
        assert!(columns.windows(2).all(|w| w[0] == w[1]), "aligned:\n{out}");
    }

    #[test]
    fn header_lists_scenarios() {
        let out = render_header(&RunHeader {
            script: "plan.yaml".into(),
            scenarios: vec![
                ScenarioLine {
                    name: "default".into(),
                    description: "10 looping VUs for 30s (gracefulStop: 30s)".into(),
                },
                ScenarioLine {
                    name: "slow".into(),
                    description: "1 looping VUs for 1m (gracefulStop: 30s)".into(),
                },
            ],
            max_vus: 11,
            max_duration: Duration::from_secs(90),
            outputs: vec![],
            segment: None,
            workers: None,
            agents: None,
            control: None,
        });
        assert!(out.contains("script: plan.yaml"), "{out}");
        assert!(
            out.contains("scenarios: (100.00%) 2 scenarios, 11 max VUs, 1m30s max duration (incl. graceful stop):"),
            "{out}"
        );
        assert!(
            out.contains("* default: 10 looping VUs for 30s (gracefulStop: 30s)"),
            "{out}"
        );
    }

    fn with_thresholds(agg: &Aggregator, thresholds: Vec<ThresholdResult>) -> String {
        let m = agg.metrics();
        let mut data = SummaryData::new(
            agg.totals(),
            m.registry(),
            m.interner(),
            Duration::from_secs(10),
        );
        data.thresholds = thresholds;
        render_metrics(&data)
    }

    fn threshold(
        metric: &str,
        tags: &[(&str, &str)],
        expression: &str,
        passed: bool,
    ) -> ThresholdResult {
        ThresholdResult {
            metric: metric.into(),
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            expression: expression.into(),
            passed,
        }
    }

    #[test]
    fn metrics_with_thresholds_are_marked() {
        let agg = sample_totals();
        let out = with_thresholds(
            &agg,
            vec![
                threshold("http_req_duration", &[], "p(95)<1000", true),
                threshold("http_req_failed", &[], "rate<0.01", false),
                threshold("http_reqs", &[("status", "500")], "count<1", false),
                threshold("http_reqs", &[("status", "404")], "count<1", true),
            ],
        );
        assert!(out.contains("   ✓ http_req_duration."), "{out}");
        assert!(out.contains("   ✗ http_req_failed."), "{out}");
        assert!(
            out.contains("     http_reqs."),
            "no threshold on the metric itself:\n{out}"
        );
        let sub = |tags: &str| {
            out.lines()
                .find(|l| l.contains(tags))
                .unwrap_or_else(|| panic!("no {tags} line in:\n{out}"))
                .to_owned()
        };
        let failed = sub("{ status:500 }");
        assert!(
            failed.trim_start().starts_with('✗') && failed.contains(": 1 "),
            "{failed}"
        );
        let never = sub("{ status:404 }");
        assert!(
            never.trim_start().starts_with('✓') && never.contains(": 0 "),
            "a threshold's sub-metric shows even without samples: {never}"
        );
        let columns: Vec<usize> = out
            .lines()
            .map(|l| l[..l.find(".: ").unwrap()].chars().count())
            .collect();
        assert!(columns.windows(2).all(|w| w[0] == w[1]), "aligned:\n{out}");
    }

    #[test]
    fn checks_are_listed_by_group() {
        let (registry, b) = Registry::with_builtins();
        let metrics = Arc::new(Metrics::new(registry));
        let t = metrics.interner();
        let root = t.intern(&[("group", ""), ("check", "status is 200"), ("scenario", "a")]);
        let root_b = t.intern(&[("group", ""), ("check", "status is 200"), ("scenario", "b")]);
        let api = t.intern(&[("group", "::api"), ("check", "fast")]);
        let nested = t.intern(&[("group", "::api::v2"), ("check", "has body")]);
        for _ in 0..3 {
            metrics.record(b.checks, root, 1.0);
        }
        metrics.record(b.checks, root_b, 1.0);
        for v in [1.0, 1.0, 1.0, 0.0, 0.0] {
            metrics.record(b.checks, api, v);
        }
        metrics.record(b.checks, nested, 0.0);
        let mut agg = Aggregator::new(metrics.clone());
        agg.tick(Duration::from_secs(1));
        let data = SummaryData::new(
            agg.totals(),
            metrics.registry(),
            metrics.interner(),
            Duration::from_secs(1),
        );
        let out = render_checks(&data);
        let expected = "
     ✓ status is 200

     █ api

       ✗ fast
        ↳  60% — ✓ 3 / ✗ 2

       █ v2

         ✗ has body
          ↳  0% — ✓ 0 / ✗ 1
";
        assert_eq!(out, expected, "got:\n{out}");
    }

    #[test]
    fn no_checks_no_section() {
        let agg = sample_totals();
        let m = agg.metrics();
        let data = SummaryData::new(
            agg.totals(),
            m.registry(),
            m.interner(),
            Duration::from_secs(1),
        );
        assert_eq!(render_checks(&data), "");
    }

    #[test]
    fn status_line_matches_k6() {
        assert_eq!(
            status_line(Duration::from_millis(10_049), 0, 10, 300, 2),
            "running (00m10.0s), 00/10 VUs, 300 complete and 2 interrupted iterations"
        );
    }
}
