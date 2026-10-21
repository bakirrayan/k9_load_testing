//! `--summary-export`: the end-of-test summary as JSON, in k6's legacy format.

use k9_metrics::{MetricId, Value, ValueType};
use serde_json::{Map, Value as Json, json};

use crate::summary::{CheckGroups, SummaryData, check_groups, child_groups};

/// k6's `--summary-export` document: `root_group` (checks by group) and
/// `metrics` (per metric and sub-metric). Threshold values follow k6: `true`
/// means the threshold *failed*.
pub fn summary_export(data: &SummaryData<'_>) -> Json {
    let groups = check_groups(data);
    json!({
        "root_group": group_json(&groups, ""),
        "metrics": each_metric(data, metric_json),
    })
}

/// How the terminal k9 writes to looks, for `handle_summary`'s `state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SummaryState {
    pub stdout_tty: bool,
    pub stderr_tty: bool,
    /// `--no-color` or `NO_COLOR`.
    pub no_color: bool,
}

/// The data k6 passes to `handleSummary()`: every metric (and sub-metric)
/// with its `type`, `contains`, `values` and `thresholds` (`{"ok": bool}`),
/// the checks by group, the summary options and the run's `state`.
pub fn summary_data(data: &SummaryData<'_>, state: SummaryState) -> Json {
    let metrics = each_metric(data, |data, id, kind, name, tags| {
        let totals = metric_totals(data, id, kind, name, tags)?;
        let def = data.registry.def(id);
        let mut out = json!({
            "type": match kind {
                k9_metrics::MetricKind::Counter => "counter",
                k9_metrics::MetricKind::Gauge => "gauge",
                k9_metrics::MetricKind::Rate => "rate",
                k9_metrics::MetricKind::Trend => "trend",
            },
            "contains": match def.value_type {
                ValueType::Default => "default",
                ValueType::Time => "time",
                ValueType::Data => "data",
            },
            "values": rate_first(totals.values),
        });
        if !totals.thresholds.is_empty() {
            out["thresholds"] = totals
                .thresholds
                .into_iter()
                .map(|(expression, passed)| (expression, json!({ "ok": passed })))
                .collect::<Map<_, _>>()
                .into();
        }
        Some(out)
    });
    let groups = check_groups(data);
    json!({
        "root_group": group_json(&groups, ""),
        "options": {
            "summaryTrendStats": data.trend_stats.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "summaryTimeUnit": data.time_unit.map(|u| u.to_string()).unwrap_or_default(),
            "noColor": state.no_color,
        },
        "state": {
            "isStdOutTTY": state.stdout_tty,
            "isStdErrTTY": state.stderr_tty,
            "testRunDurationMs": data.duration.as_secs_f64() * 1e3,
        },
        "metrics": metrics,
    })
}

/// k6 calls a rate's share `rate` in handleSummary data (`value` in the export).
fn rate_first(mut values: Json) -> Json {
    if let Some(map) = values.as_object_mut()
        && map.contains_key("passes")
        && let Some(value) = map.remove("value")
    {
        map.insert("rate".to_owned(), value);
    }
    values
}

type MetricEntry = fn(
    &SummaryData<'_>,
    MetricId,
    k9_metrics::MetricKind,
    &str,
    &[(String, String)],
) -> Option<Json>;

/// Every metric by name, then its sub-metrics (the summary's, and each tag
/// filter a threshold uses) as `name{key:value,…}`.
fn each_metric(data: &SummaryData<'_>, entry: MetricEntry) -> Map<String, Json> {
    let mut metrics = Map::new();
    let mut defs: Vec<_> = data.registry.iter().collect();
    defs.sort_by(|a, b| a.1.name.cmp(&b.1.name));
    for (id, def) in defs {
        if let Some(json) = entry(data, id, def.kind, &def.name, &[]) {
            metrics.insert(def.name.clone(), json);
        }
        let mut filters: Vec<&Vec<(String, String)>> = data
            .submetrics
            .iter()
            .filter(|(m, _)| *m == def.name)
            .map(|(_, tags)| tags)
            .collect();
        for t in &data.thresholds {
            if t.metric == def.name && !t.tags.is_empty() && !filters.contains(&&t.tags) {
                filters.push(&t.tags);
            }
        }
        for tags in filters {
            if let Some(json) = entry(data, id, def.kind, &def.name, tags) {
                let key: Vec<String> = tags.iter().map(|(k, v)| format!("{k}:{v}")).collect();
                metrics.insert(format!("{}{{{}}}", def.name, key.join(",")), json);
            }
        }
    }
    metrics
}

/// A metric's (or sub-metric's) values and its threshold results; `None` if
/// it has neither.
struct MetricTotals {
    values: Json,
    /// (expression, passed)
    thresholds: Vec<(String, bool)>,
}

fn metric_totals(
    data: &SummaryData<'_>,
    id: MetricId,
    kind: k9_metrics::MetricKind,
    name: &str,
    tags: &[(String, String)],
) -> Option<MetricTotals> {
    let thresholds: Vec<(String, bool)> = data
        .thresholds
        .iter()
        .filter(|t| t.metric == name && t.tags == tags)
        .map(|t| (t.expression.clone(), t.passed))
        .collect();
    let filter: Vec<(&str, &str)> = tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let value = match data.totals.merged(id, &filter, data.interner) {
        Some(value) => value,
        None if !thresholds.is_empty() => Value::empty(kind),
        None => return None,
    };
    let secs = data.duration.as_secs_f64();
    let values = match value {
        Value::Counter(c) => json!({
            "count": c.sum,
            "rate": if secs > 0.0 { c.sum / secs } else { 0.0 },
        }),
        Value::Gauge(g) => json!({ "value": g.value, "min": g.min, "max": g.max }),
        Value::Rate(r) => json!({
            "passes": r.trues,
            "fails": r.total - r.trues,
            "value": r.rate(),
        }),
        Value::Trend(t) => data
            .trend_stats
            .iter()
            .map(|stat| (stat.to_string(), json!(stat.value(&t))))
            .collect::<Map<_, _>>()
            .into(),
    };
    Some(MetricTotals { values, thresholds })
}

/// A metric in `--summary-export`'s legacy layout: values at the top level,
/// thresholds as `expression: crossed`.
fn metric_json(
    data: &SummaryData<'_>,
    id: MetricId,
    kind: k9_metrics::MetricKind,
    name: &str,
    tags: &[(String, String)],
) -> Option<Json> {
    let totals = metric_totals(data, id, kind, name, tags)?;
    let mut out = totals.values;
    if !totals.thresholds.is_empty() {
        out["thresholds"] = totals
            .thresholds
            .into_iter()
            .map(|(expression, passed)| (expression, Json::Bool(!passed)))
            .collect::<Map<_, _>>()
            .into();
    }
    Some(out)
}

fn group_json(groups: &CheckGroups, path: &str) -> Json {
    let checks: Vec<Json> = groups
        .get(path)
        .into_iter()
        .flatten()
        .map(|(name, (passes, total))| {
            json!({
                "name": name,
                "path": format!("{path}::{name}"),
                "passes": passes,
                "fails": total - passes,
            })
        })
        .collect();
    let children: Vec<Json> = child_groups(groups, path)
        .into_iter()
        .map(|(child, _)| group_json(groups, child))
        .collect();
    json!({
        "name": path.rsplit("::").next().unwrap_or(""),
        "path": path,
        "groups": children,
        "checks": checks,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use k9_metrics::{Aggregator, Metrics, Registry, TagSetId, ValueType};

    use super::*;
    use crate::ThresholdResult;

    #[test]
    fn exports_metrics_checks_and_thresholds_like_k6() {
        let (registry, b) = Registry::with_builtins();
        let metrics = Arc::new(Metrics::new(registry));
        let t = metrics.interner();
        let ok = t.intern(&[("expected_response", "true"), ("group", "")]);
        for ms in [10.0, 20.0, 30.0] {
            metrics.record(b.http_req_duration, ok, ms);
            metrics.record(b.http_reqs, ok, 1.0);
            metrics.record(b.http_req_failed, ok, 0.0);
        }
        metrics.record(b.vus, TagSetId::EMPTY, 4.0);
        metrics.record(b.checks, t.intern(&[("group", ""), ("check", "ok")]), 1.0);
        metrics.record(
            b.checks,
            t.intern(&[("group", "::api"), ("check", "fast")]),
            0.0,
        );
        let mut agg = Aggregator::new(metrics.clone());
        agg.tick(Duration::from_secs(1));

        let mut data = SummaryData::new(
            agg.totals(),
            metrics.registry(),
            metrics.interner(),
            Duration::from_secs(3),
        );
        data.thresholds = vec![
            ThresholdResult {
                metric: "http_req_duration".into(),
                tags: vec![],
                expression: "p(95)<1000".into(),
                passed: true,
            },
            ThresholdResult {
                metric: "http_req_duration".into(),
                tags: vec![],
                expression: "avg<1".into(),
                passed: false,
            },
        ];
        let json = summary_export(&data);
        let m = &json["metrics"];

        let dur = &m["http_req_duration"];
        assert_eq!(dur["avg"], 20.0);
        assert_eq!(dur["min"], 10.0);
        assert_eq!(dur["max"], 30.0);
        assert!(dur["med"].is_number() && dur["p(90)"].is_number() && dur["p(95)"].is_number());
        assert_eq!(
            dur["thresholds"],
            serde_json::json!({"p(95)<1000": false, "avg<1": true})
        );
        assert_eq!(m["http_req_duration{expected_response:true}"]["avg"], 20.0);

        assert_eq!(m["http_reqs"]["count"], 3.0);
        assert_eq!(m["http_reqs"]["rate"], 1.0, "per second over 3 s");
        assert!(m["http_reqs"].get("thresholds").is_none());
        assert_eq!(m["http_req_failed"]["passes"], 0);
        assert_eq!(m["http_req_failed"]["fails"], 3);
        assert_eq!(m["http_req_failed"]["value"], 0.0);
        assert_eq!(m["vus"]["value"], 4.0);
        assert_eq!(m["vus"]["min"], 4.0);
        assert!(m.get("http_req_blocked").is_none(), "never recorded");

        let root = &json["root_group"];
        assert_eq!(root["name"], "");
        assert_eq!(root["path"], "");
        assert_eq!(
            root["checks"],
            serde_json::json!([{ "name": "ok", "path": "::ok", "passes": 1, "fails": 0 }])
        );
        let api = &root["groups"][0];
        assert_eq!(
            (api["name"].as_str(), api["path"].as_str()),
            (Some("api"), Some("::api"))
        );
        assert_eq!(
            api["checks"],
            serde_json::json!([{ "name": "fast", "path": "::api::fast", "passes": 0, "fails": 1 }])
        );
        assert_eq!(api["groups"], serde_json::json!([]));
    }

    #[test]
    fn summary_data_follows_k6s_handle_summary_format() {
        let (mut registry, b) = Registry::with_builtins();
        let orders = registry
            .register(
                "orders",
                k9_metrics::MetricKind::Counter,
                ValueType::Default,
            )
            .unwrap();
        let metrics = Arc::new(Metrics::new(registry));
        let t = metrics.interner();
        let ok = t.intern(&[("expected_response", "true"), ("group", "")]);
        for ms in [10.0, 20.0, 30.0] {
            metrics.record(b.http_req_duration, ok, ms);
        }
        metrics.record(orders, TagSetId::EMPTY, 5.0);
        metrics.record(b.data_received, TagSetId::EMPTY, 2048.0);
        metrics.record(
            b.checks,
            t.intern(&[("group", "::api"), ("check", "fast")]),
            1.0,
        );
        let mut agg = Aggregator::new(metrics.clone());
        agg.tick(Duration::from_secs(1));
        let mut data = SummaryData::new(
            agg.totals(),
            metrics.registry(),
            metrics.interner(),
            Duration::from_millis(2500),
        );
        data.thresholds = vec![ThresholdResult {
            metric: "http_req_duration".into(),
            tags: vec![],
            expression: "avg<1".into(),
            passed: false,
        }];

        let json = summary_data(
            &data,
            SummaryState {
                stdout_tty: false,
                stderr_tty: true,
                no_color: false,
            },
        );
        let m = &json["metrics"];
        let dur = &m["http_req_duration"];
        assert_eq!(
            (dur["type"].as_str(), dur["contains"].as_str()),
            (Some("trend"), Some("time"))
        );
        assert_eq!(dur["values"]["avg"], 20.0);
        assert!(dur["values"]["p(95)"].is_number());
        assert_eq!(
            dur["thresholds"],
            serde_json::json!({"avg<1": {"ok": false}})
        );
        assert_eq!(
            m["http_req_duration{expected_response:true}"]["values"]["max"],
            30.0
        );
        assert_eq!(
            m["orders"],
            serde_json::json!({"type": "counter", "contains": "default", "values": {"count": 5.0, "rate": 2.0}})
        );
        assert_eq!(m["data_received"]["contains"], "data");
        assert_eq!(
            m["checks"]["values"],
            serde_json::json!({"rate": 1.0, "passes": 1, "fails": 0})
        );
        assert_eq!(json["root_group"]["groups"][0]["checks"][0]["name"], "fast");
        assert_eq!(
            json["state"],
            serde_json::json!({"isStdOutTTY": false, "isStdErrTTY": true, "testRunDurationMs": 2500.0})
        );
        assert_eq!(
            json["options"]["summaryTrendStats"],
            serde_json::json!(["avg", "min", "med", "max", "p(90)", "p(95)"])
        );
    }
}
