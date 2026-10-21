//! A plan's thresholds, bound to the run's metrics and evaluated on its totals.

use std::time::Duration;

use k9_metrics::threshold::{Expr, Selector};
use k9_metrics::{Interner, MetricId, Registry, Snapshot};

use crate::engine::EngineError;
use crate::plan::{Plan, Threshold};

/// The result of one threshold expression.
#[derive(Debug, Clone, PartialEq)]
pub struct ThresholdOutcome {
    /// The metric selector as written in the plan, e.g. `http_reqs{status:200}`.
    pub selector: String,
    /// The expression as written, e.g. `p(95)<500`.
    pub expression: String,
    /// The number the expression compared.
    pub observed: f64,
    pub passed: bool,
}

pub(crate) struct Thresholds {
    groups: Vec<Group>,
}

struct Group {
    selector: String,
    metric: MetricId,
    filter: Vec<(String, String)>,
    entries: Vec<Entry>,
}

struct Entry {
    source: String,
    expr: Expr,
    abort_on_fail: bool,
    delay_abort_eval: Duration,
}

impl Thresholds {
    /// Binds the plan's thresholds to metrics; rejects unknown metrics and
    /// aggregations that don't fit the metric's type.
    pub fn compile(plan: &Plan, registry: &Registry) -> Result<Thresholds, EngineError> {
        let mut groups = Vec::new();
        for (key, list) in &plan.options.thresholds {
            let invalid = |message: String| EngineError::InvalidThreshold {
                selector: key.clone(),
                message,
            };
            // Syntax was validated when the plan was resolved.
            let selector: Selector = key.parse().map_err(invalid)?;
            let metric = registry
                .get(&selector.metric)
                .ok_or_else(|| invalid(format!("no such metric `{}`", selector.metric)))?;
            let kind = registry.def(metric).kind;
            let mut entries = Vec::new();
            for threshold in list {
                let expr: Expr = threshold.expression().parse().map_err(invalid)?;
                expr.check_kind(kind).map_err(invalid)?;
                let (abort_on_fail, delay_abort_eval) = match threshold {
                    Threshold::Expr(_) => (false, Duration::ZERO),
                    Threshold::Detailed {
                        abort_on_fail,
                        delay_abort_eval,
                        ..
                    } => (
                        *abort_on_fail,
                        delay_abort_eval.map(|d| d.as_std()).unwrap_or_default(),
                    ),
                };
                entries.push(Entry {
                    source: threshold.expression().to_owned(),
                    expr,
                    abort_on_fail,
                    delay_abort_eval,
                });
            }
            groups.push(Group {
                selector: key.clone(),
                metric,
                filter: selector.tags,
                entries,
            });
        }
        Ok(Thresholds { groups })
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    pub fn evaluate(
        &self,
        totals: &Snapshot,
        interner: &Interner,
        elapsed: Duration,
    ) -> Vec<ThresholdOutcome> {
        let mut out = Vec::new();
        for group in &self.groups {
            let filter: Vec<(&str, &str)> = group
                .filter
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let value = totals.merged(group.metric, &filter, interner);
            for entry in &group.entries {
                let observed = entry.expr.observe(value.as_ref(), elapsed);
                out.push(ThresholdOutcome {
                    selector: group.selector.clone(),
                    expression: entry.source.clone(),
                    observed,
                    passed: entry.expr.passes(observed),
                });
            }
        }
        out
    }

    /// True if a failing threshold has `abortOnFail` and its `delayAbortEval` has passed.
    pub fn should_abort(&self, outcomes: &[ThresholdOutcome], elapsed: Duration) -> bool {
        let entries = self.groups.iter().flat_map(|g| &g.entries);
        entries.zip(outcomes).any(|(entry, outcome)| {
            !outcome.passed && entry.abort_on_fail && elapsed >= entry.delay_abort_eval
        })
    }
}
