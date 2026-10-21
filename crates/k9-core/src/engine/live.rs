//! Live control of a run: pausing, resuming and scaling the
//! `externally-controlled` scenario (the engine side of the control API).

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use k9_metrics::{MetricId, MetricKind, Metrics, Snapshot, Stats, Tick, Value, ValueType};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::plan::{Executor, Plan};

/// Why a control request was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControlError {
    #[error(
        "only a test whose scenarios are all externally-controlled can be paused once it has started (start it paused with --paused)"
    )]
    NotPausable,
    #[error("only an externally-controlled scenario's VUs can be changed")]
    NotScalable,
    #[error("can't run {vus} VUs: the externally-controlled scenario's maxVUs is {max}")]
    TooManyVus { vus: u32, max: u32 },
    #[error("the test run has finished")]
    Finished,
}

/// A run's state, as the control API reports it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Status {
    pub paused: bool,
    /// Past the initial pause (if any) and not finished.
    pub running: bool,
    pub stopped: bool,
    /// Active VUs.
    pub vus: u32,
    pub vus_max: u32,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Phase {
    #[default]
    NotStarted,
    Running,
    Finished,
}

#[derive(Default)]
struct State {
    phase: Phase,
    /// Every scenario is externally-controlled, so the run can pause mid-way.
    pausable: bool,
    /// The externally-controlled scenario's `maxVUs`.
    max_vus: Option<u32>,
    /// Every scenario's `maxVUs` (or equivalent), summed.
    vus_max: u32,
    /// The plan's VUs were taken: configuring again keeps what scaling set.
    configured: bool,
}

pub(crate) struct Live {
    state: Mutex<State>,
    /// Read on every iteration; `paused` is for waiting.
    is_paused: AtomicBool,
    paused: watch::Sender<bool>,
    /// The externally-controlled scenario's VUs.
    vus: watch::Sender<u32>,
}

impl Live {
    pub fn new() -> Live {
        Live {
            state: Mutex::new(State::default()),
            is_paused: AtomicBool::new(false),
            paused: watch::Sender::new(false),
            vus: watch::Sender::new(0),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Called under the state lock, so a pause and the start are ordered.
    fn set_paused(&self, paused: bool) {
        self.is_paused.store(paused, Ordering::Relaxed);
        self.paused.send_replace(paused);
    }

    pub fn pause(&self) -> Result<(), ControlError> {
        let state = self.state();
        match state.phase {
            Phase::Finished => Err(ControlError::Finished),
            Phase::Running if !state.pausable => Err(ControlError::NotPausable),
            _ => {
                self.set_paused(true);
                Ok(())
            }
        }
    }

    pub fn resume(&self) -> Result<(), ControlError> {
        let state = self.state();
        if state.phase == Phase::Finished {
            return Err(ControlError::Finished);
        }
        self.set_paused(false);
        Ok(())
    }

    pub fn scale(&self, vus: u32) -> Result<(), ControlError> {
        let state = self.state();
        if state.phase == Phase::Finished {
            return Err(ControlError::Finished);
        }
        match state.max_vus {
            None => Err(ControlError::NotScalable),
            Some(max) if vus > max => Err(ControlError::TooManyVus { vus, max }),
            Some(_) => {
                self.vus.send_replace(vus);
                Ok(())
            }
        }
    }

    pub fn is_paused(&self) -> bool {
        self.is_paused.load(Ordering::Relaxed)
    }

    /// (paused, running, the run's max VUs).
    pub fn status(&self) -> (bool, bool, u32) {
        let state = self.state();
        (
            *self.paused.borrow(),
            state.phase == Phase::Running,
            state.vus_max,
        )
    }

    /// Takes the plan's externally-controlled scenario (if any) as the one
    /// to scale, starting from its `vus` the first time.
    pub fn configure(&self, plan: &Plan, vus_max: u32) {
        let mut state = self.state();
        state.vus_max = vus_max;
        let scenarios = &plan.options.scenarios;
        state.pausable = !scenarios.is_empty()
            && scenarios
                .values()
                .all(|s| matches!(s.executor, Executor::ExternallyControlled { .. }));
        state.max_vus = None;
        for scenario in scenarios.values() {
            if let Executor::ExternallyControlled { vus, max_vus, .. } = scenario.executor {
                state.max_vus = Some(max_vus);
                if !state.configured {
                    self.vus.send_replace(vus);
                }
            }
        }
        state.configured = true;
    }

    /// Waits until the run isn't paused (or is stopped), then marks it running.
    pub async fn start(&self, stop: &CancellationToken) {
        let mut paused = self.paused.subscribe();
        loop {
            {
                let mut state = self.state();
                if !*paused.borrow_and_update() || stop.is_cancelled() {
                    state.phase = Phase::Running;
                    return;
                }
            }
            tokio::select! {
                _ = paused.changed() => {}
                _ = stop.cancelled() => {}
            }
        }
    }

    pub fn finish(&self) {
        self.state().phase = Phase::Finished;
    }

    pub fn paused(&self) -> watch::Receiver<bool> {
        self.paused.subscribe()
    }

    pub fn vus(&self) -> watch::Receiver<u32> {
        self.vus.subscribe()
    }
}

/// A metric's totals so far, merged across its tags, with k6's REST API
/// sample fields (times in ms).
#[derive(Debug, Clone, PartialEq)]
pub struct LiveMetric {
    pub name: String,
    pub kind: MetricKind,
    pub contains: ValueType,
    pub sample: Vec<(String, f64)>,
}

/// What a live dashboard shows, published once a second (while watched).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LiveFrame {
    pub elapsed: Duration,
    /// The last second's numbers, each metric merged across its tags.
    pub last_second: Vec<(String, Stats)>,
    /// Totals so far, as [`LiveMetric`]s.
    pub totals: Vec<LiveMetric>,
    /// Each check's results so far, by name.
    pub checks: Vec<LiveCheck>,
    /// Every threshold as of now.
    pub thresholds: Vec<crate::engine::ThresholdOutcome>,
}

impl LiveFrame {
    /// The last second's numbers for `metric`, if it recorded any.
    pub fn second(&self, metric: &str) -> Option<Stats> {
        self.last_second
            .iter()
            .find(|(name, _)| name == metric)
            .map(|(_, stats)| *stats)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveCheck {
    pub name: String,
    pub passes: u64,
    pub fails: u64,
}

pub(crate) fn live_frame(
    tick: Option<&Tick>,
    totals: &Snapshot,
    metrics: &Metrics,
    checks: MetricId,
    elapsed: Duration,
    thresholds: Vec<crate::engine::ThresholdOutcome>,
) -> LiveFrame {
    let registry = metrics.registry();
    let mut last_second: Vec<(String, Stats)> = tick
        .map(|tick| {
            tick.metrics
                .iter()
                .map(|(id, stats)| (registry.def(*id).name.clone(), *stats))
                .collect()
        })
        .unwrap_or_default();
    last_second.sort_by(|a, b| a.0.cmp(&b.0));
    let mut by_name = std::collections::BTreeMap::<String, (u64, u64)>::new();
    for (key, value) in totals.iter() {
        if key.metric != checks {
            continue;
        }
        let tags = metrics.interner().get(key.tags);
        if let (Some(name), Value::Rate(rate)) = (tags.get("check"), value) {
            let entry = by_name.entry(name.to_owned()).or_default();
            entry.0 += rate.trues;
            entry.1 += rate.total - rate.trues;
        }
    }
    LiveFrame {
        elapsed,
        last_second,
        totals: live_metrics(totals, metrics, elapsed),
        checks: by_name
            .into_iter()
            .map(|(name, (passes, fails))| LiveCheck {
                name,
                passes,
                fails,
            })
            .collect(),
        thresholds,
    }
}

/// Every metric that has data, by name.
pub(crate) fn live_metrics(
    totals: &Snapshot,
    metrics: &Metrics,
    elapsed: Duration,
) -> Vec<LiveMetric> {
    let secs = elapsed.as_secs_f64();
    let mut live: Vec<LiveMetric> = metrics
        .registry()
        .iter()
        .filter_map(|(id, def)| {
            let value = totals.merged(id, &[], metrics.interner())?;
            let sample: Vec<(&str, f64)> = match value {
                Value::Counter(c) => vec![
                    ("count", c.sum),
                    ("rate", if secs > 0.0 { c.sum / secs } else { 0.0 }),
                ],
                Value::Gauge(g) => vec![("value", g.value)],
                Value::Rate(r) => vec![("rate", r.rate())],
                Value::Trend(t) => vec![
                    ("avg", t.avg()),
                    ("max", t.max),
                    ("med", t.med()),
                    ("min", t.min),
                    ("p(90)", t.percentile(90.0)),
                    ("p(95)", t.percentile(95.0)),
                ],
            };
            Some(LiveMetric {
                name: def.name.clone(),
                kind: def.kind,
                contains: def.value_type,
                sample: sample.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
            })
        })
        .collect();
    live.sort_by(|a, b| a.name.cmp(&b.name));
    live
}
