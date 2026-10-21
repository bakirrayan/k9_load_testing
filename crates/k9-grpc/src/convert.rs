//! k9-core's protocol messages ↔ the protobuf types.

use k9_core::Segment;
use k9_core::engine::{RunEvent, ScriptError};
use k9_core::protocol::{
    AgentGroup, Assignment, FromWorker, ToWorker, WorkerProgress, WorkerSummary,
};
use k9_metrics::{
    PortableSample, PortableSamples, PortableSeries, PortableSnapshot, PortableValue,
};

use crate::pb;
use crate::pb::{event, manager_message, series, worker_message};

/// A message that doesn't hold what k9 needs.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConvertError {
    #[error("`{0}` is missing")]
    Missing(&'static str),
    #[error("invalid segment: {0}")]
    Segment(String),
}

fn required<T>(value: Option<T>, what: &'static str) -> Result<T, ConvertError> {
    value.ok_or(ConvertError::Missing(what))
}

// ---- Manager → worker ------------------------------------------------------

impl From<ToWorker> for pb::ManagerMessage {
    fn from(message: ToWorker) -> pb::ManagerMessage {
        let message = match message {
            ToWorker::Assign(a) => manager_message::Message::Assign(pb::Assignment {
                plan_json: a.plan_json,
                script: a.script,
                script_source: a.script_source,
                modules: pairs_to(a.modules),
                env: pairs_to(a.env),
                segment: Some(pb::Segment {
                    sequence: a
                        .segment
                        .points()
                        .into_iter()
                        .map(|(num, den)| pb::Fraction { num, den })
                        .collect(),
                    index: a.segment.index() as u32,
                }),
                setup_data: a.setup_data,
                forward_samples: a.forward_samples,
            }),
            ToWorker::Start => manager_message::Message::Start(pb::Start {}),
            ToWorker::Stop => manager_message::Message::Stop(pb::Stop {}),
        };
        pb::ManagerMessage {
            message: Some(message),
        }
    }
}

impl TryFrom<pb::ManagerMessage> for ToWorker {
    type Error = ConvertError;

    fn try_from(message: pb::ManagerMessage) -> Result<ToWorker, ConvertError> {
        Ok(match required(message.message, "ManagerMessage.message")? {
            manager_message::Message::Assign(a) => {
                let segment = required(a.segment, "Assignment.segment")?;
                let points = segment.sequence.iter().map(|f| (f.num, f.den)).collect();
                let segment = Segment::from_points(points, segment.index as usize)
                    .map_err(|e| ConvertError::Segment(e.to_string()))?;
                ToWorker::Assign(Assignment {
                    plan_json: a.plan_json,
                    script: a.script,
                    script_source: a.script_source,
                    modules: pairs_from(a.modules),
                    env: pairs_from(a.env),
                    segment,
                    setup_data: a.setup_data,
                    forward_samples: a.forward_samples,
                })
            }
            manager_message::Message::Start(_) => ToWorker::Start,
            manager_message::Message::Stop(_) => ToWorker::Stop,
        })
    }
}

// ---- Worker → manager ------------------------------------------------------

impl From<FromWorker> for pb::WorkerMessage {
    fn from(message: FromWorker) -> pb::WorkerMessage {
        use worker_message::Message;
        let message = match message {
            FromWorker::Hello {
                protocol,
                version,
                pid,
                slot,
                token,
                group,
            } => Message::Hello(pb::Hello {
                protocol,
                version,
                pid,
                slot,
                token,
                agent: group.agent,
                workers: group.workers,
                weight: group.weight,
            }),
            FromWorker::Ready => Message::Ready(pb::Ready {}),
            FromWorker::Tick {
                elapsed_ms,
                metrics,
                progress,
            } => Message::Tick(pb::Tick {
                elapsed_ms,
                metrics: Some(snapshot_to(metrics)),
                progress: Some(pb::Progress {
                    active_vus: progress.active_vus,
                    peak_vus: progress.peak_vus,
                    iterations_complete: progress.iterations_complete,
                    iterations_interrupted: progress.iterations_interrupted,
                }),
            }),
            FromWorker::Event(event) => Message::Event(pb::Event {
                event: Some(match event {
                    RunEvent::ScriptError(e) => event::Event::ScriptError(pb::ScriptError {
                        message: e.message,
                        details: e.details,
                    }),
                    RunEvent::RequestError(e) => event::Event::RequestError(e),
                }),
            }),
            FromWorker::Samples(s) => Message::Samples(pb::Samples {
                new_metrics: s.new_metrics,
                new_tag_sets: s
                    .new_tag_sets
                    .into_iter()
                    .map(|tags| pb::TagSet {
                        tags: pairs_to(tags),
                    })
                    .collect(),
                samples: s
                    .samples
                    .into_iter()
                    .map(|s| pb::Sample {
                        metric: s.metric,
                        tags: s.tags,
                        micros: s.micros,
                        value: s.value,
                    })
                    .collect(),
            }),
            FromWorker::Done(summary) => Message::Done(pb::Summary {
                iterations_complete: summary.iterations_complete,
                iterations_interrupted: summary.iterations_interrupted,
                script_errors: counts_to(summary.script_errors),
                request_errors: counts_to(summary.request_errors),
                metrics: Some(snapshot_to(summary.metrics)),
            }),
            FromWorker::Failed { message } => Message::Failed(pb::Failed { message }),
        };
        pb::WorkerMessage {
            message: Some(message),
        }
    }
}

impl TryFrom<pb::WorkerMessage> for FromWorker {
    type Error = ConvertError;

    fn try_from(message: pb::WorkerMessage) -> Result<FromWorker, ConvertError> {
        use worker_message::Message;
        Ok(match required(message.message, "WorkerMessage.message")? {
            Message::Hello(h) => FromWorker::Hello {
                protocol: h.protocol,
                version: h.version,
                pid: h.pid,
                slot: h.slot,
                token: h.token,
                group: AgentGroup {
                    agent: h.agent,
                    workers: h.workers,
                    weight: h.weight,
                },
            },
            Message::Ready(_) => FromWorker::Ready,
            Message::Tick(t) => {
                let p = required(t.progress, "Tick.progress")?;
                FromWorker::Tick {
                    elapsed_ms: t.elapsed_ms,
                    metrics: snapshot_from(t.metrics.unwrap_or_default())?,
                    progress: WorkerProgress {
                        active_vus: p.active_vus,
                        peak_vus: p.peak_vus,
                        iterations_complete: p.iterations_complete,
                        iterations_interrupted: p.iterations_interrupted,
                    },
                }
            }
            Message::Event(e) => FromWorker::Event(match required(e.event, "Event.event")? {
                event::Event::ScriptError(e) => RunEvent::ScriptError(ScriptError {
                    message: e.message,
                    details: e.details,
                }),
                event::Event::RequestError(e) => RunEvent::RequestError(e),
            }),
            Message::Samples(s) => FromWorker::Samples(PortableSamples {
                new_metrics: s.new_metrics,
                new_tag_sets: s
                    .new_tag_sets
                    .into_iter()
                    .map(|t| pairs_from(t.tags))
                    .collect(),
                samples: s
                    .samples
                    .into_iter()
                    .map(|s| PortableSample {
                        metric: s.metric,
                        tags: s.tags,
                        micros: s.micros,
                        value: s.value,
                    })
                    .collect(),
            }),
            Message::Done(s) => FromWorker::Done(WorkerSummary {
                iterations_complete: s.iterations_complete,
                iterations_interrupted: s.iterations_interrupted,
                script_errors: counts_from(s.script_errors),
                request_errors: counts_from(s.request_errors),
                metrics: snapshot_from(s.metrics.unwrap_or_default())?,
            }),
            Message::Failed(f) => FromWorker::Failed { message: f.message },
        })
    }
}

// ---- Metrics ---------------------------------------------------------------

fn snapshot_to(snapshot: PortableSnapshot) -> pb::Snapshot {
    pb::Snapshot {
        series: snapshot
            .series
            .into_iter()
            .map(|s| pb::Series {
                metric: s.metric,
                tags: pairs_to(s.tags),
                value: Some(match s.value {
                    PortableValue::Counter { sum } => series::Value::Counter(pb::Counter { sum }),
                    PortableValue::Gauge { value, min, max } => {
                        series::Value::Gauge(pb::Gauge { value, min, max })
                    }
                    PortableValue::Rate { trues, total } => {
                        series::Value::Rate(pb::Rate { trues, total })
                    }
                    PortableValue::Trend {
                        count,
                        sum,
                        min,
                        max,
                        clamped,
                        buckets,
                    } => series::Value::Trend(pb::Trend {
                        count,
                        sum,
                        min,
                        max,
                        clamped,
                        buckets: buckets
                            .into_iter()
                            .map(|(value, count)| pb::Bucket { value, count })
                            .collect(),
                    }),
                }),
            })
            .collect(),
    }
}

fn snapshot_from(snapshot: pb::Snapshot) -> Result<PortableSnapshot, ConvertError> {
    let series = snapshot
        .series
        .into_iter()
        .map(|s| {
            let value = match required(s.value, "Series.value")? {
                series::Value::Counter(c) => PortableValue::Counter { sum: c.sum },
                series::Value::Gauge(g) => PortableValue::Gauge {
                    value: g.value,
                    min: g.min,
                    max: g.max,
                },
                series::Value::Rate(r) => PortableValue::Rate {
                    trues: r.trues,
                    total: r.total,
                },
                series::Value::Trend(t) => PortableValue::Trend {
                    count: t.count,
                    sum: t.sum,
                    min: t.min,
                    max: t.max,
                    clamped: t.clamped,
                    buckets: t.buckets.into_iter().map(|b| (b.value, b.count)).collect(),
                },
            };
            Ok(PortableSeries {
                metric: s.metric,
                tags: pairs_from(s.tags),
                value,
            })
        })
        .collect::<Result<_, ConvertError>>()?;
    Ok(PortableSnapshot { series })
}

fn pairs_to(pairs: Vec<(String, String)>) -> Vec<pb::KeyValue> {
    pairs
        .into_iter()
        .map(|(key, value)| pb::KeyValue { key, value })
        .collect()
}

fn pairs_from(pairs: Vec<pb::KeyValue>) -> Vec<(String, String)> {
    pairs.into_iter().map(|kv| (kv.key, kv.value)).collect()
}

fn counts_to(counts: Vec<(String, u64)>) -> Vec<pb::ErrorCount> {
    counts
        .into_iter()
        .map(|(message, count)| pb::ErrorCount { message, count })
        .collect()
}

fn counts_from(counts: Vec<pb::ErrorCount>) -> Vec<(String, u64)> {
    counts.into_iter().map(|c| (c.message, c.count)).collect()
}
