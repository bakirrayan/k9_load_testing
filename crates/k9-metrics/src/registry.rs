//! Metric definitions. A registry is built before a run (built-ins plus any
//! custom metrics a script declares) and is read-only while the run is live.

use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MetricId(pub(crate) u32);

impl MetricId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// k6's four metric types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// Cumulative sum (`http_reqs`, `data_sent`).
    Counter,
    /// Latest value, plus min and max (`vus`).
    Gauge,
    /// Share of non-zero samples (`checks`, `http_req_failed`).
    Rate,
    /// Distribution: avg, min, med, max, percentiles (`http_req_duration`).
    Trend,
}

/// What a metric's values mean, for display. Time values are milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Default,
    Time,
    Data,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricDef {
    pub name: String,
    pub kind: MetricKind,
    pub value_type: ValueType,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetricError {
    #[error(
        "invalid metric name {0:?}: use letters, digits and `_`, starting with a letter or `_` (max 128 characters)"
    )]
    InvalidName(String),
    #[error("metric `{name}` is already registered as a {existing:?}, not a {requested:?}")]
    KindMismatch {
        name: String,
        existing: MetricKind,
        requested: MetricKind,
    },
}

#[derive(Debug, Clone, Default)]
pub struct Registry {
    defs: Vec<MetricDef>,
    by_name: HashMap<String, MetricId>,
}

/// Ids of k6's built-in metrics, for direct use on the hot path.
#[derive(Debug, Clone, Copy)]
pub struct Builtins {
    pub vus: MetricId,
    pub vus_max: MetricId,
    pub iterations: MetricId,
    pub iteration_duration: MetricId,
    pub dropped_iterations: MetricId,
    pub data_sent: MetricId,
    pub data_received: MetricId,
    pub checks: MetricId,
    pub group_duration: MetricId,
    pub http_reqs: MetricId,
    pub http_req_duration: MetricId,
    pub http_req_blocked: MetricId,
    pub http_req_connecting: MetricId,
    pub http_req_tls_handshaking: MetricId,
    pub http_req_sending: MetricId,
    pub http_req_waiting: MetricId,
    pub http_req_receiving: MetricId,
    pub http_req_failed: MetricId,
    /// k9's own: iteration time measured from when an arrival-rate iteration
    /// was due, not when it started (coordinated omission).
    pub iteration_duration_corrected: MetricId,
    pub ws_sessions: MetricId,
    pub ws_connecting: MetricId,
    pub ws_session_duration: MetricId,
    pub ws_msgs_sent: MetricId,
    pub ws_msgs_received: MetricId,
    pub ws_ping: MetricId,
    pub grpc_req_duration: MetricId,
    pub grpc_streams: MetricId,
    pub grpc_streams_msgs_sent: MetricId,
    pub grpc_streams_msgs_received: MetricId,
    /// How long each stream lasted, from opening to its end (or cancel).
    pub grpc_stream_duration: MetricId,
}

impl Registry {
    pub fn new() -> Registry {
        Registry::default()
    }

    /// A registry holding k6's built-in metrics.
    pub fn with_builtins() -> (Registry, Builtins) {
        use MetricKind::*;
        use ValueType::*;
        let mut r = Registry::new();
        let mut add = |name: &str, kind, value_type| {
            r.register(name, kind, value_type)
                .expect("built-in metric names are valid and unique")
        };
        let builtins = Builtins {
            vus: add("vus", Gauge, Default),
            vus_max: add("vus_max", Gauge, Default),
            iterations: add("iterations", Counter, Default),
            iteration_duration: add("iteration_duration", Trend, Time),
            dropped_iterations: add("dropped_iterations", Counter, Default),
            data_sent: add("data_sent", Counter, Data),
            data_received: add("data_received", Counter, Data),
            checks: add("checks", Rate, Default),
            group_duration: add("group_duration", Trend, Time),
            http_reqs: add("http_reqs", Counter, Default),
            http_req_duration: add("http_req_duration", Trend, Time),
            http_req_blocked: add("http_req_blocked", Trend, Time),
            http_req_connecting: add("http_req_connecting", Trend, Time),
            http_req_tls_handshaking: add("http_req_tls_handshaking", Trend, Time),
            http_req_sending: add("http_req_sending", Trend, Time),
            http_req_waiting: add("http_req_waiting", Trend, Time),
            http_req_receiving: add("http_req_receiving", Trend, Time),
            http_req_failed: add("http_req_failed", Rate, Default),
            iteration_duration_corrected: add("iteration_duration_corrected", Trend, Time),
            ws_sessions: add("ws_sessions", Counter, Default),
            ws_connecting: add("ws_connecting", Trend, Time),
            ws_session_duration: add("ws_session_duration", Trend, Time),
            ws_msgs_sent: add("ws_msgs_sent", Counter, Default),
            ws_msgs_received: add("ws_msgs_received", Counter, Default),
            ws_ping: add("ws_ping", Trend, Time),
            grpc_req_duration: add("grpc_req_duration", Trend, Time),
            grpc_streams: add("grpc_streams", Counter, Default),
            grpc_streams_msgs_sent: add("grpc_streams_msgs_sent", Counter, Default),
            grpc_streams_msgs_received: add("grpc_streams_msgs_received", Counter, Default),
            grpc_stream_duration: add("grpc_stream_duration", Trend, Time),
        };
        (r, builtins)
    }

    /// Registers a metric, or returns the existing id if one with the same name
    /// and kind exists.
    pub fn register(
        &mut self,
        name: &str,
        kind: MetricKind,
        value_type: ValueType,
    ) -> Result<MetricId, MetricError> {
        if !is_valid_name(name) {
            return Err(MetricError::InvalidName(name.to_owned()));
        }
        if let Some(&id) = self.by_name.get(name) {
            let existing = self.def(id).kind;
            return if existing == kind {
                Ok(id)
            } else {
                Err(MetricError::KindMismatch {
                    name: name.to_owned(),
                    existing,
                    requested: kind,
                })
            };
        }
        let id = MetricId(u32::try_from(self.defs.len()).expect("fewer than 2^32 metrics"));
        self.defs.push(MetricDef {
            name: name.to_owned(),
            kind,
            value_type,
        });
        self.by_name.insert(name.to_owned(), id);
        Ok(id)
    }

    pub fn get(&self, name: &str) -> Option<MetricId> {
        self.by_name.get(name).copied()
    }

    pub fn def(&self, id: MetricId) -> &MetricDef {
        &self.defs[id.index()]
    }

    pub fn len(&self) -> usize {
        self.defs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.defs.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (MetricId, &MetricDef)> {
        self.defs
            .iter()
            .enumerate()
            .map(|(i, def)| (MetricId(i as u32), def))
    }
}

/// k6's rule: `^[a-zA-Z_][a-zA-Z0-9_]{0,127}$`.
fn is_valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    name.len() <= 128
        && (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl fmt::Display for MetricKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
