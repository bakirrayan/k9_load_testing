//! The manager ↔ worker protocol, used by `--workers` and `--agents`.
//!
//! The wire format is gRPC (the k9-grpc crate and its `worker.proto`); this
//! module holds the messages themselves. A worker's first message is
//! [`FromWorker::Hello`] with its [`PROTOCOL_VERSION`]; the manager refuses
//! other versions. Then:
//! manager → `Assign` (plan, script, segment, `setup()` data), worker →
//! `Ready`, manager → `Start` (to every worker at once), worker → `Tick` every
//! second and `Event`s as they happen, and finally `Done` (or `Failed`).
//! `Stop` interrupts a running worker (Ctrl-C, `abortOnFail`).

use serde::{Deserialize, Serialize};

use crate::engine::RunEvent;
use crate::plan::Plan;
use crate::segment::Segment;
use k9_metrics::PortableSnapshot;

/// Bumped on every incompatible change to the messages below (3: gRPC;
/// 4: modules, and plans with inlined `.proto` and multipart files).
pub const PROTOCOL_VERSION: u32 = 4;

/// Manager → worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ToWorker {
    Assign(Assignment),
    /// Start the scenarios now.
    Start,
    /// Interrupt every iteration and finish.
    Stop,
}

/// What a worker runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    /// The resolved plan as JSON (its serde layout needs a self-describing format).
    pub plan_json: String,
    /// The script file, which the worker loads itself (a local worker from
    /// this path; an agent on another machine from `script_source`, saved
    /// under this file name).
    pub script: String,
    /// The script's text, for workers that can't read the manager's files.
    pub script_source: Option<String>,
    /// The local modules the script imports, for the same workers: (path
    /// relative to the script's directory, source).
    pub modules: Vec<(String, String)>,
    /// `-e KEY=VALUE` pairs.
    pub env: Vec<(String, String)>,
    pub segment: Segment,
    /// What `setup()` returned in the manager, as JSON.
    pub setup_data: Option<String>,
    /// Send raw samples (`FromWorker::Samples`): the manager has outputs.
    pub forward_samples: bool,
}

impl Assignment {
    pub fn new(
        plan: &Plan,
        segment: Segment,
        script: String,
        env: Vec<(String, String)>,
        setup_data: Option<String>,
    ) -> Assignment {
        Assignment {
            plan_json: serde_json::to_string(plan).expect("plans serialize"),
            script,
            script_source: None,
            modules: Vec::new(),
            env,
            segment,
            setup_data,
            forward_samples: false,
        }
    }

    pub fn plan(&self) -> Result<Plan, serde_json::Error> {
        serde_json::from_str(&self.plan_json)
    }
}

/// Which agent a worker belongs to (`k9 agent --workers N --weight W`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGroup {
    /// The agent's id for this run; empty for a worker on its own.
    pub agent: String,
    /// How many workers the agent runs (0 counts as 1).
    pub workers: u32,
    /// The agent's share of the test relative to other agents' (0 counts as
    /// 1), split evenly among its workers.
    pub weight: u32,
}

/// Worker → manager.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FromWorker {
    Hello {
        protocol: u32,
        /// The worker's k9 version.
        version: String,
        pid: u32,
        /// For a worker `k9 run --workers` started: which of them, from 1 (0
        /// otherwise). Its pid may not be the started process's: on Windows a
        /// venv's python.exe runs the real Python as its child.
        slot: u32,
        /// The secret the manager started it with, so only its own workers join.
        token: String,
        /// For an agent's worker: which agent, and its share of the test.
        group: AgentGroup,
    },
    /// Assignment loaded; waiting for `Start`.
    Ready,
    /// Metrics recorded since the previous tick, and live progress.
    Tick {
        elapsed_ms: u64,
        metrics: PortableSnapshot,
        progress: WorkerProgress,
    },
    /// The first occurrence of an error in this worker.
    Event(RunEvent),
    /// Raw samples for the manager's outputs, in order.
    Samples(k9_metrics::PortableSamples),
    Done(WorkerSummary),
    /// The worker couldn't run its assignment.
    Failed {
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WorkerProgress {
    pub active_vus: u32,
    /// Most VUs active at once so far.
    pub peak_vus: u32,
    pub iterations_complete: u64,
    pub iterations_interrupted: u64,
}

/// A worker's results once its segment has finished.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerSummary {
    pub iterations_complete: u64,
    pub iterations_interrupted: u64,
    pub script_errors: Vec<(String, u64)>,
    pub request_errors: Vec<(String, u64)>,
    /// Metrics recorded since the last tick.
    pub metrics: PortableSnapshot,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error(
        "worker (k9 {version}, pid {pid}) speaks protocol v{protocol}, this k9 speaks v{PROTOCOL_VERSION}: install the same k9 version everywhere"
    )]
    Version {
        protocol: u32,
        version: String,
        pid: u32,
    },
    #[error("expected the worker's Hello, got {0}")]
    Unexpected(String),
    #[error("a process connected without this run's token")]
    Token,
}

/// Checks a worker's first message: a Hello, for this protocol version,
/// with this run's `token`. Returns the worker's pid.
pub fn check_hello(message: &FromWorker, token: &str) -> Result<u32, ProtocolError> {
    match message {
        FromWorker::Hello { token: sent, .. } if sent != token => Err(ProtocolError::Token),
        FromWorker::Hello { protocol, pid, .. } if *protocol == PROTOCOL_VERSION => Ok(*pid),
        FromWorker::Hello {
            protocol,
            version,
            pid,
            ..
        } => Err(ProtocolError::Version {
            protocol: *protocol,
            version: version.clone(),
            pid: *pid,
        }),
        other => Err(ProtocolError::Unexpected(format!("{other:?}"))),
    }
}
