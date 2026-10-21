//! Running a plan across processes. A *manager* engine
//! runs `setup()`, `teardown()`, thresholds and the summary, and hands the
//! scenarios to a [`Remote`] (k9-cli's worker pool); each *worker* engine runs
//! one execution segment's scenarios and reports its metrics and progress
//! every tick. The manager merges them as if it had recorded them itself.

use std::sync::Arc;

use k9_metrics::PortableSnapshot;
use k9_output::Output;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::engine::RunEvent;
use crate::engine::script::{BoxFuture, Script};
use crate::protocol::WorkerProgress;
use crate::segment::Segment;

/// How [`run_with`](crate::engine::run_with) runs a plan.
pub struct RunOptions {
    /// The part of the plan this engine runs.
    pub segment: Segment,
    pub outputs: Vec<Box<dyn Output>>,
    pub script: Option<Arc<dyn Script>>,
    pub role: Role,
}

impl Default for RunOptions {
    fn default() -> RunOptions {
        RunOptions {
            segment: Segment::full(),
            outputs: Vec::new(),
            script: None,
            role: Role::Standalone,
        }
    }
}

pub enum Role {
    /// Runs the hooks and its segment's scenarios.
    Standalone,
    /// Runs its segment's scenarios only (the manager runs the hooks; the
    /// script already has the manager's `setup()` data), without thresholds,
    /// and sends a [`WorkerTick`] every progress tick.
    Worker(mpsc::UnboundedSender<WorkerTick>),
    /// Runs the hooks, thresholds and outputs; the scenarios run in the remote.
    Manager(Arc<dyn Remote>),
}

/// What a worker engine reports, a few times a second.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerTick {
    /// Metrics recorded since the previous tick that carried metrics (once a
    /// second; empty in between). Without `vus`/`vus_max`, which the manager
    /// records itself from everyone's progress.
    pub metrics: PortableSnapshot,
    pub progress: WorkerProgress,
}

/// Where a manager's scenarios run.
pub trait Remote: Send + Sync {
    /// Runs the scenarios with `setup()`'s data, sending updates as they come,
    /// until every part has finished; cancelling `stop` interrupts them all.
    fn run(
        &self,
        setup_data: Option<String>,
        stop: CancellationToken,
        updates: mpsc::UnboundedSender<RemoteUpdate>,
    ) -> BoxFuture<'_, RemoteOutcome>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum RemoteUpdate {
    Tick {
        /// Which part (e.g. worker index) it's from.
        worker: usize,
        metrics: PortableSnapshot,
        progress: WorkerProgress,
    },
    /// An error a part ran into (the manager announces each message once).
    Event(RunEvent),
    /// Raw samples from a part, for the manager's outputs; a part's batches
    /// arrive in order.
    Samples {
        worker: usize,
        samples: k9_metrics::PortableSamples,
    },
}

/// How the remote parts ended, all together.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RemoteOutcome {
    pub iterations_complete: u64,
    pub iterations_interrupted: u64,
    pub script_errors: Vec<(String, u64)>,
    pub request_errors: Vec<(String, u64)>,
    /// Set if a part failed (e.g. a worker process crashed).
    pub failure: Option<String>,
}
