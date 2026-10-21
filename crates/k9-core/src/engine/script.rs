//! The engine's side of a test script. k9-py implements these traits for Python
//! modules; the engine only sees futures, so this crate stays Python-free.

use std::future::Future;
use std::pin::Pin;

use indexmap::IndexMap;
use k9_metrics::{MetricKind, ValueType};

use crate::duration::Duration;
use crate::engine::context::ScriptContext;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An exception raised by script code, already formatted for the user.
/// Iteration errors are counted by `message`, so that should be one line;
/// `details` (e.g. a traceback) is shown the first time a message is seen.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[error("{message}")]
pub struct ScriptError {
    pub message: String,
    pub details: Option<String>,
}

impl ScriptError {
    pub fn new(message: impl Into<String>) -> ScriptError {
        ScriptError {
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(message: impl Into<String>, details: impl Into<String>) -> ScriptError {
        ScriptError {
            message: message.into(),
            details: Some(details.into()),
        }
    }
}

/// A custom metric a script declares (k6's `new Counter(name)` etc.); the
/// engine registers it before the run starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricDecl {
    pub name: String,
    pub kind: MetricKind,
    pub value_type: ValueType,
}

impl MetricDecl {
    pub fn new(name: impl Into<String>, kind: MetricKind, value_type: ValueType) -> MetricDecl {
        MetricDecl {
            name: name.into(),
            kind,
            value_type,
        }
    }
}

/// A loaded test script: its hooks and the code each VU iterates.
pub trait Script: Send + Sync {
    /// Custom metrics the script declared while loading.
    fn metrics(&self) -> Vec<MetricDecl> {
        Vec::new()
    }
    /// Runs the script's `setup()`, if it has one. Called once, before any VU,
    /// with a context whose root group is `::setup`.
    fn setup(&self, ctx: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>>;
    /// What `setup()` returned, as JSON, to send to worker processes.
    fn setup_data(&self) -> Option<String> {
        None
    }
    /// Uses `setup()` data from the manager instead of running `setup()`.
    fn set_setup_data(&self, _json: String) {}
    /// Runs the script's `teardown()`, if it has one. Called once, after every
    /// VU, with a context whose root group is `::teardown`.
    fn teardown(&self, ctx: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>>;
    /// The script side of a new VU; `ctx` sends requests on the VU's client.
    fn vu(&self, vu: VuInfo, ctx: ScriptContext) -> Box<dyn ScriptVu>;
}

/// One VU's view of the script.
pub trait ScriptVu: Send {
    /// Runs once, before the VU's first iteration (the script's `vu_init()`).
    /// If it fails, the VU runs no iterations.
    fn init(&mut self) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async { Ok(()) })
    }
    /// Runs one iteration: the scenario's `exec` function, or the transaction
    /// picked for it. Dropping the future interrupts the iteration, which
    /// must then stop running script code.
    fn iteration(&mut self, function: &str) -> BoxFuture<'_, Result<(), ScriptError>>;
    /// Runs the scenario's `onStart` or `onStop` function: like an
    /// iteration, but it isn't one (the VU's iteration count stays put).
    fn task(&mut self, function: &str) -> BoxFuture<'_, Result<(), ScriptError>> {
        self.iteration(function)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VuInfo {
    /// Unique across the run's scenarios, starting at 1 (k6's `__VU`).
    pub id: u64,
    pub scenario: String,
    /// The function each iteration runs.
    pub exec: String,
    /// The scenario's `env`, on top of the run's environment.
    pub env: IndexMap<String, String>,
}

/// A `setup()` or `teardown()` that failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HookError {
    #[error("{hook}() failed: {error}")]
    Failed {
        hook: &'static str,
        error: ScriptError,
    },
    #[error("{hook}() timed out after {timeout}")]
    TimedOut {
        hook: &'static str,
        timeout: Duration,
    },
}

/// Runs a hook within its timeout.
pub(crate) async fn run_hook(
    hook: &'static str,
    timeout: Duration,
    call: BoxFuture<'_, Result<(), ScriptError>>,
) -> Result<(), HookError> {
    match tokio::time::timeout(timeout.as_std(), call).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(HookError::Failed { hook, error }),
        Err(_) => Err(HookError::TimedOut { hook, timeout }),
    }
}
