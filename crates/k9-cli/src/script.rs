//! Test scripts: the CLI loads them through a [`ScriptHost`], which the Python
//! package provides (k9-py). The standalone binary has none, so it only runs
//! declarative plans.

use std::path::Path;
use std::sync::Arc;

use k9_core::engine::{BoxFuture, Script, ScriptError};

/// Loads test scripts, e.g. by importing a Python module.
pub trait ScriptHost {
    /// Loads `file`, making `env` (the `-e KEY=VALUE` pairs) visible to it.
    /// Errors are the script's exception, formatted for the user.
    fn load(&self, file: &Path, env: &[(String, String)]) -> Result<LoadedScript, String>;

    /// The program (and leading arguments) that runs this `k9`, to start
    /// worker processes with; `worker …` is appended.
    fn worker_command(&self) -> Vec<String> {
        own_command()
    }

    /// The local modules `file` imports (files under its directory), as
    /// paths relative to that directory: they go to agents with the script.
    fn local_modules(&self, _file: &Path) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }
}

/// This executable, for the standalone binary's workers.
pub(crate) fn own_command() -> Vec<String> {
    let exe = std::env::current_exe()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "k9".to_owned());
    vec![exe]
}

pub struct LoadedScript {
    /// A Tier 1 script's `k9.plan.Plan`, in the plan format: the run is then
    /// declarative, with no Python.
    pub plan: Option<serde_json::Value>,
    /// The script's `options`, as JSON; null if it has none.
    pub options: serde_json::Value,
    /// Functions a scenario's `exec` may name.
    pub functions: Vec<Function>,
    /// Lifecycle hooks the script defines, e.g. `setup`.
    pub hooks: Vec<String>,
    pub script: Arc<dyn Script>,
    /// The script's `handle_summary()`, if it has one.
    pub summary_hook: Option<Arc<dyn SummaryHook>>,
}

/// What `handle_summary()` returned: (target, content) pairs, where target is
/// `stdout`, `stderr` or a file path.
pub type SummaryOutputs = Vec<(String, Vec<u8>)>;

/// A script's `handle_summary(data)` (k6's `handleSummary`): given the end-of-
/// test data, it says what to write where.
pub trait SummaryHook: Send + Sync {
    /// `data` is k6's handleSummary data; `text` is the summary k9 would print.
    fn handle_summary(
        &self,
        data: serde_json::Value,
        text: String,
    ) -> BoxFuture<'_, Result<SummaryOutputs, ScriptError>>;
}

pub struct Function {
    pub name: String,
    /// `async def` (iterations share one event loop) or plain `def`.
    pub is_async: bool,
}

pub(crate) fn is_script(file: &Path) -> bool {
    file.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("py"))
}
