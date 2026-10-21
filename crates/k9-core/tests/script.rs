//! The engine driving a script (the interface k9-py implements for Python).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use k9_core::engine::{
    self, BoxFuture, EngineError, HookError, RunControl, Script, ScriptContext, ScriptError,
    ScriptVu, VuInfo,
};
use k9_core::{Plan, PlanConfig};

fn plan(options: &str) -> Plan {
    PlanConfig {
        options: serde_json::from_str(options).unwrap(),
        ..Default::default()
    }
    .resolve_script(&["default", "other"])
    .unwrap_or_else(|e| panic!("{e}"))
}

/// A script whose behaviour is set per test; it logs what the engine asked of it.
#[derive(Default)]
struct Fake {
    log: Mutex<Vec<String>>,
    iteration_time: Duration,
    /// Every n-th iteration (counted per VU) fails.
    fail_every: Option<u64>,
    setup_fails: bool,
    setup_time: Duration,
    teardown_fails: bool,
    /// Iterations whose future was dropped before finishing.
    dropped: Arc<AtomicU64>,
}

impl Fake {
    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
}

impl Script for Fake {
    fn setup(&self, _ctx: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async move {
            tokio::time::sleep(self.setup_time).await;
            self.log.lock().unwrap().push("setup".into());
            if self.setup_fails {
                return Err(ScriptError::new("ValueError: no token"));
            }
            Ok(())
        })
    }

    fn teardown(&self, _ctx: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async move {
            self.log.lock().unwrap().push("teardown".into());
            if self.teardown_fails {
                return Err(ScriptError::new("RuntimeError: cleanup"));
            }
            Ok(())
        })
    }

    fn vu(&self, vu: VuInfo, _ctx: ScriptContext) -> Box<dyn ScriptVu> {
        self.log
            .lock()
            .unwrap()
            .push(format!("vu {} {} {}", vu.id, vu.scenario, vu.exec));
        Box::new(FakeVu {
            iterations: 0,
            time: self.iteration_time,
            fail_every: self.fail_every,
            dropped: self.dropped.clone(),
        })
    }
}

struct FakeVu {
    iterations: u64,
    time: Duration,
    fail_every: Option<u64>,
    dropped: Arc<AtomicU64>,
}

/// Counts a drop unless disarmed.
struct DropGuard(Option<Arc<AtomicU64>>);

impl Drop for DropGuard {
    fn drop(&mut self) {
        if let Some(counter) = &self.0 {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl ScriptVu for FakeVu {
    fn iteration(&mut self, _function: &str) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async move {
            let mut guard = DropGuard(Some(self.dropped.clone()));
            self.iterations += 1;
            tokio::time::sleep(self.time).await;
            guard.0 = None;
            match self.fail_every {
                Some(n) if self.iterations.is_multiple_of(n) => {
                    Err(ScriptError::new("ZeroDivisionError: division by zero"))
                }
                _ => Ok(()),
            }
        })
    }
}

async fn run(plan: &Plan, script: Arc<Fake>) -> Result<engine::RunResult, EngineError> {
    engine::run_script(plan, RunControl::new(), Vec::new(), script).await
}

#[tokio::test]
async fn runs_setup_then_iterations_then_teardown() {
    let script = Arc::new(Fake::default());
    let plan = plan(
        r#"{"scenarios": {"s": {"executor": "per-vu-iterations", "vus": 2, "iterations": 3}}}"#,
    );
    let result = run(&plan, script.clone()).await.unwrap();

    assert_eq!(result.iterations_complete, 6);
    let log = script.log();
    assert_eq!(log.first().unwrap(), "setup");
    assert_eq!(log.last().unwrap(), "teardown");
    let mut vus: Vec<_> = log[1..log.len() - 1].to_vec();
    vus.sort();
    assert_eq!(vus, ["vu 1 s default", "vu 2 s default"]);
    assert!(result.teardown_error.is_none());
    assert!(result.script_errors.is_empty());
}

#[tokio::test]
async fn each_scenario_runs_its_exec_function_and_vu_ids_are_unique() {
    let script = Arc::new(Fake::default());
    let plan = plan(
        r#"{"scenarios": {
            "a": {"executor": "shared-iterations", "vus": 2, "iterations": 2},
            "b": {"executor": "shared-iterations", "vus": 1, "iterations": 1, "exec": "other"}}}"#,
    );
    run(&plan, script.clone()).await.unwrap();
    let mut vus: Vec<_> = script
        .log()
        .into_iter()
        .filter(|l| l.starts_with("vu "))
        .collect();
    vus.sort();
    let ids: Vec<&str> = vus.iter().map(|l| l.split(' ').nth(1).unwrap()).collect();
    let mut unique = ids.clone();
    unique.dedup();
    assert_eq!(unique.len(), 3, "{vus:?}");
    assert_eq!(vus.iter().filter(|l| l.ends_with(" b other")).count(), 1);
    assert_eq!(vus.iter().filter(|l| l.ends_with(" a default")).count(), 2);
}

#[tokio::test]
async fn iteration_errors_are_counted_and_the_iterations_still_complete() {
    let script = Arc::new(Fake {
        fail_every: Some(2),
        ..Fake::default()
    });
    let plan = plan(r#"{"vus": 1, "iterations": 10}"#);
    let result = run(&plan, script).await.unwrap();
    assert_eq!(result.iterations_complete, 10, "as in k6");
    assert_eq!(
        result.script_errors,
        [("ZeroDivisionError: division by zero".to_owned(), 5)]
    );
}

#[tokio::test]
async fn a_failing_setup_stops_the_run_before_any_iteration() {
    let script = Arc::new(Fake {
        setup_fails: true,
        ..Fake::default()
    });
    let plan = plan(r#"{"vus": 1, "iterations": 10}"#);
    let err = run(&plan, script.clone()).await.err().unwrap();
    assert!(
        matches!(&err, EngineError::Setup(HookError::Failed { hook: "setup", error })
            if error.message == "ValueError: no token"),
        "{err:?}"
    );
    assert_eq!(err.to_string(), "setup() failed: ValueError: no token");
    assert_eq!(script.log(), ["setup"], "no VUs, no teardown");
}

#[tokio::test]
async fn setup_timeout_is_enforced() {
    let script = Arc::new(Fake {
        setup_time: Duration::from_secs(5),
        ..Fake::default()
    });
    let plan = plan(r#"{"setupTimeout": "100ms"}"#);
    let started = std::time::Instant::now();
    let err = run(&plan, script).await.err().unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(
        matches!(
            err,
            EngineError::Setup(HookError::TimedOut { hook: "setup", .. })
        ),
        "{err:?}"
    );
    assert_eq!(err.to_string(), "setup() timed out after 100ms");
}

#[tokio::test]
async fn a_failing_teardown_is_reported_with_the_results() {
    let script = Arc::new(Fake {
        teardown_fails: true,
        ..Fake::default()
    });
    let plan = plan(r#"{"vus": 1, "iterations": 2}"#);
    let result = run(&plan, script).await.unwrap();
    assert_eq!(result.iterations_complete, 2);
    let err = result.teardown_error.expect("teardown failed");
    assert_eq!(err.to_string(), "teardown() failed: RuntimeError: cleanup");
}

#[tokio::test]
async fn interrupted_iterations_are_dropped_and_teardown_still_runs() {
    let script = Arc::new(Fake {
        iteration_time: Duration::from_secs(10),
        ..Fake::default()
    });
    let plan = plan(
        r#"{"scenarios": {"s": {"executor": "constant-vus", "vus": 3, "duration": "100ms", "gracefulStop": "100ms"}}}"#,
    );
    let result = run(&plan, script.clone()).await.unwrap();
    assert_eq!(result.iterations_interrupted, 3);
    assert_eq!(script.dropped.load(Ordering::Relaxed), 3);
    assert_eq!(script.log().last().unwrap(), "teardown", "as in k6");
}
