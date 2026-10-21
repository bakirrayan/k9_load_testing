//! The manager and worker roles, in one process: a fake
//! remote runs worker-role engines on tasks, the way the real one runs them
//! in worker processes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use k9_core::engine::{
    self, BoxFuture, MetricDecl, Remote, RemoteOutcome, RemoteUpdate, Role, RunControl, RunEvent,
    RunOptions, RunResult, Script, ScriptContext, ScriptError, ScriptVu, VuInfo, WorkerTick,
};
use k9_core::{Plan, PlanConfig, Segment};
use k9_metrics::{MetricKind, Value, ValueType};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// A script that counts hooks and adds to a custom counter every iteration.
#[derive(Default)]
struct Counting {
    setups: AtomicU32,
    teardowns: AtomicU32,
    /// What the manager's setup() "returned", or what a worker was given.
    data: Mutex<Option<String>>,
    /// The iteration raises this error, if set.
    error: Option<&'static str>,
    iteration_time: Duration,
}

impl Script for Counting {
    fn metrics(&self) -> Vec<MetricDecl> {
        vec![MetricDecl::new(
            "work",
            MetricKind::Counter,
            ValueType::Default,
        )]
    }
    fn setup(&self, _: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        self.setups.fetch_add(1, Ordering::Relaxed);
        *self.data.lock().unwrap() = Some(r#"{"token":"abc"}"#.into());
        Box::pin(async { Ok(()) })
    }
    fn teardown(&self, _: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        self.teardowns.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(()) })
    }
    fn setup_data(&self) -> Option<String> {
        self.data.lock().unwrap().clone()
    }
    fn set_setup_data(&self, json: String) {
        *self.data.lock().unwrap() = Some(json);
    }
    fn vu(&self, _: VuInfo, ctx: ScriptContext) -> Box<dyn ScriptVu> {
        Box::new(CountingVu {
            ctx,
            error: self.error,
            time: self.iteration_time,
        })
    }
}

struct CountingVu {
    ctx: ScriptContext,
    error: Option<&'static str>,
    time: Duration,
}

impl ScriptVu for CountingVu {
    fn iteration(&mut self, _function: &str) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async move {
            tokio::time::sleep(self.time).await;
            self.ctx.add("work", 1.0, "", &[]).unwrap();
            match self.error {
                Some(message) => Err(ScriptError::new(message)),
                None => Ok(()),
            }
        })
    }
}

/// Runs each segment in a worker-role engine on a task.
struct FakeRemote {
    plan: Plan,
    workers: Vec<Arc<Counting>>,
    /// Makes the remote report a failure instead of running.
    fail: Option<&'static str>,
}

impl Remote for FakeRemote {
    fn run(
        &self,
        setup_data: Option<String>,
        stop: CancellationToken,
        updates: mpsc::UnboundedSender<RemoteUpdate>,
    ) -> BoxFuture<'_, RemoteOutcome> {
        Box::pin(async move {
            if let Some(failure) = self.fail {
                return RemoteOutcome {
                    failure: Some(failure.into()),
                    ..RemoteOutcome::default()
                };
            }
            let segments = Segment::split(self.workers.len() as u32);
            let mut tasks = Vec::new();
            for (i, (script, segment)) in self.workers.iter().zip(segments).enumerate() {
                if let Some(data) = &setup_data {
                    script.set_setup_data(data.clone());
                }
                let control = RunControl::new();
                let (ticks_tx, mut ticks) = mpsc::unbounded_channel::<WorkerTick>();
                let mut events = control.events();
                let forward = updates.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            tick = ticks.recv() => match tick {
                                Some(tick) => {
                                    let _ = forward.send(RemoteUpdate::Tick { worker: i, metrics: tick.metrics, progress: tick.progress });
                                }
                                None => break,
                            },
                            Ok(event) = events.recv() => { let _ = forward.send(RemoteUpdate::Event(event)); }
                        }
                    }
                });
                let (stop, control2) = (stop.clone(), control.clone());
                tokio::spawn(async move {
                    stop.cancelled().await;
                    control2.stop();
                });
                let plan = self.plan.clone();
                let script: Arc<dyn Script> = script.clone();
                tasks.push(tokio::spawn(async move {
                    let options = RunOptions {
                        segment,
                        script: Some(script),
                        role: Role::Worker(ticks_tx),
                        ..RunOptions::default()
                    };
                    engine::run_with(&plan, control, options).await.unwrap()
                }));
            }
            let mut outcome = RemoteOutcome::default();
            for task in tasks {
                let result: RunResult = task.await.unwrap();
                outcome.iterations_complete += result.iterations_complete;
                outcome.iterations_interrupted += result.iterations_interrupted;
                outcome.script_errors.extend(result.script_errors);
            }
            outcome
        })
    }
}

fn plan(options: serde_json::Value) -> Plan {
    PlanConfig {
        options: serde_json::from_value(options).unwrap(),
        ..Default::default()
    }
    .resolve_script(&["default"])
    .unwrap_or_else(|e| panic!("{e}"))
}

struct Setup {
    manager: Arc<Counting>,
    workers: Vec<Arc<Counting>>,
}

fn scripts(workers: usize, worker: impl Fn() -> Counting) -> Setup {
    Setup {
        manager: Arc::new(Counting::default()),
        workers: (0..workers).map(|_| Arc::new(worker())).collect(),
    }
}

async fn run_managed(
    plan: &Plan,
    s: &Setup,
    control: RunControl,
    fail: Option<&'static str>,
) -> RunResult {
    let remote = Arc::new(FakeRemote {
        plan: plan.clone(),
        workers: s.workers.clone(),
        fail,
    });
    let options = RunOptions {
        script: Some(s.manager.clone()),
        role: Role::Manager(remote),
        ..RunOptions::default()
    };
    engine::run_with(plan, control, options).await.unwrap()
}

fn counter(result: &RunResult, name: &str) -> f64 {
    let m = result.aggregator.metrics();
    let id = m.registry().get(name).unwrap();
    match result.aggregator.totals().merged(id, &[], m.interner()) {
        Some(Value::Counter(c)) => c.sum,
        other => panic!("{name}: {other:?}"),
    }
}

#[tokio::test]
async fn hooks_run_once_in_the_manager_and_workers_share_the_load() {
    let s = scripts(3, Counting::default);
    let plan = plan(
        serde_json::json!({"scenarios": {"s": {"executor": "shared-iterations", "vus": 5, "iterations": 101}}}),
    );
    let result = run_managed(&plan, &s, RunControl::new(), None).await;

    assert_eq!(result.iterations_complete, 101);
    assert_eq!(
        counter(&result, "iterations"),
        101.0,
        "workers' metrics reach the manager"
    );
    assert_eq!(counter(&result, "work"), 101.0);
    assert_eq!(s.manager.setups.load(Ordering::Relaxed), 1);
    assert_eq!(s.manager.teardowns.load(Ordering::Relaxed), 1);
    for worker in &s.workers {
        assert_eq!(
            worker.setups.load(Ordering::Relaxed) + worker.teardowns.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            worker.data.lock().unwrap().as_deref(),
            Some(r#"{"token":"abc"}"#),
            "setup data reached the worker"
        );
    }
    assert_eq!(result.max_vus, 5);
    let m = result.aggregator.metrics();
    let vus = m.registry().get("vus").unwrap();
    match result.aggregator.totals().merged(vus, &[], m.interner()) {
        Some(Value::Gauge(g)) => assert!(g.max >= 1.0 && g.max <= 5.0, "{g:?}"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn thresholds_see_every_workers_samples_and_abort_them_all() {
    let s = scripts(2, || Counting {
        iteration_time: Duration::from_millis(10),
        ..Counting::default()
    });
    let plan = plan(serde_json::json!({
        "scenarios": {"s": {"executor": "constant-vus", "vus": 4, "duration": "30s"}},
        "thresholds": {"work": [{"threshold": "count<20", "abortOnFail": true}]},
    }));
    let started = std::time::Instant::now();
    let result = run_managed(&plan, &s, RunControl::new(), None).await;
    assert!(result.aborted_by_threshold);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    assert!(result.thresholds_failed());
    assert_eq!(
        s.manager.teardowns.load(Ordering::Relaxed),
        1,
        "teardown after an abort, as in k6"
    );
}

#[tokio::test]
async fn stopping_the_manager_stops_the_workers() {
    let s = scripts(2, || Counting {
        iteration_time: Duration::from_secs(30),
        ..Counting::default()
    });
    let plan = plan(
        serde_json::json!({"scenarios": {"s": {"executor": "constant-vus", "vus": 4, "duration": "1m"}}}),
    );
    let control = RunControl::new();
    let stopper = control.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        stopper.stop();
    });
    let started = std::time::Instant::now();
    let result = run_managed(&plan, &s, control, None).await;
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(result.stopped);
    assert_eq!(result.iterations_interrupted, 4);
}

#[tokio::test]
async fn errors_are_counted_across_workers_and_announced_once() {
    let s = scripts(3, || Counting {
        error: Some("ValueError: bad"),
        ..Counting::default()
    });
    let plan = plan(serde_json::json!({"vus": 3, "iterations": 9}));
    let control = RunControl::new();
    let mut events = control.events();
    let result = run_managed(&plan, &s, control, None).await;
    assert_eq!(result.script_errors, [("ValueError: bad".to_owned(), 9)]);
    let mut seen: HashMap<String, u32> = HashMap::new();
    while let Ok(event) = events.try_recv() {
        if let RunEvent::ScriptError(err) = event {
            *seen.entry(err.message).or_default() += 1;
        }
    }
    assert_eq!(seen, HashMap::from([("ValueError: bad".to_owned(), 1)]));
}

#[tokio::test]
async fn a_failing_remote_is_reported_and_teardown_still_runs() {
    let s = scripts(2, Counting::default);
    let plan = plan(serde_json::json!({"vus": 2, "iterations": 4}));
    let result = run_managed(
        &plan,
        &s,
        RunControl::new(),
        Some("worker 1 (pid 42) exited with code 3"),
    )
    .await;
    assert_eq!(
        result.remote_failure.as_deref(),
        Some("worker 1 (pid 42) exited with code 3")
    );
    assert_eq!(s.manager.teardowns.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn progress_sums_the_workers() {
    let s = scripts(2, || Counting {
        iteration_time: Duration::from_millis(50),
        ..Counting::default()
    });
    let plan = plan(serde_json::json!({"vus": 4, "duration": "1500ms"}));
    let control = RunControl::new();
    let mut progress = control.progress();
    let peak = Arc::new(AtomicU32::new(0));
    let seen = peak.clone();
    tokio::spawn(async move {
        while progress.changed().await.is_ok() {
            let p = *progress.borrow_and_update();
            seen.fetch_max(p.active_vus, Ordering::Relaxed);
        }
    });
    let result = run_managed(&plan, &s, control, None).await;
    assert!(result.iterations_complete > 20);
    assert_eq!(
        peak.load(Ordering::Relaxed),
        4,
        "2 VUs in each of 2 workers"
    );
}
