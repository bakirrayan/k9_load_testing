//! The engine: runs a [`Plan`]'s scenarios, drives VUs through their flows, and
//! records k6's metrics.
//!
//! Lifecycle: init (compile flows, build clients) → running (scenarios start at
//! their `startTime`) → graceful stop (per scenario: no new iterations, in-flight
//! ones get `gracefulStop` to finish) → done. [`RunControl::stop`] interrupts
//! every in-flight iteration at once, like k6's first Ctrl-C; the run still
//! returns its results. A run can start paused ([`RunControl::pause`] before
//! it starts): it waits for [`RunControl::resume`] before `setup()`.
//!
//! Scripts (e.g. Python modules, via k9-py) plug in through [`Script`]: its
//! `setup()` runs before the scenarios start, `teardown()` after they end, and
//! each VU runs the scenario's `exec` function instead of a flow. Script code
//! sends requests and records metrics through a [`ScriptContext`].
//!
//! All of k6's executors are implemented. `externally-controlled` runs
//! [`RunControl::scale`]'s number of VUs (up to its `maxVUs`) for its
//! `duration`, not counting time spent paused; it has no graceful stop, and a
//! run whose scenarios are all externally-controlled can pause mid-way.

mod arrival;
mod compile;
mod context;
mod extract;
mod feeder;
mod grpc;
mod live;
mod ramp;
mod remote;
mod script;
mod tags;
mod template;
mod thresholds;
mod vu;
mod websocket;

pub use context::{RequestMeta, ScriptContext, UnknownMetric};
pub use feeder::rows as feeder_rows;
pub use grpc::{GrpcStreamSession, StreamReceiver, StreamSender};
pub use live::{ControlError, LiveCheck, LiveFrame, LiveMetric, Status};
pub use remote::{Remote, RemoteOutcome, RemoteUpdate, Role, RunOptions, WorkerTick};
pub use script::{BoxFuture, HookError, MetricDecl, Script, ScriptError, ScriptVu, VuInfo};
pub use thresholds::ThresholdOutcome;
pub use websocket::WsSession;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use k9_http::{Client, ClientConfig};
use k9_metrics::{Aggregator, Builtins, Metrics, PortableSnapshot, Registry, TagSetId};
use k9_output::Output;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::engine::compile::compile;
use crate::engine::live::Live;
use crate::engine::thresholds::Thresholds;
use crate::engine::vu::{Budget, ScenarioCtx, Shared, Transaction, Vu};
use crate::plan::{Executor, HttpVersion, Plan, Scenario};
use crate::protocol::WorkerProgress;
use crate::segment::Segment;

/// How often progress is published; metrics are aggregated every 4th tick (1 s).
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);
const TICKS_PER_AGGREGATION: u32 = 4;

/// Handle for stopping a run and watching its progress. Cheap to clone.
#[derive(Clone)]
pub struct RunControl {
    stop: CancellationToken,
    progress: watch::Sender<Progress>,
    events: broadcast::Sender<RunEvent>,
    live: Arc<Live>,
    metrics: watch::Sender<Arc<Vec<LiveMetric>>>,
    frames: watch::Sender<Arc<LiveFrame>>,
}

/// Something worth telling the user while the run is live.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RunEvent {
    /// The first time script code raised this error (by message).
    ScriptError(ScriptError),
    /// The first time a request failed with this error.
    RequestError(String),
}

impl Default for RunControl {
    fn default() -> Self {
        RunControl::new()
    }
}

impl RunControl {
    pub fn new() -> RunControl {
        RunControl {
            stop: CancellationToken::new(),
            progress: watch::Sender::new(Progress::default()),
            // Only first occurrences are sent, so a small buffer is plenty.
            events: broadcast::Sender::new(256),
            live: Arc::new(Live::new()),
            metrics: watch::Sender::new(Arc::new(Vec::new())),
            frames: watch::Sender::new(Arc::new(LiveFrame::default())),
        }
    }

    /// A [`LiveFrame`] every second while the run is live (a dashboard's feed).
    pub fn frames(&self) -> watch::Receiver<Arc<LiveFrame>> {
        self.frames.subscribe()
    }

    /// Errors as they first occur. Subscribe before the run starts.
    pub fn events(&self) -> broadcast::Receiver<RunEvent> {
        self.events.subscribe()
    }

    /// Interrupts all in-flight iterations and ends the run.
    pub fn stop(&self) {
        self.stop.cancel();
    }

    pub fn is_stopped(&self) -> bool {
        self.stop.is_cancelled()
    }

    /// Completes once [`RunControl::stop`] has been called.
    pub async fn stopped(&self) {
        self.stop.cancelled().await
    }

    /// Updated a few times per second while the run is live.
    pub fn progress(&self) -> watch::Receiver<Progress> {
        self.progress.subscribe()
    }

    /// Holds new iterations (in-flight ones finish), and the
    /// externally-controlled scenario's clock. Before the run starts, any run
    /// can be paused: it then waits for [`RunControl::resume`] to start.
    pub fn pause(&self) -> Result<(), ControlError> {
        self.live.pause()
    }

    pub fn resume(&self) -> Result<(), ControlError> {
        self.live.resume()
    }

    /// Sets the externally-controlled scenario's VUs (up to its `maxVUs`).
    /// VUs taken away finish their current iteration first.
    pub fn scale(&self, vus: u32) -> Result<(), ControlError> {
        self.live.scale(vus)
    }

    /// Reads `plan` before the run starts, so that a control API serving
    /// already can scale its externally-controlled scenario (the run keeps
    /// what was set) and report its max VUs.
    pub fn prepare(&self, plan: &Plan) {
        let vus_max = plan
            .options
            .scenarios
            .values()
            .map(|s| s.executor.max_vus())
            .sum();
        self.live.configure(plan, vus_max);
    }

    /// Every metric's totals, updated each second while someone watches.
    pub fn metrics(&self) -> watch::Receiver<Arc<Vec<LiveMetric>>> {
        self.metrics.subscribe()
    }

    pub fn status(&self) -> Status {
        let (paused, running, vus_max) = self.live.status();
        let progress = *self.progress.borrow();
        Status {
            paused,
            running,
            stopped: self.is_stopped(),
            vus: progress.active_vus,
            vus_max: vus_max.max(progress.max_vus),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Progress {
    pub elapsed: Duration,
    pub active_vus: u32,
    pub max_vus: u32,
    pub iterations_complete: u64,
    pub iterations_interrupted: u64,
}

pub struct RunResult {
    /// Totals and per-second time series of every metric.
    pub aggregator: Aggregator,
    pub builtins: Builtins,
    pub duration: Duration,
    pub max_vus: u32,
    pub iterations_complete: u64,
    pub iterations_interrupted: u64,
    /// Failed requests (no HTTP response), by error message, most frequent first.
    pub request_errors: Vec<(String, u64)>,
    /// Exceptions raised by script iterations, by message, most frequent first.
    pub script_errors: Vec<(String, u64)>,
    /// Set if the script's `teardown()` failed.
    pub teardown_error: Option<HookError>,
    /// True if [`RunControl::stop`] ended the run early.
    pub stopped: bool,
    /// Every threshold expression, evaluated on the final totals.
    pub thresholds: Vec<ThresholdOutcome>,
    /// True if a threshold with `abortOnFail` ended the run early.
    pub aborted_by_threshold: bool,
    /// Outputs that failed (e.g. disk full); a failed output stops receiving samples.
    pub output_errors: Vec<String>,
    /// For a manager: why its remote part failed (e.g. a worker crashed).
    pub remote_failure: Option<String>,
}

impl RunResult {
    pub fn thresholds_failed(&self) -> bool {
        self.thresholds.iter().any(|t| !t.passed)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(
        "scenario `{scenario}` is externally-controlled, so it can't be split across workers or execution segments"
    )]
    Unsplittable { scenario: String },
    #[error("{location}: {message}")]
    InvalidRequest { location: String, message: String },
    #[error("feeder `{name}`: {message}")]
    InvalidFeeder { name: String, message: String },
    #[error("could not set up the HTTP client: {0}")]
    Client(k9_http::HttpError),
    #[error("invalid threshold on `{selector}`: {message}")]
    InvalidThreshold { selector: String, message: String },
    #[error("custom metric: {0}")]
    Metric(#[from] k9_metrics::MetricError),
    /// The script's `setup()` failed, so no scenario ran.
    #[error(transparent)]
    Setup(HookError),
}

/// Runs `plan` to completion (or until `control` stops it).
pub async fn run(plan: &Plan, control: RunControl) -> Result<RunResult, EngineError> {
    run_with_outputs(plan, control, Vec::new()).await
}

/// Like [`run`], also streaming every raw sample to `outputs` once per second.
pub async fn run_with_outputs(
    plan: &Plan,
    control: RunControl,
    outputs: Vec<Box<dyn Output>>,
) -> Result<RunResult, EngineError> {
    let options = RunOptions {
        outputs,
        ..RunOptions::default()
    };
    run_with(plan, control, options).await
}

/// Runs a script's plan: `setup()`, the scenarios (each VU iterating the
/// scenario's `exec` function), then `teardown()`.
pub async fn run_script(
    plan: &Plan,
    control: RunControl,
    outputs: Vec<Box<dyn Output>>,
    script: Arc<dyn Script>,
) -> Result<RunResult, EngineError> {
    let options = RunOptions {
        outputs,
        script: Some(script),
        ..RunOptions::default()
    };
    run_with(plan, control, options).await
}

/// Runs one execution segment of `plan`: this engine's share of
/// every scenario's VUs, iterations and ramp steps. Engines given the
/// segments of one sequence together run exactly the whole plan, with
/// disjoint VU ids.
pub async fn run_segment(
    plan: &Plan,
    segment: Segment,
    control: RunControl,
    outputs: Vec<Box<dyn Output>>,
    script: Option<Arc<dyn Script>>,
) -> Result<RunResult, EngineError> {
    let options = RunOptions {
        segment,
        outputs,
        script,
        role: Role::Standalone,
    };
    run_with(plan, control, options).await
}

/// Runs `plan` as `options` say: which segment, which script, and whether
/// this engine stands alone, is a worker, or manages remote workers.
/// `tlsAuth`'s certificates and keys, read from their files unless inline.
/// The plan's network options, parsed (they were checked when it loaded).
pub(crate) fn network(
    http: &crate::plan::HttpOptions,
) -> Result<crate::plan::Network, EngineError> {
    http.network().map_err(|issues| {
        let text = issues
            .iter()
            .map(|i| format!("{}: {}", i.path, i.message))
            .collect::<Vec<_>>()
            .join("; ");
        EngineError::Client(k9_http::HttpError::Config(text))
    })
}

fn client_certs(
    auth: &[crate::plan::TlsAuth],
) -> Result<Vec<k9_http::ClientCert>, k9_http::HttpError> {
    let pem = |value: &str| -> Result<String, k9_http::HttpError> {
        if crate::plan::TlsAuth::is_pem(value) {
            Ok(value.to_owned())
        } else {
            std::fs::read_to_string(value)
                .map_err(|e| k9_http::HttpError::Config(format!("tlsAuth: reading {value}: {e}")))
        }
    };
    auth.iter()
        .map(|a| {
            Ok(k9_http::ClientCert {
                domains: a.domains.clone(),
                cert_pem: pem(&a.cert)?,
                key_pem: pem(&a.key)?,
            })
        })
        .collect()
}

pub async fn run_with(
    plan: &Plan,
    control: RunControl,
    options: RunOptions,
) -> Result<RunResult, EngineError> {
    let RunOptions {
        segment,
        outputs,
        script,
        role,
    } = options;
    let split = !segment.is_full() || matches!(role, Role::Manager(_));
    for (name, scenario) in &plan.options.scenarios {
        if split && matches!(scenario.executor, Executor::ExternallyControlled { .. }) {
            return Err(EngineError::Unsplittable {
                scenario: name.clone(),
            });
        }
    }
    let http = &plan.options.http;
    let network = network(http)?;
    let base_client = Client::new(ClientConfig {
        max_redirects: http.max_redirects,
        insecure_skip_tls_verify: http.insecure_skip_tls_verify,
        discard_body: http.discard_response_bodies,
        user_agent: (!http.user_agent.is_empty()).then(|| http.user_agent.clone()),
        http_version: match http.http_version {
            HttpVersion::Auto => k9_http::HttpVersion::Auto,
            HttpVersion::Http1 => k9_http::HttpVersion::Http1,
            HttpVersion::Http2 => k9_http::HttpVersion::Http2,
            HttpVersion::Http3 => k9_http::HttpVersion::Http3,
        },
        client_certs: client_certs(&http.tls_auth).map_err(EngineError::Client)?,
        tls_versions: http
            .tls_version
            .as_ref()
            .map(|v| v.range().map_err(k9_http::HttpError::Config))
            .transpose()
            .map_err(EngineError::Client)?,
        cipher_suites: http.tls_cipher_suites.clone(),
        hosts: network.hosts,
        dns: network.dns,
        block_hostnames: network.block_hostnames,
        blocked_ips: network.blacklist_ips,
        no_connection_reuse: http.no_connection_reuse,
        debug: http.http_debug.as_deref().and_then(|d| d.parse().ok()),
        ..ClientConfig::default()
    })
    .map_err(EngineError::Client)?;
    let protos = grpc::load_protos(plan, &base_client).await?;
    let flows = compile(plan, &segment, &protos)?;
    let (mut registry, b) = Registry::with_builtins();
    for decl in script.iter().flat_map(|s| s.metrics()) {
        registry.register(&decl.name, decl.kind, decl.value_type)?;
    }
    let thresholds = if matches!(role, Role::Worker(_)) {
        // The manager judges thresholds on everyone's samples.
        let mut without = plan.clone();
        without.options.thresholds.clear();
        Thresholds::compile(&without, &registry)?
    } else {
        Thresholds::compile(plan, &registry)?
    };
    let thresholds = Arc::new(thresholds);
    let metrics = Arc::new(if outputs.is_empty() {
        Metrics::new(registry)
    } else {
        Metrics::with_samples(registry)
    });
    let shared = Arc::new(Shared {
        metrics: metrics.clone(),
        b,
        flows,
        script_errors: Mutex::new(HashMap::new()),
        active_vus: AtomicU32::new(0),
        peak_vus: AtomicU32::new(0),
        complete: AtomicU64::new(0),
        interrupted: AtomicU64::new(0),
        errors: Mutex::new(HashMap::new()),
        events: control.events.clone(),
        live: control.live.clone(),
    });
    let max_vus: u32 = plan
        .options
        .scenarios
        .values()
        .map(|s| segment.share(u64::from(s.executor.max_vus())) as u32)
        .sum();

    control.live.configure(plan, max_vus);
    control.live.start(&control.stop).await;
    let started = Instant::now();
    let finished = CancellationToken::new();
    // Stops every scenario; cancelled by the user's stop or by a threshold abort.
    let run_stop = control.stop.child_token();
    let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (updates_tx, updates_rx) = mpsc::unbounded_channel();
    let (mode, remote) = match role {
        Role::Standalone => (TickerMode::Standalone, None),
        Role::Worker(ticks) => (TickerMode::Worker(ticks), None),
        Role::Manager(remote) => (TickerMode::Manager(updates_rx), Some((remote, updates_tx))),
    };
    let hooks = !matches!(mode, TickerMode::Worker(_));
    let ticker = tokio::spawn(ticker(
        shared.clone(),
        control.progress.clone(),
        started,
        max_vus,
        finished.clone(),
        ThresholdWatch {
            thresholds: thresholds.clone(),
            run_stop: run_stop.clone(),
            aborted: aborted.clone(),
        },
        Outputs::new(outputs),
        mode,
        control.events.clone(),
        control.metrics.clone(),
        control.frames.clone(),
    ));

    let driven = drive(
        plan,
        &segment,
        &shared,
        script,
        hooks,
        remote,
        &base_client,
        &run_stop,
    )
    .await;
    control.live.finish();
    finished.cancel();
    let (aggregator, output_errors) = ticker.await.expect("the ticker task doesn't panic");
    let (teardown_error, outcome) = driven.map_err(EngineError::Setup)?;
    let outcome = outcome.unwrap_or_default();
    let duration = started.elapsed();
    let outcomes = thresholds.evaluate(aggregator.totals(), metrics.interner(), duration);

    Ok(RunResult {
        aggregator,
        builtins: b,
        duration,
        max_vus,
        iterations_complete: shared.complete.load(Ordering::Relaxed) + outcome.iterations_complete,
        iterations_interrupted: shared.interrupted.load(Ordering::Relaxed)
            + outcome.iterations_interrupted,
        request_errors: combined(by_frequency(&shared.errors), outcome.request_errors),
        script_errors: combined(by_frequency(&shared.script_errors), outcome.script_errors),
        teardown_error,
        stopped: control.is_stopped(),
        thresholds: outcomes,
        aborted_by_threshold: aborted.load(Ordering::Relaxed),
        output_errors,
        remote_failure: outcome.failure,
    })
}

/// Error counts from two places, summed by message, most frequent first.
fn combined(a: Vec<(String, u64)>, b: Vec<(String, u64)>) -> Vec<(String, u64)> {
    if b.is_empty() {
        return a;
    }
    let mut counts: HashMap<String, u64> = HashMap::new();
    for (message, n) in a.into_iter().chain(b) {
        *counts.entry(message).or_default() += n;
    }
    by_frequency(&Mutex::new(counts))
}

type Driven = (Option<HookError>, Option<RemoteOutcome>);

/// Runs the script's `setup()` (if `hooks`), the scenarios (here, or in the
/// remote), then `teardown()`. Returns the setup error if setup failed
/// (nothing else runs), else the teardown error and the remote's outcome.
#[allow(clippy::too_many_arguments)]
async fn drive(
    plan: &Plan,
    segment: &Segment,
    shared: &Arc<Shared>,
    script: Option<Arc<dyn Script>>,
    hooks: bool,
    remote: Option<(Arc<dyn Remote>, mpsc::UnboundedSender<RemoteUpdate>)>,
    base_client: &Client,
    run_stop: &CancellationToken,
) -> Result<Driven, HookError> {
    if run_stop.is_cancelled() {
        // Stopped while paused before the start: nothing runs.
        return Ok((None, None));
    }
    let hook_script = if hooks { script.clone() } else { None };
    let options = &plan.options;
    let plan_tags: Vec<(String, String)> = options
        .tags
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    // Hooks get their own client, outside any scenario (as in k6).
    let hook_ctx = |root| {
        ScriptContext::new(
            shared.clone(),
            plan_tags.clone(),
            base_client.with_separate_pool(),
            root,
        )
    };
    if let Some(script) = &hook_script {
        let setup = script.setup(hook_ctx("::setup"));
        tokio::select! {
            outcome = script::run_hook("setup", options.setup_timeout, setup) => outcome?,
            // Stopped during setup: no scenarios, no teardown.
            _ = run_stop.cancelled() => return Ok((None, None)),
        }
    }

    if let Some((remote, updates)) = remote {
        let data = match &script {
            Some(script) => {
                let script = script.clone();
                tokio::task::spawn_blocking(move || script.setup_data())
                    .await
                    .unwrap_or(None)
            }
            None => None,
        };
        let outcome = remote.run(data, run_stop.clone(), updates).await;
        return Ok((
            teardown(hook_script, options, hook_ctx).await,
            Some(outcome),
        ));
    }

    let mut scenarios = JoinSet::new();
    // VU ids follow global slots: each scenario's slots start after the
    // previous scenarios' (so ids are unique across scenarios and segments).
    let mut first_id = 1;
    for (name, scenario) in &options.scenarios {
        let ctx = scenario_ctx(plan, name, scenario, shared);
        scenarios.spawn(run_scenario(
            shared.clone(),
            script.clone(),
            ctx,
            scenario.clone(),
            Slots {
                segment: segment.clone(),
                first_id,
            },
            base_client.clone(),
            run_stop.clone(),
        ));
        first_id += u64::from(scenario.executor.max_vus());
    }
    join_all(&mut scenarios).await;

    Ok((teardown(hook_script, options, hook_ctx).await, None))
}

/// Runs `teardown()`; like k6, also after a stop or a threshold abort.
async fn teardown(
    script: Option<Arc<dyn Script>>,
    options: &crate::plan::Options,
    hook_ctx: impl Fn(&'static str) -> ScriptContext,
) -> Option<HookError> {
    let script = script?;
    let teardown = script.teardown(hook_ctx("::teardown"));
    script::run_hook("teardown", options.teardown_timeout, teardown)
        .await
        .err()
}

/// Drains error counts, most frequent first.
fn by_frequency(errors: &Mutex<HashMap<String, u64>>) -> Vec<(String, u64)> {
    let mut list: Vec<(String, u64)> = errors
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .drain()
        .collect();
    list.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    list
}

fn scenario_ctx(plan: &Plan, name: &str, scenario: &Scenario, shared: &Shared) -> Arc<ScenarioCtx> {
    let mut tags: Vec<(String, String)> = plan
        .options
        .tags
        .iter()
        .chain(&scenario.tags)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    tags.push(("scenario".to_owned(), name.to_owned()));
    let mut iteration: Vec<(&str, &str)> =
        tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    iteration.push(("group", ""));
    let iteration_tags = shared.intern(&iteration);
    let transactions = scenario
        .transactions
        .iter()
        .map(|(t, weight)| {
            let mut tags = iteration.clone();
            tags.push(("transaction", t));
            Transaction {
                name: t.clone(),
                weight: *weight,
                iteration_tags: shared.intern(&tags),
            }
        })
        .collect();
    Arc::new(ScenarioCtx {
        name: name.to_owned(),
        env: scenario.env.clone(),
        flow: scenario.exec.clone(),
        tags,
        iteration_tags,
        transactions,
        on_start: scenario.on_start.clone(),
        on_stop: scenario.on_stop.clone(),
    })
}

/// Runs one scenario: waits for its start time, runs its VUs until the executor
/// is done, then gives in-flight iterations `gracefulStop` to finish.
async fn run_scenario(
    shared: Arc<Shared>,
    script: Option<Arc<dyn Script>>,
    ctx: Arc<ScenarioCtx>,
    scenario: Scenario,
    slots: Slots,
    base_client: Client,
    stop: CancellationToken,
) {
    tokio::select! {
        _ = tokio::time::sleep(scenario.start_time.as_std()) => {}
        _ = stop.cancelled() => return,
    }
    let segment = slots.segment.clone();
    let mut vus = VuSpawner {
        shared,
        script,
        slots,
        omitted: Arc::new(arrival::Omitted::default()),
        ctx,
        base_client,
        graceful: CancellationToken::new(),
        hard: stop.child_token(),
        tasks: JoinSet::new(),
    };

    let arrivals = match &scenario.executor {
        Executor::ConstantArrivalRate {
            rate,
            time_unit,
            duration,
            pre_allocated_vus,
            max_vus,
        } => Some((
            arrival::Arrivals::constant(*rate, time_unit.as_std(), duration.as_std()),
            duration.as_std(),
            *pre_allocated_vus,
            *max_vus,
        )),
        Executor::RampingArrivalRate {
            start_rate,
            time_unit,
            stages,
            pre_allocated_vus,
            max_vus,
        } => Some((
            arrival::Arrivals::ramping(*start_rate, time_unit.as_std(), stages),
            stages.iter().map(|s| s.duration.as_std()).sum(),
            *pre_allocated_vus,
            *max_vus,
        )),
        _ => None,
    };
    if let Some((times, total, pre, max)) = arrivals {
        run_arrivals(&mut vus, times, total, pre, max, &stop).await;
        vus.graceful.cancel();
        tokio::select! {
            _ = join_all(&mut vus.tasks) => {}
            _ = tokio::time::sleep(scenario.graceful_stop.as_std()) => {
                vus.hard.cancel();
                join_all(&mut vus.tasks).await;
            }
        }
        // Dropped iterations no VU came free for: due, still waiting at the end.
        let end = std::time::Instant::now();
        while let Some(due) = vus.omitted.pop() {
            vus.shared.metrics.record_duration(
                vus.shared.b.iteration_duration_corrected,
                vus.ctx.iteration_tags,
                end.saturating_duration_since(due),
            );
        }
        return;
    }

    if let Executor::ExternallyControlled { duration, .. } = &scenario.executor {
        let live = vus.shared.live.clone();
        externally_controlled(&mut vus, &live, duration.as_std(), &stop).await;
        // No graceful stop (as in k6): in-flight iterations are interrupted.
        vus.hard.cancel();
        join_all(&mut vus.tasks).await;
        return;
    }

    if let Executor::RampingVus {
        start_vus,
        stages,
        graceful_ramp_down,
    } = &scenario.executor
    {
        let total = stages.iter().map(|s| s.duration.as_std()).sum();
        // The global schedule, keeping this segment's share at each step.
        let steps = ramp::steps(*start_vus, stages)
            .into_iter()
            .map(|(at, vus)| (at, segment.share(u64::from(vus)) as u32))
            .collect();
        ramp_vus(&mut vus, steps, total, graceful_ramp_down.as_std(), &stop).await;
    } else {
        let (count, budget, run_for) = match &scenario.executor {
            Executor::ConstantVus { vus, duration } => (*vus, Budget::Unlimited, *duration),
            Executor::SharedIterations {
                vus,
                iterations,
                max_duration,
            } => {
                let mine = segment.share_per_slot(*iterations, u64::from(*vus));
                (
                    *vus,
                    Budget::Shared(Arc::new(AtomicU64::new(mine))),
                    *max_duration,
                )
            }
            Executor::PerVuIterations {
                vus,
                iterations,
                max_duration,
            } => (*vus, Budget::PerVu(*iterations), *max_duration),
            _ => unreachable!("rejected before the run starts"),
        };
        for local in 0..segment.share(u64::from(count)) {
            vus.spawn(local, budget.clone());
        }
        tokio::select! {
            _ = join_all(&mut vus.tasks) => return,
            _ = tokio::time::sleep(run_for.as_std()) => {}
            _ = stop.cancelled() => {}
        }
    }

    // The executor is done: no new iterations; in-flight ones get gracefulStop.
    vus.graceful.cancel();
    tokio::select! {
        _ = join_all(&mut vus.tasks) => return,
        _ = tokio::time::sleep(scenario.graceful_stop.as_std()) => {}
    }
    vus.hard.cancel();
    join_all(&mut vus.tasks).await;
}

/// Starts VUs for one scenario. Each VU gets its own stop tokens, children of
/// the scenario's, so it can be retired on its own (ramp-down).
/// Which VU slots of a scenario this engine runs, and the id of slot 0.
struct Slots {
    segment: Segment,
    first_id: u64,
}

struct VuSpawner {
    shared: Arc<Shared>,
    slots: Slots,
    /// Arrival-rate iterations dropped for lack of a VU.
    omitted: Arc<arrival::Omitted>,
    /// Not in [`Shared`]: script code keeps [`ScriptContext`]s (which hold
    /// `Shared`) in objects the script owns, and that cycle would never be freed.
    script: Option<Arc<dyn Script>>,
    ctx: Arc<ScenarioCtx>,
    base_client: Client,
    graceful: CancellationToken,
    hard: CancellationToken,
    tasks: JoinSet<()>,
}

/// Tokens for retiring one VU: `graceful` lets it finish its iteration,
/// `hard` interrupts it.
struct VuHandle {
    graceful: CancellationToken,
    hard: CancellationToken,
}

impl VuSpawner {
    /// Starts this segment's `local`-th VU of the scenario.
    fn spawn(&mut self, local: u64, budget: Budget) -> VuHandle {
        self.spawn_with(local, budget, None)
    }

    /// Starts this segment's `local`-th VU; with `jobs`, it runs the
    /// iterations it's handed instead of claiming from `budget`.
    fn spawn_with(&mut self, local: u64, budget: Budget, jobs: Option<Jobs>) -> VuHandle {
        let handle = VuHandle {
            graceful: self.graceful.child_token(),
            hard: self.hard.child_token(),
        };
        let id = self.slots.first_id + self.slots.segment.slot(local);
        let mut client = self.base_client.with_separate_pool();
        let local_ips = &self.shared.flows.local_ips;
        if !local_ips.is_empty() {
            client = client.with_local_ip(local_ips.nth(id - 1));
        }
        let script = self.script.as_ref().map(|script| {
            let info = VuInfo {
                id,
                scenario: self.ctx.name.clone(),
                exec: self.ctx.flow.clone(),
                env: self.ctx.env.clone(),
            };
            // Shares the VU's client: same connections, same cookie jar.
            let ctx = ScriptContext::new(
                self.shared.clone(),
                self.ctx.tags.clone(),
                client.clone(),
                "",
            );
            script.vu(info, ctx)
        });
        let vu = Vu::new(id, self.shared.clone(), self.ctx.clone(), client, script);
        self.tasks.spawn(vu_loop(
            vu,
            budget,
            jobs,
            self.shared.clone(),
            handle.graceful.clone(),
            handle.hard.clone(),
        ));
        handle
    }
}

/// An arrival-rate VU's work: due times to run iterations at, and where to
/// say it's free again.
struct Jobs {
    due: mpsc::UnboundedReceiver<std::time::Instant>,
    free: mpsc::UnboundedSender<usize>,
    index: usize,
    /// Say it's free once `vu_init` is done (pre-allocated VUs); a VU started
    /// for a waiting iteration already has that job.
    free_after_init: bool,
    omitted: Arc<arrival::Omitted>,
}

/// Runs an arrival-rate schedule until `total`: each due iteration goes to a
/// free VU, or a new one (up to `max`), or is dropped. Iterations and VUs are
/// split among segments by slot, like every other executor.
async fn run_arrivals(
    vus: &mut VuSpawner,
    times: arrival::Arrivals,
    total: Duration,
    pre: u32,
    max: u32,
    stop: &CancellationToken,
) {
    let segment = vus.slots.segment.clone();
    // A part that runs any iterations gets at least one VU for them.
    let mine = |n: u32| segment.share(u64::from(n)).max(1) as usize;
    let max = mine(max);
    let pre = mine(pre).min(max);
    let (free_tx, mut free_rx) = mpsc::unbounded_channel::<usize>();
    let mut senders: Vec<mpsc::UnboundedSender<std::time::Instant>> = Vec::new();
    let add_vu = |vus: &mut VuSpawner, senders: &mut Vec<_>, free_after_init: bool| {
        let (tx, rx) = mpsc::unbounded_channel();
        let index = senders.len();
        senders.push(tx);
        let jobs = Jobs {
            due: rx,
            free: free_tx.clone(),
            index,
            free_after_init,
            omitted: vus.omitted.clone(),
        };
        vus.spawn_with(index as u64, Budget::Unlimited, Some(jobs));
        index
    };
    for _ in 0..pre {
        add_vu(vus, &mut senders, true);
    }
    let start = tokio::time::Instant::now();
    let mut free: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
    for (i, at) in times.enumerate() {
        if at >= total {
            break;
        }
        if !segment.owns(i as u64) {
            continue;
        }
        tokio::select! {
            _ = tokio::time::sleep_until(start + at) => {}
            _ = stop.cancelled() => return,
        }
        let due = start.into_std() + at;
        while let Ok(index) = free_rx.try_recv() {
            free.push_back(index);
        }
        let vu = match free.pop_front() {
            Some(index) => Some(index),
            None if senders.len() < max => Some(add_vu(vus, &mut senders, false)),
            None => None,
        };
        match vu {
            Some(index) => {
                let _ = senders[index].send(due);
            }
            None => {
                vus.shared.metrics.record(
                    vus.shared.b.dropped_iterations,
                    vus.ctx.iteration_tags,
                    1.0,
                );
                vus.omitted.push(due);
            }
        }
    }
    tokio::select! {
        _ = tokio::time::sleep_until(start + total) => {}
        _ = stop.cancelled() => {}
    }
    // Closing the job channels lets idle VUs finish.
    drop(senders);
}

/// Runs the `ramping-vus` schedule until its last stage ends. Retired VUs
/// finish their current iteration, or are interrupted after `graceful_ramp_down`.
async fn ramp_vus(
    vus: &mut VuSpawner,
    steps: Vec<(Duration, u32)>,
    total: Duration,
    graceful_ramp_down: Duration,
    stop: &CancellationToken,
) {
    let start = tokio::time::Instant::now();
    let mut active: Vec<VuHandle> = Vec::new();
    for (offset, target) in steps {
        tokio::select! {
            _ = tokio::time::sleep_until(start + offset) => {}
            _ = stop.cancelled() => return,
        }
        while active.len() < target as usize {
            let local = active.len() as u64;
            active.push(vus.spawn(local, Budget::Unlimited));
        }
        while active.len() > target as usize {
            let retired = active.pop().expect("more active VUs than the target");
            retired.graceful.cancel();
            tokio::spawn(async move {
                tokio::select! {
                    _ = tokio::time::sleep(graceful_ramp_down) => retired.hard.cancel(),
                    _ = retired.hard.cancelled() => {}
                }
            });
        }
    }
    tokio::select! {
        _ = tokio::time::sleep_until(start + total) => {}
        _ = stop.cancelled() => {}
    }
}

/// Runs as many VUs as [`RunControl::scale`] says for `duration`, not
/// counting time spent paused. Retired VUs finish their current iteration.
async fn externally_controlled(
    vus: &mut VuSpawner,
    live: &Live,
    duration: Duration,
    stop: &CancellationToken,
) {
    let mut target = live.vus();
    let mut paused = live.paused();
    let mut active: Vec<VuHandle> = Vec::new();
    let mut left = duration;
    loop {
        let want = *target.borrow_and_update() as usize;
        while active.len() < want {
            let local = active.len() as u64;
            active.push(vus.spawn(local, Budget::Unlimited));
        }
        while active.len() > want {
            let retired = active.pop().expect("more active VUs than the target");
            retired.graceful.cancel();
        }
        let is_paused = *paused.borrow_and_update();
        let since = tokio::time::Instant::now();
        tokio::select! {
            _ = tokio::time::sleep(left), if !is_paused => return,
            _ = target.changed() => {}
            _ = paused.changed() => {}
            _ = stop.cancelled() => return,
        }
        if !is_paused {
            left = left.saturating_sub(since.elapsed());
        }
    }
}

async fn vu_loop(
    mut vu: Vu,
    mut budget: Budget,
    mut jobs: Option<Jobs>,
    shared: Arc<Shared>,
    graceful: CancellationToken,
    hard: CancellationToken,
) {
    let active = shared.active_vus.fetch_add(1, Ordering::Relaxed) + 1;
    shared.peak_vus.fetch_max(active, Ordering::Relaxed);
    let ready = tokio::select! {
        ready = vu.init() => ready,
        _ = hard.cancelled() => false,
    };
    if let Some(jobs) = &jobs
        && ready
        && jobs.free_after_init
    {
        let _ = jobs.free.send(jobs.index);
    }
    while ready && !hard.is_cancelled() {
        if shared.live.is_paused() {
            let mut paused = shared.live.paused();
            tokio::select! {
                _ = paused.wait_for(|p| !*p) => {}
                _ = graceful.cancelled() => {}
                _ = hard.cancelled() => break,
            }
        }
        let due = match &mut jobs {
            // Arrival-rate VUs run what they're handed until the schedule ends
            // (then their channel closes); in-flight ones finish as usual.
            Some(jobs) => {
                let next = tokio::select! {
                    next = jobs.due.recv() => next,
                    _ = hard.cancelled() => None,
                };
                match next {
                    Some(due) => Some((due, jobs.omitted.clone())),
                    None => break,
                }
            }
            None if !graceful.is_cancelled() && budget.claim() => None,
            None => break,
        };
        tokio::select! {
            biased;
            _ = hard.cancelled() => {
                shared.interrupted.fetch_add(1, Ordering::Relaxed);
                break;
            }
            _ = vu.iteration(due.as_ref().map(|(at, omitted)| (*at, &**omitted))) => {
                shared.complete.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Some(jobs) = &jobs {
            let _ = jobs.free.send(jobs.index);
        }
        // An iteration that never awaits (e.g. an empty flow) must not starve
        // the timers and other tasks on this thread.
        tokio::task::yield_now().await;
    }
    if ready && !hard.is_cancelled() {
        tokio::select! {
            biased;
            _ = hard.cancelled() => {}
            _ = vu.stop() => {}
        }
    }
    shared.active_vus.fetch_sub(1, Ordering::Relaxed);
}

async fn join_all(tasks: &mut JoinSet<()>) {
    while let Some(joined) = tasks.join_next().await {
        if let Err(e) = joined
            && e.is_panic()
        {
            std::panic::resume_unwind(e.into_panic());
        }
    }
}

/// Publishes progress every 250 ms and aggregates metrics every second, until
/// the run finishes; then does a final aggregation and returns the aggregator.
///
/// `vus` is sampled on each tick while VUs are active, so it never records the
/// zero left after the run (periods with no active VUs aren't sampled either).
/// A run too short for any tick to see a VU records its peak once at the end.
/// What the ticker needs to enforce `abortOnFail`.
struct ThresholdWatch {
    thresholds: Arc<Thresholds>,
    run_stop: CancellationToken,
    aborted: Arc<std::sync::atomic::AtomicBool>,
}

/// What the ticker does besides aggregating locally.
enum TickerMode {
    Standalone,
    /// Sends progress every tick and metrics every aggregation to the manager.
    Worker(mpsc::UnboundedSender<WorkerTick>),
    /// Absorbs the workers' metrics and adds their progress to its own.
    Manager(mpsc::UnboundedReceiver<RemoteUpdate>),
}

/// The manager's view of its workers.
#[derive(Default)]
struct Workers {
    progress: HashMap<usize, WorkerProgress>,
    /// Snapshots absorbed so far; orders gauges across workers.
    snapshots: u64,
    /// Errors already announced (script errors by message, request errors).
    announced: HashSet<(bool, String)>,
    problems: Vec<String>,
    /// Raw samples' decoders, by worker (for the manager's outputs).
    decoders: HashMap<usize, k9_metrics::SampleDecoder>,
}

impl Workers {
    fn total(&self) -> WorkerProgress {
        let mut total = WorkerProgress::default();
        for p in self.progress.values() {
            total.active_vus += p.active_vus;
            total.peak_vus += p.peak_vus;
            total.iterations_complete += p.iterations_complete;
            total.iterations_interrupted += p.iterations_interrupted;
        }
        total
    }

    fn apply(
        &mut self,
        update: RemoteUpdate,
        aggregator: &mut Aggregator,
        metrics: &Metrics,
        events: &broadcast::Sender<RunEvent>,
        outputs: &mut Outputs,
    ) {
        match update {
            RemoteUpdate::Samples { worker, samples } => {
                let decoder = self.decoders.entry(worker).or_default();
                outputs.feed(
                    decoder.decode(samples, metrics.registry(), metrics.interner()),
                    metrics,
                );
            }
            RemoteUpdate::Tick {
                worker,
                metrics: snapshot,
                progress,
            } => {
                self.progress.insert(worker, progress);
                if snapshot.series.is_empty() {
                    return;
                }
                self.snapshots += 1;
                match snapshot.into_snapshot(metrics.registry(), metrics.interner(), self.snapshots)
                {
                    Ok(snapshot) => aggregator.absorb(snapshot),
                    Err(e) => self.problems.push(format!("worker {worker}: {e}")),
                }
            }
            RemoteUpdate::Event(event) => {
                let key = match &event {
                    RunEvent::ScriptError(err) => (true, err.message.clone()),
                    RunEvent::RequestError(message) => (false, message.clone()),
                };
                if self.announced.insert(key) {
                    let _ = events.send(event);
                }
            }
        }
    }
}

/// Metrics a worker leaves out: the manager records VU counts itself.
const MANAGER_METRICS: [&str; 2] = ["vus", "vus_max"];

fn without_vus(mut snapshot: PortableSnapshot) -> PortableSnapshot {
    snapshot
        .series
        .retain(|s| !MANAGER_METRICS.contains(&s.metric.as_str()));
    snapshot
}

#[allow(clippy::too_many_arguments)]
async fn ticker(
    shared: Arc<Shared>,
    progress: watch::Sender<Progress>,
    started: Instant,
    max_vus: u32,
    finished: CancellationToken,
    watch: ThresholdWatch,
    mut outputs: Outputs,
    mut mode: TickerMode,
    events: broadcast::Sender<RunEvent>,
    live_metrics: watch::Sender<Arc<Vec<LiveMetric>>>,
    frames: watch::Sender<Arc<LiveFrame>>,
) -> (Aggregator, Vec<String>) {
    let publish_metrics = |aggregator: &Aggregator| {
        if live_metrics.receiver_count() > 0 {
            live_metrics.send_replace(Arc::new(live::live_metrics(
                aggregator.totals(),
                &shared.metrics,
                started.elapsed(),
            )));
        }
    };
    let metrics = &shared.metrics;
    let b = &shared.b;
    let mut aggregator = Aggregator::new(metrics.clone());
    // First tick one interval in, not at t=0 while VUs are still starting.
    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now() + PROGRESS_INTERVAL,
        PROGRESS_INTERVAL,
    );
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ticks = 0u32;
    let mut sampled_vus = false;
    let mut workers = Workers::default();
    let mut peak = 0;
    // This engine's own progress plus its workers'.
    let current = |workers: &Workers| {
        let remote = workers.total();
        WorkerProgress {
            active_vus: shared.active_vus.load(Ordering::Relaxed) + remote.active_vus,
            peak_vus: shared.peak_vus.load(Ordering::Relaxed) + remote.peak_vus,
            iterations_complete: shared.complete.load(Ordering::Relaxed)
                + remote.iterations_complete,
            iterations_interrupted: shared.interrupted.load(Ordering::Relaxed)
                + remote.iterations_interrupted,
        }
    };
    let publish = |p: WorkerProgress| {
        progress.send_replace(Progress {
            elapsed: started.elapsed(),
            active_vus: p.active_vus,
            max_vus,
            iterations_complete: p.iterations_complete,
            iterations_interrupted: p.iterations_interrupted,
        });
    };
    metrics.record(b.vus_max, TagSetId::EMPTY, f64::from(max_vus));
    loop {
        let update = async {
            match &mut mode {
                TickerMode::Manager(updates) => updates.recv().await,
                _ => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = interval.tick() => {
                let now = current(&workers);
                publish(now);
                peak = peak.max(now.active_vus);
                if now.active_vus > 0 {
                    metrics.record(b.vus, TagSetId::EMPTY, f64::from(now.active_vus));
                    sampled_vus = true;
                }
                ticks += 1;
                let aggregate = ticks.is_multiple_of(TICKS_PER_AGGREGATION);
                if let TickerMode::Worker(to_manager) = &mode {
                    let snapshot = if aggregate {
                        let delta = aggregator.tick_with_delta(started.elapsed());
                        without_vus(delta.to_portable(metrics.registry(), metrics.interner()))
                    } else {
                        PortableSnapshot { series: Vec::new() }
                    };
                    let _ = to_manager.send(WorkerTick { metrics: snapshot, progress: now });
                    // A worker's outputs forward its samples to the manager.
                    if aggregate {
                        outputs.feed(aggregator.drain_samples(), metrics);
                    }
                    continue;
                }
                if aggregate {
                    metrics.record(b.vus_max, TagSetId::EMPTY, f64::from(max_vus));
                    let elapsed = started.elapsed();
                    aggregator.tick(elapsed);
                    outputs.feed(aggregator.drain_samples(), metrics);
                    publish_metrics(&aggregator);
                    let mut outcomes = Vec::new();
                    if !watch.thresholds.is_empty() {
                        outcomes =
                            watch.thresholds.evaluate(aggregator.totals(), metrics.interner(), elapsed);
                        if watch.thresholds.should_abort(&outcomes, elapsed) {
                            watch.aborted.store(true, Ordering::Relaxed);
                            watch.run_stop.cancel();
                        }
                    }
                    if frames.receiver_count() > 0 {
                        frames.send_replace(Arc::new(live::live_frame(
                            aggregator.ticks().last(),
                            aggregator.totals(),
                            metrics,
                            b.checks,
                            elapsed,
                            outcomes,
                        )));
                    }
                }
            }
            Some(update) = update => {
                workers.apply(update, &mut aggregator, metrics, &events, &mut outputs);
            }
            _ = finished.cancelled() => break,
        }
    }
    // The remote has finished: take in its last updates.
    if let TickerMode::Manager(updates) = &mut mode {
        while let Ok(update) = updates.try_recv() {
            workers.apply(update, &mut aggregator, metrics, &events, &mut outputs);
        }
    }
    let now = current(&workers);
    if !sampled_vus {
        // (Workers' peaks may not coincide; their sum is an upper bound.)
        let peak = peak.max(now.peak_vus);
        metrics.record(b.vus, TagSetId::EMPTY, f64::from(peak));
    }
    if let TickerMode::Worker(to_manager) = &mode {
        let delta = aggregator.tick_with_delta(started.elapsed());
        let snapshot = without_vus(delta.to_portable(metrics.registry(), metrics.interner()));
        let _ = to_manager.send(WorkerTick {
            metrics: snapshot,
            progress: now,
        });
        outputs.feed(aggregator.drain_samples(), metrics);
    } else {
        aggregator.tick(started.elapsed());
        outputs.feed(aggregator.drain_samples(), metrics);
        publish_metrics(&aggregator);
    }
    publish(now);
    let mut problems = outputs.stop(metrics);
    problems.append(&mut workers.problems);
    (aggregator, problems)
}

/// Outputs plus their errors; an output that fails once is dropped.
struct Outputs {
    live: Vec<Box<dyn Output>>,
    errors: Vec<String>,
}

impl Outputs {
    fn new(live: Vec<Box<dyn Output>>) -> Outputs {
        Outputs {
            live,
            errors: Vec::new(),
        }
    }

    fn feed(&mut self, samples: Vec<k9_metrics::Sample>, metrics: &Metrics) {
        if samples.is_empty() {
            return;
        }
        let errors = &mut self.errors;
        self.live
            .retain_mut(|output| match output.add_samples(&samples, metrics) {
                Ok(()) => true,
                Err(e) => {
                    errors.push(format!("{}: {e}", output.description()));
                    false
                }
            });
    }

    fn stop(mut self, metrics: &Metrics) -> Vec<String> {
        for output in &mut self.live {
            if let Err(e) = output.stop(metrics) {
                self.errors.push(format!("{}: {e}", output.description()));
            }
        }
        self.errors
    }
}
