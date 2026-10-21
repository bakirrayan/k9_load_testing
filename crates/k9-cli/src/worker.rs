//! `k9 worker`: a worker process, started by `k9 run --workers N`. It connects
//! to the manager, runs the execution segment it's assigned (without the
//! hooks, which the manager runs), and reports metrics and progress. `k9
//! agent` runs the same conversation ([`work`]) for managers on other machines.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use k9_core::engine::{self, Role, RunControl, RunOptions, Script, WorkerTick};
use k9_core::protocol::{
    AgentGroup, Assignment, FromWorker, PROTOCOL_VERSION, ToWorker, WorkerSummary,
};
use k9_grpc::ClientOptions;
use k9_metrics::{Metrics, PortableSnapshot, Sample, SampleEncoder};
use k9_output::Output;
use tokio::sync::mpsc;

use crate::exit_code;
use crate::script::ScriptHost;

pub(crate) fn run(
    connect: &str,
    token: &str,
    slot: u32,
    threads: Option<usize>,
    host: Option<&dyn ScriptHost>,
) -> i32 {
    let runtime = match runtime(threads) {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("k9 worker: could not start the async runtime: {err}");
            return exit_code::GENERIC_ENGINE;
        }
    };
    let options = ClientOptions::new(token);
    let served = runtime.block_on(async {
        // Ctrl-C in the terminal reaches every process; the manager decides.
        tokio::spawn(async { while tokio::signal::ctrl_c().await.is_ok() {} });
        work(connect, &options, host, slot, &AgentGroup::default()).await
    });
    match served {
        Ok(Served::Unreachable(message)) | Err(message) => {
            eprintln!("k9 worker (pid {}): {message}", std::process::id());
            exit_code::GENERIC_ENGINE
        }
        Ok(_) => exit_code::OK,
    }
}

pub(crate) fn runtime(threads: Option<usize>) -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(threads) = threads {
        builder.worker_threads(threads);
    }
    builder.enable_all().build()
}

/// How a conversation with a manager ended.
pub(crate) enum Served {
    /// It assigned work, and the run is over (whatever its outcome).
    Ran,
    /// It didn't need this worker (it had enough, or gave up before starting).
    TurnedAway,
    /// Nothing answered at its address (yet).
    Unreachable(String),
}

/// Connects to the manager at `address`, runs what it assigns, and reports.
/// `slot` is the worker's place among those `k9 run --workers` started (0 for
/// an agent's). `Err` is for what retrying won't fix: a refused token, TLS,
/// the protocol.
pub(crate) async fn work(
    address: &str,
    options: &ClientOptions,
    host: Option<&dyn ScriptHost>,
    slot: u32,
    group: &AgentGroup,
) -> Result<Served, String> {
    let (tx, mut rx) = match k9_grpc::connect(address, options).await {
        Ok(streams) => streams,
        Err(e @ k9_grpc::Error::Connect { .. }) => return Ok(Served::Unreachable(e.to_string())),
        Err(e) => return Err(e.to_string()),
    };
    let token = &options.token;
    let gone = || Ok(Served::Ran);
    let hello = FromWorker::Hello {
        protocol: PROTOCOL_VERSION,
        version: k9_core::VERSION.to_owned(),
        pid: std::process::id(),
        slot,
        token: token.to_owned(),
        group: group.clone(),
    };
    if tx.send(hello).await.is_err() {
        return gone();
    }

    let assignment = match rx.recv().await {
        Some(Ok(ToWorker::Assign(assignment))) => assignment,
        // The manager has enough workers, or gave up (e.g. another worker
        // failed to start).
        Some(Ok(ToWorker::Stop)) | None => return Ok(Served::TurnedAway),
        Some(Ok(other)) => return Err(format!("expected an assignment, got {other:?}")),
        Some(Err(e)) => return Err(e.to_string()),
    };
    let prepared = tokio::task::block_in_place(|| prepare(&assignment, host));
    // Keeps a shipped script's directory until the run is over.
    let (plan, script, _shipped) = match prepared {
        Ok(prepared) => prepared,
        Err(message) => {
            let _ = tx.send(FromWorker::Failed { message }).await;
            drop(tx);
            until_closed(&mut rx).await;
            return Ok(Served::Ran);
        }
    };
    if tx.send(FromWorker::Ready).await.is_err() {
        return gone();
    }
    match rx.recv().await {
        Some(Ok(ToWorker::Start)) => {}
        _ => return Ok(Served::Ran),
    }

    let control = RunControl::new();
    let started = Instant::now();
    // One sender; ticks, events and samples are forwarded to it in order.
    let (out, mut outbox) = mpsc::unbounded_channel::<FromWorker>();
    let sender = tokio::spawn(async move {
        while let Some(message) = outbox.recv().await {
            if tx.send(message).await.is_err() {
                break;
            }
        }
    });
    let (ticks_tx, mut ticks) = mpsc::unbounded_channel::<WorkerTick>();
    let ticks_out = out.clone();
    let forward_ticks = tokio::spawn(async move {
        while let Some(tick) = ticks.recv().await {
            let _ = ticks_out.send(FromWorker::Tick {
                elapsed_ms: started.elapsed().as_millis() as u64,
                metrics: tick.metrics,
                progress: tick.progress,
            });
        }
    });
    let mut events = control.events();
    let events_out = out.clone();
    let forward_events = tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            let _ = events_out.send(FromWorker::Event(event));
        }
    });
    // `Stop` from the manager, or the manager going away, interrupts the run;
    // the stream's end (after `Done`) lets the worker exit.
    let stopper = control.clone();
    let listen = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Some(Ok(ToWorker::Stop)) => stopper.stop(),
                Some(Ok(_)) => {}
                None | Some(Err(_)) => {
                    stopper.stop();
                    return;
                }
            }
        }
    });

    let mut outputs: Vec<Box<dyn Output>> = Vec::new();
    if assignment.forward_samples {
        outputs.push(Box::new(ForwardSamples {
            encoder: SampleEncoder::default(),
            out: out.clone(),
        }));
    }
    let options = RunOptions {
        segment: assignment.segment.clone(),
        outputs,
        script,
        role: Role::Worker(ticks_tx),
    };
    let result = engine::run_with(&plan, control, options).await;
    // The engine is done: its last tick is on its way; then report.
    let _ = forward_ticks.await;
    forward_events.abort();
    let last = match result {
        Ok(result) => FromWorker::Done(WorkerSummary {
            iterations_complete: result.iterations_complete,
            iterations_interrupted: result.iterations_interrupted,
            script_errors: result.script_errors,
            request_errors: result.request_errors,
            metrics: PortableSnapshot { series: Vec::new() },
        }),
        Err(e) => FromWorker::Failed {
            message: e.to_string(),
        },
    };
    let _ = out.send(last);
    drop(out);
    let _ = sender.await;
    // Exiting now could cut off what's still in flight: wait for the manager
    // to end the run's stream.
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, listen).await;
    Ok(Served::Ran)
}

/// How long a finished worker waits for the manager to close the stream.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(15);

/// Waits (a while) for the manager to end its stream.
async fn until_closed(rx: &mut k9_grpc::Inbound<ToWorker>) {
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, async {
        while let Some(Ok(_)) = rx.recv().await {}
    })
    .await;
}

/// Samples in a batch, at most (about 2 MB on the wire).
const SAMPLES_PER_MESSAGE: usize = 65_536;

/// The worker's only output: its samples, sent on to the manager's outputs.
/// Its messages share the connection's queue, so they arrive before `Done`.
struct ForwardSamples {
    encoder: SampleEncoder,
    out: mpsc::UnboundedSender<FromWorker>,
}

impl Output for ForwardSamples {
    fn description(&self) -> String {
        "the manager's outputs".to_owned()
    }

    fn add_samples(&mut self, samples: &[Sample], metrics: &Metrics) -> std::io::Result<()> {
        // The manager records `vus` and `vus_max` itself, for all workers.
        let registry = metrics.registry();
        let own: Vec<Sample> = samples
            .iter()
            .filter(|s| !matches!(registry.def(s.metric).name.as_str(), "vus" | "vus_max"))
            .copied()
            .collect();
        for chunk in own.chunks(SAMPLES_PER_MESSAGE) {
            let batch = self.encoder.encode(chunk, registry, metrics.interner());
            if self.out.send(FromWorker::Samples(batch)).is_err() {
                return Err(std::io::Error::other("the manager has gone"));
            }
        }
        Ok(())
    }

    fn stop(&mut self, _metrics: &Metrics) -> std::io::Result<()> {
        Ok(())
    }
}

/// A script the manager sent (agents), saved in a directory of its own that
/// goes away with it.
struct ShippedScript {
    dir: PathBuf,
    path: PathBuf,
}

impl ShippedScript {
    /// Saves the script, and its `modules` beside it.
    fn save(
        name: &str,
        source: &str,
        modules: &[(String, String)],
    ) -> Result<ShippedScript, String> {
        static RUNS: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "k9-agent-{}-{}",
            std::process::id(),
            RUNS.fetch_add(1, Ordering::Relaxed)
        ));
        let name = Path::new(name)
            .file_name()
            .map_or_else(|| "script.py".into(), |n| n.to_os_string());
        let path = dir.join(name);
        std::fs::create_dir_all(&dir)
            .and_then(|()| std::fs::write(&path, source))
            .map_err(|e| format!("could not save the script in {}: {e}", dir.display()))?;
        let shipped = ShippedScript { dir, path };
        for (relative, source) in modules {
            let path = module_path(&shipped.dir, relative)?;
            path.parent()
                .map_or(Ok(()), std::fs::create_dir_all)
                .and_then(|()| std::fs::write(&path, source))
                .map_err(|e| format!("could not save module {relative}: {e}"))?;
        }
        Ok(shipped)
    }
}

/// Where module `relative` goes under `dir`: a plain relative path only
/// (no `..`, no root), so the manager can't write anywhere else.
fn module_path(dir: &Path, relative: &str) -> Result<PathBuf, String> {
    use std::path::Component;
    let path = Path::new(relative);
    let plain =
        !relative.is_empty() && path.components().all(|c| matches!(c, Component::Normal(_)));
    if !plain {
        return Err(format!(
            "the manager sent module `{relative}`, which isn't a plain relative path"
        ));
    }
    Ok(dir.join(path))
}

impl Drop for ShippedScript {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

type Prepared = (
    k9_core::Plan,
    Option<Arc<dyn Script>>,
    Option<ShippedScript>,
);

/// The plan, and the script with the manager's `setup()` data.
fn prepare(assignment: &Assignment, host: Option<&dyn ScriptHost>) -> Result<Prepared, String> {
    let plan = assignment
        .plan()
        .map_err(|e| format!("the plan didn't arrive intact: {e}"))?;
    if assignment.script.is_empty() {
        return Ok((plan, None, None));
    }
    let host = host.ok_or("this k9 can't run Python scripts (pip install k9-load)")?;
    let shipped = match &assignment.script_source {
        Some(source) => Some(ShippedScript::save(
            &assignment.script,
            source,
            &assignment.modules,
        )?),
        None => None,
    };
    let path = shipped
        .as_ref()
        .map_or_else(|| PathBuf::from(&assignment.script), |s| s.path.clone());
    let loaded = host
        .load(&path, &assignment.env)
        .map_err(|e| format!("the script raised while loading:\n{e}"))?;
    if let Some(data) = &assignment.setup_data {
        loaded.script.set_setup_data(data.clone());
    }
    Ok((plan, Some(loaded.script), shipped))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modules_stay_in_the_scripts_directory() {
        let dir = Path::new("/tmp/k9-agent-x");
        assert_eq!(
            module_path(dir, "lib/checks.py").unwrap(),
            dir.join("lib/checks.py")
        );
        for bad in [
            "",
            "../escape.py",
            "/etc/passwd",
            "lib/../../x.py",
            "./a.py",
        ] {
            assert!(module_path(dir, bad).is_err(), "{bad}");
        }
    }
}
