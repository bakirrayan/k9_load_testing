//! `k9 run --workers N` and `--agents N`: the manager side. Starts N worker
//! processes (`k9 worker`, or `python -m k9 worker` from the wheel), which
//! connect back over gRPC on loopback with this run's token, or waits for N
//! `k9 agent`s on other machines; then runs the scenarios in them, one
//! execution segment each (see `k9_core::protocol` for the conversation, and
//! k9-grpc's `worker.proto` for the wire format). Agents get the script's text
//! and the plan's feeder rows, since they can't read this machine's files.
//!
//! Workers keep the terminal's stdout and stderr, so `print()` in scripts
//! still shows. They ignore Ctrl-C: the manager handles it and sends `Stop`.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

use k9_core::engine::{BoxFuture, Remote, RemoteOutcome, RemoteUpdate, RunControl};
use k9_core::protocol::{AgentGroup, Assignment, FromWorker, ProtocolError, ToWorker, check_hello};
use k9_core::{Plan, Segment};
use k9_grpc::{Connection, Listener, ServerOptions, Tls};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// How long local workers get to start (import the script) and say hello.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the manager waits for agents to connect.
const AGENTS_TIMEOUT: Duration = Duration::from_secs(600);
/// How long workers get to finish after `Stop` before they're killed.
const STOP_GRACE: Duration = Duration::from_secs(10);

/// How to start workers and what they run.
pub(crate) struct WorkerSetup {
    pub count: u32,
    pub launch: Launch,
    /// The script (absolute path), or empty for a declarative plan.
    pub script: String,
    pub env: Vec<(String, String)>,
    /// The manager has `--out` outputs: workers send their raw samples.
    pub forward_samples: bool,
}

/// Where the workers come from.
pub(crate) enum Launch {
    /// Processes on this machine, started with this program (and leading
    /// arguments), e.g. `python -m k9`; `worker …` is appended.
    Local(Vec<String>),
    /// `k9 agent`s, on other machines, that connect to the manager.
    Agents(AgentsSetup),
}

pub(crate) struct AgentsSetup {
    pub listen: String,
    /// Default: a new one for this run.
    pub token: Option<String>,
    /// (certificate, private key) PEM files, for TLS.
    pub tls: Option<(PathBuf, PathBuf)>,
    /// The script's local modules, relative to its directory.
    pub modules: Vec<String>,
}

/// Where agents connect: bound before the run starts, so the header can say.
pub(crate) struct AgentsListener {
    listener: Listener,
    token: String,
    /// E.g. `https://0.0.0.0:6570`.
    pub address: String,
    /// The token was made up for this run (so it has to be shown).
    pub generated_token: Option<String>,
    /// The script's text, for agents.
    script_source: Option<String>,
    /// Its local modules: (path relative to the script, source).
    modules: Vec<(String, String)>,
}

impl AgentsSetup {
    /// Reads what agents will need (they can't read this machine's files):
    /// `plan`'s feeder files, into its rows, and the `script`; then listens.
    pub async fn bind(&self, plan: &mut Plan, script: &str) -> Result<AgentsListener, String> {
        plan.inline_files()?;
        let script_source = if script.is_empty() {
            None
        } else {
            Some(std::fs::read_to_string(script).map_err(|e| format!("{script}: {e}"))?)
        };
        let dir = Path::new(script).parent().unwrap_or(Path::new("."));
        let modules = self
            .modules
            .iter()
            .map(|module| {
                let path = dir.join(module);
                std::fs::read_to_string(&path)
                    .map(|source| (module.clone(), source))
                    .map_err(|e| format!("{}: {e}", path.display()))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let (token, generated_token) = match &self.token {
            Some(token) if !token.is_empty() => (token.clone(), None),
            _ => {
                let token = token();
                (token.clone(), Some(token))
            }
        };
        let mut options = ServerOptions::new(token.clone());
        if let Some((cert, key)) = &self.tls {
            let read = |path: &PathBuf| {
                std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))
            };
            options.tls = Some(Tls {
                cert_pem: read(cert)?,
                key_pem: read(key)?,
            });
        }
        let listener = Listener::bind(&self.listen, &options)
            .await
            .map_err(|e| format!("could not listen for agents on {}: {e}", self.listen))?;
        let scheme = if self.tls.is_some() { "https://" } else { "" };
        let address = format!("{scheme}{}", listener.local_addr());
        Ok(AgentsListener {
            listener,
            token,
            address,
            generated_token,
            script_source,
            modules,
        })
    }
}

struct Worker {
    /// E.g. `worker 2 (pid 123)`, `agent 1 (10.0.0.5:51234, pid 99)`.
    label: String,
    /// Local workers only: the process this run started.
    child: Option<Child>,
    conn: Connection,
    /// Its share of the test relative to the other workers'.
    weight: u64,
}

pub(crate) struct WorkerPool {
    /// Keeps serving (and turning away extra workers) for as long as the run lasts.
    listening: tokio::task::JoinHandle<()>,
    workers: Mutex<Vec<Worker>>,
    plan: Plan,
    script: String,
    /// The script's text, for agents.
    script_source: Option<String>,
    /// Its local modules, for agents.
    modules: Vec<(String, String)>,
    env: Vec<(String, String)>,
    forward_samples: bool,
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.listening.abort();
    }
}

/// A secret for this run, so only the workers it started (or the agents
/// given it) can join: 128 bits from the OS's random hash keys.
pub(crate) fn token() -> String {
    let mut out = String::new();
    for _ in 0..2 {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default(),
        );
        hasher.write_u32(std::process::id());
        out.push_str(&format!("{:016x}", hasher.finish()));
    }
    out
}

/// A worker's hello: its pid and slot, if it's for this run.
async fn hello(
    conn: &mut Connection,
    token: &str,
) -> Result<Option<(u32, u32, AgentGroup)>, String> {
    let hello = tokio::time::timeout(Duration::from_secs(10), conn.rx.recv())
        .await
        .map_err(|_| "a worker connected but didn't say hello".to_owned())?
        .ok_or("a worker connected and hung up")?
        .map_err(|e| e.to_string())?;
    match check_hello(&hello, token) {
        Ok(pid) => match hello {
            FromWorker::Hello { slot, group, .. } => Ok(Some((pid, slot, group))),
            _ => Ok(Some((pid, 0, AgentGroup::default()))),
        },
        // Not one of ours: ignore it.
        Err(ProtocolError::Token) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

impl WorkerPool {
    /// Starts the workers (or waits for the agents) until each has said hello.
    /// `agents` is the listener for [`Launch::Agents`].
    pub async fn start(
        setup: WorkerSetup,
        plan: Plan,
        agents: Option<AgentsListener>,
        control: &RunControl,
    ) -> Result<WorkerPool, String> {
        let (mut script_source, mut modules) = (None, Vec::new());
        let (mut listener, workers) = match (&setup.launch, agents) {
            (Launch::Local(command), _) => Self::start_local(command, setup.count).await?,
            (Launch::Agents(_), Some(mut agents)) => {
                script_source = agents.script_source.take();
                modules = std::mem::take(&mut agents.modules);
                Self::wait_for_agents(agents, setup.count, control).await?
            }
            (Launch::Agents(_), None) => unreachable!("agents are bound before the run"),
        };
        // Workers that turn up now aren't needed: tell them to stop.
        let listening = tokio::spawn(async move {
            while let Some(conn) = listener.accept().await {
                let _ = conn.tx.send(ToWorker::Stop).await;
            }
        });
        Ok(WorkerPool {
            listening,
            workers: Mutex::new(workers),
            plan,
            script: setup.script,
            script_source,
            modules,
            env: setup.env,
            forward_samples: setup.forward_samples,
        })
    }

    async fn start_local(
        command: &[String],
        count: u32,
    ) -> Result<(Listener, Vec<Worker>), String> {
        let token = token();
        let mut listener = Listener::bind("127.0.0.1:0", &ServerOptions::new(token.clone()))
            .await
            .map_err(|e| format!("could not listen for workers: {e}"))?;
        let addr = listener.local_addr();
        let (program, leading) = command
            .split_first()
            .ok_or("no command to start workers with")?;
        // Each worker gets its share of the CPUs: a full-size runtime per
        // process only adds idle threads that spin on every wake-up.
        let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
        let threads = (cpus / count as usize).max(1).to_string();
        let mut children = Vec::new();
        for slot in 1..=count {
            let child = Command::new(program)
                .args(leading)
                .args(["worker", "--connect", &addr.to_string(), "--token", &token])
                .args(["--slot", &slot.to_string()])
                .args(["--threads", &threads])
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .map_err(|e| format!("could not start a worker ({program}): {e}"))?;
            children.push(child);
        }

        let mut connected = Vec::new();
        let accepting = async {
            while connected.len() < children.len() {
                let mut conn = listener
                    .accept()
                    .await
                    .ok_or("the worker listener stopped")?;
                if let Some((pid, slot, _)) = hello(&mut conn, &token).await? {
                    connected.push((slot, (pid, conn)));
                }
            }
            Ok::<(), String>(())
        };
        match tokio::time::timeout(CONNECT_TIMEOUT, accepting).await {
            Ok(Ok(())) => {}
            Ok(Err(message)) => return Err(format!("starting workers: {message}")),
            Err(_) => {
                for child in &mut children {
                    if let Ok(Some(status)) = child.try_wait() {
                        return Err(format!("a worker exited while starting ({status})"));
                    }
                }
                return Err(format!(
                    "workers didn't connect within {}s",
                    CONNECT_TIMEOUT.as_secs()
                ));
            }
        }

        // Pair connections with processes (the order they connected in is
        // arbitrary).
        let workers = pair_by_slot(children, connected)
            .map_err(|e| format!("starting workers: {e}"))?
            .into_iter()
            .enumerate()
            .map(|(i, (child, (pid, conn)))| Worker {
                label: format!("worker {i} (pid {pid})"),
                child: Some(child),
                conn,
                weight: 1,
            })
            .collect();
        Ok((listener, workers))
    }

    async fn wait_for_agents(
        agents: AgentsListener,
        count: u32,
        control: &RunControl,
    ) -> Result<(Listener, Vec<Worker>), String> {
        let AgentsListener {
            mut listener,
            token,
            ..
        } = agents;
        // By agent id (or one of their own for workers without one), in the
        // order they turned up.
        let mut agents: Vec<Joining> = Vec::new();
        let complete = |agents: &[Joining]| agents.iter().filter(|a| a.is_complete()).count();
        let accepting = async {
            while complete(&agents) < count as usize {
                let mut conn = listener
                    .accept()
                    .await
                    .ok_or("the agent listener stopped")?;
                let peer = conn.peer.map_or_else(|| "?".to_owned(), |p| p.to_string());
                match hello(&mut conn, &token).await {
                    Ok(Some((pid, _, group))) => {
                        let known = agents
                            .iter()
                            .position(|a| !group.agent.is_empty() && a.id == group.agent);
                        let i = match known {
                            Some(i) => i,
                            None if agents.len() < count as usize => {
                                agents.push(Joining {
                                    id: group.agent.clone(),
                                    peer: peer.clone(),
                                    workers: group.workers.max(1),
                                    weight: group.weight.max(1),
                                    conns: Vec::new(),
                                });
                                agents.len() - 1
                            }
                            // Every place is taken (maybe by agents still
                            // waiting for their workers).
                            None => {
                                let _ = conn.tx.send(ToWorker::Stop).await;
                                continue;
                            }
                        };
                        let agent = &mut agents[i];
                        let label = if agent.workers > 1 {
                            format!(
                                "agent {i} worker {}/{} ({peer}, pid {pid})",
                                agent.conns.len() + 1,
                                agent.workers
                            )
                        } else {
                            format!("agent {i} ({peer}, pid {pid})")
                        };
                        eprintln!("{label} connected");
                        agent.conns.push((label, conn));
                    }
                    Ok(None) => {}
                    // One bad agent doesn't stop the others from joining.
                    Err(e) => eprintln!("WARN an agent from {peer} was turned away: {e}"),
                }
            }
            Ok::<(), String>(())
        };
        let joined = |agents: Vec<Joining>| {
            // Workers of agents that didn't fill up in time aren't needed.
            let (agents, extra): (Vec<Joining>, Vec<Joining>) =
                agents.into_iter().partition(Joining::is_complete);
            for agent in extra {
                for (_, conn) in agent.conns {
                    tokio::spawn(async move {
                        let _ = conn.tx.send(ToWorker::Stop).await;
                    });
                }
            }
            shares(agents)
        };
        tokio::select! {
            accepted = tokio::time::timeout(AGENTS_TIMEOUT, accepting) => match accepted {
                Ok(Ok(())) => Ok((listener, joined(agents))),
                Ok(Err(message)) => Err(format!("waiting for agents: {message}")),
                Err(_) => Err(format!(
                    "only {} of {count} agents connected within {}s",
                    complete(&agents),
                    AGENTS_TIMEOUT.as_secs()
                )),
            },
            _ = control.stopped() => Err("stopped while waiting for agents".to_owned()),
        }
    }
}

/// An agent whose workers are connecting.
struct Joining {
    /// Empty for an agent with one worker and no id.
    id: String,
    peer: String,
    workers: u32,
    weight: u32,
    conns: Vec<(String, Connection)>,
}

impl Joining {
    fn is_complete(&self) -> bool {
        self.conns.len() >= self.workers as usize
    }
}

/// The agents' workers, each weighted so an agent's share of the test
/// follows its weight, split evenly among its workers; says who gets what.
fn shares(agents: Vec<Joining>) -> Vec<Worker> {
    let total: u64 = agents.iter().map(|a| u64::from(a.weight)).sum();
    // Every agent's worker count divides `per`, so shares stay whole numbers.
    let per = agents
        .iter()
        .fold(1, |acc, a| lcm(acc, u64::from(a.workers)));
    let mut workers = Vec::new();
    for (i, agent) in agents.into_iter().enumerate() {
        let (num, den) = reduce(u64::from(agent.weight), total);
        let share = if num == den {
            "all".to_owned()
        } else {
            format!("{num}/{den}")
        };
        let plural = if agent.workers == 1 { "" } else { "s" };
        eprintln!(
            "agent {i} ({}) joined with {} worker{plural}: {share} of the test",
            agent.peer, agent.workers
        );
        let weight = u64::from(agent.weight) * per / u64::from(agent.workers);
        for (label, conn) in agent.conns {
            workers.push(Worker {
                label,
                child: None,
                conn,
                weight,
            });
        }
    }
    workers
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 { a } else { gcd(b, a % b) }
}

fn lcm(a: u64, b: u64) -> u64 {
    a / gcd(a, b) * b
}

fn reduce(num: u64, den: u64) -> (u64, u64) {
    let g = gcd(num, den).max(1);
    (num / g, den / g)
}

/// What a worker's reader task reports.
enum Inbox {
    Message(usize, FromWorker),
    /// The connection ended (the process exited or crashed) or broke.
    Closed(usize, Option<String>),
}

impl Remote for WorkerPool {
    fn run(
        &self,
        setup_data: Option<String>,
        stop: CancellationToken,
        updates: mpsc::UnboundedSender<RemoteUpdate>,
    ) -> BoxFuture<'_, RemoteOutcome> {
        let workers = std::mem::take(&mut *self.workers.lock().unwrap_or_else(|p| p.into_inner()));
        Box::pin(self.drive(workers, setup_data, stop, updates))
    }
}

impl WorkerPool {
    async fn drive(
        &self,
        workers: Vec<Worker>,
        setup_data: Option<String>,
        stop: CancellationToken,
        updates: mpsc::UnboundedSender<RemoteUpdate>,
    ) -> RemoteOutcome {
        let n = workers.len();
        let weights: Vec<u64> = workers.iter().map(|w| w.weight).collect();
        let mut outcome = RemoteOutcome::default();
        let (inbox_tx, mut inbox) = mpsc::unbounded_channel();
        let mut writers = Vec::new();
        let mut children = Vec::new();
        let mut labels = Vec::new();
        for (i, (worker, segment)) in workers
            .into_iter()
            .zip(Segment::weighted(&weights))
            .enumerate()
        {
            let Worker {
                label,
                child,
                conn: Connection { mut rx, tx, .. },
                ..
            } = worker;
            labels.push(label);
            children.push(child);
            writers.push(Some(tx));
            let mut assignment = Assignment::new(
                &self.plan,
                segment,
                self.script.clone(),
                self.env.clone(),
                setup_data.clone(),
            );
            assignment.forward_samples = self.forward_samples;
            assignment.script_source = self.script_source.clone();
            assignment.modules = self.modules.clone();
            let assign = ToWorker::Assign(assignment);
            if let Some(w) = &writers[i]
                && w.send(assign).await.is_err()
            {
                outcome.failure = Some(format!("{}: its stream closed", labels[i]));
            }
            let inbox_tx = inbox_tx.clone();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Some(Ok(message)) => {
                            if inbox_tx.send(Inbox::Message(i, message)).is_err() {
                                return;
                            }
                        }
                        None => {
                            let _ = inbox_tx.send(Inbox::Closed(i, None));
                            return;
                        }
                        Some(Err(e)) => {
                            let _ = inbox_tx.send(Inbox::Closed(i, Some(e.to_string())));
                            return;
                        }
                    }
                }
            });
        }
        drop(inbox_tx);

        // Every worker loads the script, then all start at once.
        let mut ready = 0;
        let mut finished = vec![false; n];
        while outcome.failure.is_none() && ready < n {
            tokio::select! {
                message = inbox.recv() => match message {
                    Some(Inbox::Message(_, FromWorker::Ready)) => ready += 1,
                    Some(Inbox::Message(i, FromWorker::Failed { message })) => {
                        finished[i] = true;
                        outcome.failure = Some(format!("{} couldn't start: {message}", labels[i]));
                    }
                    Some(Inbox::Message(_, _)) => {}
                    Some(Inbox::Closed(i, error)) => {
                        finished[i] = true;
                        outcome.failure = Some(exited(&labels[i], children[i].as_mut(), error).await);
                    }
                    None => break,
                },
                _ = stop.cancelled() => break,
            }
        }
        let start = if outcome.failure.is_none() && !stop.is_cancelled() {
            ToWorker::Start
        } else {
            ToWorker::Stop
        };
        let mut stopping = matches!(start, ToWorker::Stop);
        send_all(&mut writers, &start, &finished).await;

        // Armed (reset) once workers are told to stop.
        let grace = tokio::time::sleep(Duration::from_secs(365 * 24 * 3600));
        tokio::pin!(grace);
        if stopping {
            grace
                .as_mut()
                .reset(tokio::time::Instant::now() + STOP_GRACE);
        }
        while finished.iter().any(|done| !done) {
            tokio::select! {
                message = inbox.recv() => {
                    let failed = match message {
                        Some(Inbox::Message(i, FromWorker::Tick { metrics, progress, .. })) => {
                            let _ = updates.send(RemoteUpdate::Tick { worker: i, metrics, progress });
                            false
                        }
                        Some(Inbox::Message(_, FromWorker::Event(event))) => {
                            let _ = updates.send(RemoteUpdate::Event(event));
                            false
                        }
                        Some(Inbox::Message(i, FromWorker::Samples(samples))) => {
                            let _ = updates.send(RemoteUpdate::Samples { worker: i, samples });
                            false
                        }
                        Some(Inbox::Message(i, FromWorker::Done(summary))) => {
                            finished[i] = true;
                            outcome.iterations_complete += summary.iterations_complete;
                            outcome.iterations_interrupted += summary.iterations_interrupted;
                            outcome.script_errors.extend(summary.script_errors);
                            outcome.request_errors.extend(summary.request_errors);
                            false
                        }
                        Some(Inbox::Message(i, FromWorker::Failed { message })) => {
                            finished[i] = true;
                            outcome.failure.get_or_insert(format!("{} failed: {message}", labels[i]));
                            true
                        }
                        Some(Inbox::Message(_, _)) => false,
                        Some(Inbox::Closed(i, error)) => {
                            let unexpected = !finished[i];
                            finished[i] = true;
                            if unexpected {
                                let message = exited(&labels[i], children[i].as_mut(), error).await;
                                outcome.failure.get_or_insert(message);
                            }
                            unexpected
                        }
                        None => break,
                    };
                    if failed && !stopping {
                        // One part failed: the run can't be what was asked for.
                        stopping = true;
                        send_all(&mut writers, &ToWorker::Stop, &finished).await;
                        grace.as_mut().reset(tokio::time::Instant::now() + STOP_GRACE);
                    }
                }
                _ = stop.cancelled(), if !stopping => {
                    stopping = true;
                    send_all(&mut writers, &ToWorker::Stop, &finished).await;
                    grace.as_mut().reset(tokio::time::Instant::now() + STOP_GRACE);
                }
                _ = &mut grace => {
                    for (i, child) in children.iter_mut().enumerate() {
                        if let (false, Some(child)) = (finished[i], child) {
                            let _ = child.start_kill();
                        }
                    }
                    outcome.failure.get_or_insert(format!(
                        "workers didn't stop within {}s and were killed",
                        STOP_GRACE.as_secs()
                    ));
                    break;
                }
            }
        }
        drop(writers); // workers exit once their connection closes
        for child in children.iter_mut().flatten() {
            if tokio::time::timeout(Duration::from_secs(5), child.wait())
                .await
                .is_err()
            {
                let _ = child.start_kill();
            }
        }
        outcome
    }
}

async fn send_all(
    writers: &mut [Option<mpsc::Sender<ToWorker>>],
    message: &ToWorker,
    finished: &[bool],
) {
    for (i, writer) in writers.iter_mut().enumerate() {
        if finished[i] {
            continue;
        }
        if let Some(w) = writer
            && w.send(message.clone()).await.is_err()
        {
            *writer = None; // it's gone; its reader reports why
        }
    }
}

/// Why worker `i`'s connection ended before it finished.
async fn exited(label: &str, child: Option<&mut Child>, error: Option<String>) -> String {
    let status = match child {
        Some(child) => tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .ok()
            .and_then(Result::ok),
        None => None,
    };
    let how = match (status, error) {
        (Some(status), _) => match status.code() {
            Some(code) => format!("exited with code {code}"),
            None => format!("was stopped by a signal ({status})"),
        },
        // An agent on another machine: all the manager sees is its stream.
        (None, Some(e)) => format!("disconnected ({e})"),
        (None, None) => "disconnected".to_owned(),
    };
    format!("{label} {how} before finishing")
}

/// Pairs each connection with the process started in its slot (from 1).
fn pair_by_slot<C, W>(started: Vec<C>, connected: Vec<(u32, W)>) -> Result<Vec<(C, W)>, String> {
    let mut started: Vec<Option<C>> = started.into_iter().map(Some).collect();
    connected
        .into_iter()
        .map(|(slot, conn)| {
            let process = (slot as usize)
                .checked_sub(1)
                .and_then(|at| started.get_mut(at))
                .ok_or_else(|| {
                    format!("a worker said it's in slot {slot}, which this run didn't start")
                })?
                .take()
                .ok_or_else(|| format!("two workers said they're in slot {slot}"))?;
            Ok((process, conn))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::pair_by_slot;

    #[test]
    fn workers_pair_with_the_process_started_in_their_slot() {
        // On Windows a venv's python.exe starts the real Python as its child,
        // so a worker's pid isn't the started process's: the slot is.
        let started = vec!["first", "second", "third"];
        let connected = vec![(3, "c3"), (1, "c1"), (2, "c2")];
        let paired = pair_by_slot(started, connected).unwrap();
        assert_eq!(
            paired,
            vec![("third", "c3"), ("first", "c1"), ("second", "c2")]
        );
    }

    #[test]
    fn a_slot_no_process_was_started_in_is_refused() {
        let err = pair_by_slot(vec!["first"], vec![(2, "c2")]).unwrap_err();
        assert_eq!(
            err,
            "a worker said it's in slot 2, which this run didn't start"
        );
        let err = pair_by_slot(vec!["first", "second"], vec![(1, "a"), (1, "b")]).unwrap_err();
        assert_eq!(err, "two workers said they're in slot 1");
    }
}
