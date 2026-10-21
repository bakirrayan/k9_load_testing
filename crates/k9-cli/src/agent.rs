//! `k9 agent`: runs tests for a manager on another machine (`k9 run --agents
//! N`). It connects to the manager (waiting for it if it isn't up yet), runs
//! the part of the test it's assigned, and then waits for the next test,
//! unless `--once`. The conversation is a local worker's ([`crate::worker`]).
//!
//! With `--workers N`, the agent starts N worker processes for each test
//! (`k9 agent --once --join ID:N`), which join the manager as one agent and
//! share its part of the test.

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use k9_core::protocol::AgentGroup;
use k9_grpc::ClientOptions;

use crate::exit_code;
use crate::script::ScriptHost;
use crate::worker::{self, Served};

/// Between attempts to reach a manager that isn't up (or doesn't need us).
const RETRY: Duration = Duration::from_secs(1);

pub(crate) struct AgentFlags {
    pub manager: String,
    pub token: String,
    pub tls_ca: Option<PathBuf>,
    pub once: bool,
    pub threads: Option<usize>,
    /// Worker processes per test.
    pub workers: u32,
    /// This agent's share relative to other agents'.
    pub weight: u32,
    /// `ID:N`: this process is one of agent ID's N workers.
    pub join: Option<String>,
}

pub(crate) fn run(flags: AgentFlags, host: Option<&dyn ScriptHost>) -> i32 {
    if flags.workers > 1 && flags.join.is_none() {
        return supervise(&flags, host);
    }
    let group = match &flags.join {
        None => AgentGroup {
            agent: String::new(),
            workers: 1,
            weight: flags.weight,
        },
        Some(join) => match join.rsplit_once(':').map(|(id, n)| (id, n.parse::<u32>())) {
            Some((id, Ok(workers))) if !id.is_empty() && workers >= 1 => AgentGroup {
                agent: id.to_owned(),
                workers,
                weight: flags.weight,
            },
            _ => {
                eprintln!("error: --join expects AGENT_ID:WORKERS, got `{join}`");
                return exit_code::INVALID_CONFIG;
            }
        },
    };
    let mut options = ClientOptions::new(flags.token.clone());
    if let Some(ca) = &flags.tls_ca {
        match std::fs::read(ca) {
            Ok(pem) => options.ca_pem = Some(pem),
            Err(e) => {
                eprintln!("error: {}: {e}", ca.display());
                return exit_code::INVALID_CONFIG;
            }
        }
    }
    let runtime = match worker::runtime(flags.threads) {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("error: could not start the async runtime: {e}");
            return exit_code::GENERIC_ENGINE;
        }
    };
    runtime.block_on(serve(&flags, &options, host, &group))
}

/// `--workers N`: for each test, N worker processes that join as this agent;
/// the first exit code that isn't 0 is the agent's.
fn supervise(flags: &AgentFlags, host: Option<&dyn ScriptHost>) -> i32 {
    let command = host.map_or_else(crate::script::own_command, |h| h.worker_command());
    let n = flags.workers;
    loop {
        let id = format!("{}-{}", std::process::id(), crate::workers::token());
        let mut children = Vec::new();
        for _ in 0..n {
            let mut child = Command::new(&command[0]);
            child
                .args(&command[1..])
                .args(["agent", "--manager", &flags.manager, "--once"])
                .args(["--join", &format!("{id}:{n}")])
                .args(["--weight", &flags.weight.to_string()])
                .env("K9_AGENT_TOKEN", &flags.token);
            if let Some(ca) = &flags.tls_ca {
                child.arg("--tls-ca").arg(ca);
            }
            if let Some(threads) = flags.threads {
                child.args(["--threads", &threads.to_string()]);
            }
            match child.spawn() {
                Ok(child) => children.push(child),
                Err(e) => {
                    eprintln!(
                        "error: could not start a worker ({}): {e}",
                        command.join(" ")
                    );
                    for mut child in children {
                        let _ = child.kill();
                        let _ = child.wait();
                    }
                    return exit_code::GENERIC_ENGINE;
                }
            }
        }
        eprintln!("agent: {n} workers waiting for a test at {}", flags.manager);
        let mut code = exit_code::OK;
        for mut child in children {
            let status = child
                .wait()
                .map(|s| s.code().unwrap_or(exit_code::GENERIC_ENGINE));
            match status {
                Ok(exit_code::OK) => {}
                Ok(other) if code == exit_code::OK => code = other,
                Ok(_) => {}
                Err(e) => {
                    eprintln!("error: waiting for a worker: {e}");
                    code = exit_code::GENERIC_ENGINE;
                }
            }
        }
        if flags.once || code != exit_code::OK {
            return code;
        }
        eprintln!(
            "agent: test finished; waiting for the next one at {}",
            flags.manager
        );
    }
}

async fn serve(
    flags: &AgentFlags,
    options: &ClientOptions,
    host: Option<&dyn ScriptHost>,
    group: &AgentGroup,
) -> i32 {
    let manager = &flags.manager;
    let mut waiting = false;
    loop {
        let served = tokio::select! {
            served = worker::work(manager, options, host, 0, group) => served,
            _ = tokio::signal::ctrl_c() => return exit_code::EXTERNAL_ABORT,
        };
        match served {
            Ok(Served::Ran) => {
                waiting = false;
                if flags.once {
                    return exit_code::OK;
                }
                eprintln!("agent: test finished; waiting for the next one at {manager}");
            }
            Ok(Served::TurnedAway | Served::Unreachable(_)) => {
                if !waiting {
                    eprintln!("agent: waiting for a test at {manager}");
                    waiting = true;
                }
                tokio::select! {
                    _ = tokio::time::sleep(RETRY) => {}
                    _ = tokio::signal::ctrl_c() => return exit_code::EXTERNAL_ABORT,
                }
            }
            Err(message) => {
                eprintln!("error: {message}");
                return exit_code::INVALID_CONFIG;
            }
        }
    }
}
