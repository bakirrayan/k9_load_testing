//! The `k9` command line.
//!
//! Exposed as a library so the same entry point serves both the standalone Rust
//! binary (declarative plans, no Python needed) and the `k9` console script that
//! ships in the Python wheel.

mod agent;
mod bottleneck;
mod control;
mod convert;
mod new;
mod run;
mod script;
mod worker;
mod workers;

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use k9_core::engine::Script;
use k9_core::plan::Stage;
use k9_core::{Duration, Format, OptionsConfig, Overrides, Plan, PlanConfig};
use serde::Serialize;

pub use convert::{ConvertOptions, Converted, convert};
pub use script::{Function, LoadedScript, ScriptHost, SummaryHook, SummaryOutputs};

/// Process exit codes. Values match k6's so existing CI configs stay portable.
pub mod exit_code {
    pub const OK: i32 = 0;
    /// A control command (`k9 status`, `k9 scale`, …) couldn't reach the
    /// control API, or it refused the request.
    pub const COMMAND_FAILED: i32 = 1;
    /// Usage errors (bad flags), as reported by clap.
    pub const USAGE: i32 = 2;
    /// A threshold failed (k6: `ThresholdsHaveFailed`).
    pub const THRESHOLDS_FAILED: i32 = 99;
    /// `setup()` ran past `setupTimeout` (k6: `SetupTimeout`).
    pub const SETUP_TIMEOUT: i32 = 100;
    /// `teardown()` ran past `teardownTimeout` (k6: `TeardownTimeout`).
    pub const TEARDOWN_TIMEOUT: i32 = 101;
    /// The engine failed in an unexpected way (k6: `GenericEngine`).
    pub const GENERIC_ENGINE: i32 = 103;
    /// The plan or options are invalid (k6: `InvalidConfig`).
    pub const INVALID_CONFIG: i32 = 104;
    /// The run was stopped from outside, e.g. with Ctrl-C (k6: `ExternalAbort`).
    pub const EXTERNAL_ABORT: i32 = 105;
    /// The script raised while loading, in `setup()` or in `teardown()`
    /// (k6: `ScriptException`).
    pub const SCRIPT_EXCEPTION: i32 = 107;
}

#[derive(Debug, Parser)]
#[command(
    name = "k9",
    version = k9_core::VERSION,
    about = "Load testing with a Rust engine and a Python API"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
// Parsed once per process: the size of `Run` doesn't matter.
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Run a test
    Run {
        /// Script (.py) or plan file (.yaml, .yml, .toml or .json)
        file: PathBuf,
        #[command(flatten)]
        options: OptionFlags,
        #[command(flatten)]
        script: ScriptFlags,
        #[command(flatten)]
        run: RunFlags,
    },
    /// Show a running test's status (through its control API)
    Status {
        #[command(flatten)]
        api: ApiFlags,
    },
    /// Show a running test's metrics so far
    Stats {
        #[command(flatten)]
        api: ApiFlags,
    },
    /// Pause a running test (new iterations wait)
    Pause {
        #[command(flatten)]
        api: ApiFlags,
    },
    /// Resume a paused test
    Resume {
        #[command(flatten)]
        api: ApiFlags,
    },
    /// Set the VUs of a running test's externally-controlled scenario
    Scale {
        /// Number of VUs (up to the scenario's maxVUs)
        #[arg(short = 'u', long)]
        vus: u32,
        #[command(flatten)]
        api: ApiFlags,
    },
    /// Stop a running test, like Ctrl-C
    Stop {
        #[command(flatten)]
        api: ApiFlags,
    },
    /// Run tests for a manager on another machine (`k9 run --agents N`)
    Agent {
        /// The manager's address: HOST:PORT, or https://HOST:PORT for TLS
        #[arg(long, value_name = "ADDRESS")]
        manager: String,
        /// The manager's agent token (its --agent-token)
        #[arg(long, env = "K9_AGENT_TOKEN", hide_env_values = true)]
        token: String,
        /// CA certificate (PEM) to verify the manager's TLS certificate with
        /// (default: the public web roots)
        #[arg(long, value_name = "PATH")]
        tls_ca: Option<PathBuf>,
        /// Run one test, then exit (default: serve one test after another)
        #[arg(long)]
        once: bool,
        /// Tokio worker threads (default: one per CPU)
        #[arg(long)]
        threads: Option<usize>,
        /// Worker processes to run the agent's share of the test in (or `auto`:
        /// one per CPU core), for scripts that need more than one Python
        /// interpreter's worth of CPU
        #[arg(long, value_name = "N", default_value = "1", value_parser = parse_workers)]
        workers: u32,
        /// This agent's share of the test relative to other agents' (or `auto`:
        /// its CPU cores)
        #[arg(long, value_name = "W", default_value = "1", value_parser = parse_weight)]
        weight: u32,
        /// One of an agent's workers: AGENT_ID:WORKERS (set by the agent itself)
        #[arg(long, hide = true, value_name = "ID:N")]
        join: Option<String>,
    },
    /// Print version information
    Version,
    /// Convert a HAR recording into a plan, correlating the values a
    /// response handed out and later requests sent back
    Convert {
        /// The recording (.har)
        file: PathBuf,
        /// Write the plan here instead of to stdout
        #[arg(short = 'O', long)]
        output: Option<PathBuf>,
        /// Keep only requests to these hosts and their subdomains
        #[arg(long, value_delimiter = ',')]
        only: Vec<String>,
        /// Leave out requests to these hosts and their subdomains
        #[arg(long, value_delimiter = ',')]
        skip: Vec<String>,
        /// Leave out images, fonts, stylesheets and scripts
        #[arg(long)]
        no_static: bool,
        /// Send recorded values as recorded, without extractors
        #[arg(long)]
        no_correlate: bool,
    },
    /// Write a starter plan or script
    New {
        /// The file to write (default: script.py, or plan.yaml for
        /// --template yaml)
        file: Option<PathBuf>,
        /// What to write (default: from the file's extension; .py is async)
        #[arg(long, value_enum)]
        template: Option<new::Template>,
        /// The URL the test requests
        #[arg(long, default_value = new::DEFAULT_URL)]
        url: String,
        /// Overwrite the file if it exists
        #[arg(long)]
        force: bool,
    },
    /// Print the resolved plan without running it
    Inspect {
        /// Script (.py) or plan file (.yaml, .yml, .toml or .json)
        file: PathBuf,
        #[command(flatten)]
        options: OptionFlags,
        #[command(flatten)]
        script: ScriptFlags,
    },
    /// Run as a worker of `k9 run --workers N` (started by k9 itself)
    #[command(hide = true)]
    Worker {
        /// The manager's address
        #[arg(long)]
        connect: String,
        /// The run's secret
        #[arg(long)]
        token: String,
        /// Which of the run's workers this is, from 1
        #[arg(long, default_value_t = 0)]
        slot: u32,
        /// Tokio worker threads (default: one per CPU)
        #[arg(long)]
        threads: Option<usize>,
    },
}

/// Where a running test's control API is (`k9 run --address`).
#[derive(Debug, Args)]
struct ApiFlags {
    /// The control API's address
    #[arg(
        short = 'a',
        long,
        value_name = "HOST:PORT",
        default_value = "localhost:6565"
    )]
    address: String,
}

/// Flags for scripts.
#[derive(Debug, Args)]
struct ScriptFlags {
    /// Make KEY=VALUE visible to the script as k9.env (repeatable)
    #[arg(short = 'e', long = "env", value_name = "KEY=VALUE", value_parser = parse_env_pair)]
    env: Vec<(String, String)>,
}

fn parse_env_pair(pair: &str) -> Result<(String, String), String> {
    match pair.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key.to_owned(), value.to_owned())),
        _ => Err(format!("expected KEY=VALUE, got `{pair}`")),
    }
}

/// Flags that only apply to `k9 run`.
#[derive(Debug, Args)]
struct RunFlags {
    /// Stream samples to an output, e.g. json=results.jsonl (repeatable)
    #[arg(short = 'o', long = "out", value_name = "TYPE=PATH")]
    out: Vec<String>,
    /// Write the end-of-test summary as JSON (k6's format) to this file
    #[arg(long, value_name = "PATH")]
    summary_export: Option<PathBuf>,
    /// Write a self-contained HTML report (summary, thresholds, checks and
    /// charts) to this file
    #[arg(long, value_name = "PATH")]
    report: Option<PathBuf>,
    /// Don't evaluate thresholds
    #[arg(long)]
    no_thresholds: bool,
    /// Run only this part of the test, e.g. 1/3:2/3; the parts of one sequence
    /// add up to the whole test (k6's execution segments)
    #[arg(long, value_name = "FROM:TO")]
    execution_segment: Option<String>,
    /// The points that cut the test into parts, e.g. 0,1/3,2/3,1 (default: 0,
    /// the segment's own bounds, 1). Give every part the same sequence
    #[arg(long, value_name = "POINTS")]
    execution_segment_sequence: Option<String>,
    /// Split the test across N processes (or `auto`: one per CPU core), for
    /// scripts that need more than one Python interpreter's worth of CPU
    #[arg(long, value_name = "N", default_value = "1", value_parser = parse_workers)]
    workers: u32,
    /// Run the test on N agents (`k9 agent`, on other machines) instead of here
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u32).range(1..))]
    agents: Option<u32>,
    /// Where agents connect (with --agents)
    #[arg(
        long,
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6570",
        requires = "agents"
    )]
    listen: String,
    /// The token agents must present (with --agents; default: a new one,
    /// printed with the command to start agents)
    #[arg(
        long,
        env = "K9_AGENT_TOKEN",
        hide_env_values = true,
        requires = "agents"
    )]
    agent_token: Option<String>,
    /// TLS certificate (PEM) for agents to connect over (with --tls-key)
    #[arg(long, value_name = "PATH", requires_all = ["agents", "tls_key"])]
    tls_cert: Option<PathBuf>,
    /// The TLS certificate's private key (PEM)
    #[arg(long, value_name = "PATH", requires = "tls_cert")]
    tls_key: Option<PathBuf>,
    /// Serve the control API (k6's /v1/status: pause, resume, scale, stop) on
    /// this address, e.g. localhost:6565
    #[arg(short = 'a', long, value_name = "HOST:PORT")]
    address: Option<String>,
    /// Start paused; resume through the control API (needs --address)
    #[arg(short = 'p', long, requires = "address")]
    paused: bool,
    /// Show a live dashboard in the terminal while the test runs
    #[arg(long)]
    tui: bool,
    /// Don't show progress while the test runs
    #[arg(short = 'q', long)]
    quiet: bool,
    /// Skip the end-of-test summary, handle_summary() and --summary-export
    #[arg(long)]
    no_summary: bool,
    /// Plain output, without colours (also with NO_COLOR set)
    #[arg(long)]
    no_color: bool,
    /// Also export the trend stats k9.run() reports (count, p(99), p(99.9), …)
    #[arg(long, hide = true)]
    export_all_trend_stats: bool,
    /// The dashboard's colours: brand (the logo's gold and blue) or neon
    #[arg(long, value_name = "THEME", default_value = "brand")]
    theme: k9_tui::ThemeName,
}

fn parse_weight(value: &str) -> Result<u32, String> {
    match value {
        "auto" => parse_workers(value),
        _ => value
            .parse::<u32>()
            .ok()
            .filter(|w| *w >= 1)
            .ok_or_else(|| format!("expected a weight (1 or more) or `auto`, got `{value}`")),
    }
}

fn parse_workers(value: &str) -> Result<u32, String> {
    if value == "auto" {
        return Ok(std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1));
    }
    match value.parse::<u32>() {
        Ok(n) if n >= 1 => Ok(n),
        _ => Err(format!(
            "expected a number of workers (1 or more) or `auto`, got `{value}`"
        )),
    }
}

/// Option overrides. They beat `K9_*` environment variables, which beat the
/// plan. Setting --duration, --iterations or --stage replaces the plan's
/// scenarios, as in k6.
#[derive(Debug, Args)]
struct OptionFlags {
    /// Number of VUs
    #[arg(short = 'u', long)]
    vus: Option<u32>,
    /// Test duration, e.g. 30s or 1m30s
    #[arg(short = 'd', long)]
    duration: Option<Duration>,
    /// Total iterations, shared among the VUs
    #[arg(short = 'i', long)]
    iterations: Option<u64>,
    /// A ramping stage as duration:target VUs, e.g. 30s:10 (repeatable)
    #[arg(short = 's', long = "stage", value_name = "DURATION:TARGET")]
    stages: Vec<Stage>,
    /// Redirects to follow per request (default 10)
    #[arg(long)]
    max_redirects: Option<u32>,
    /// Accept any TLS certificate
    #[arg(long)]
    insecure_skip_tls_verify: bool,
    /// User-Agent header ("" to send none)
    #[arg(long)]
    user_agent: Option<String>,
    /// Read response bodies but don't keep them
    #[arg(long)]
    discard_response_bodies: bool,
    /// HTTP versions to speak: auto (HTTP/2 when an HTTPS server accepts it), 1.1, 2 or 3 (QUIC)
    #[arg(long, value_name = "VERSION")]
    http_version: Option<k9_core::plan::HttpVersion>,
    /// Requests of a batch step in flight at once (default 20)
    #[arg(long, value_name = "N")]
    batch: Option<u32>,
    /// Requests of a batch step in flight at once per host (default 6, 0: no limit)
    #[arg(long, value_name = "N")]
    batch_per_host: Option<u32>,
    /// DNS lookups: ttl=5m|0|inf, select=random|first|roundRobin,
    /// policy=preferIPv4|preferIPv6|onlyIPv4|onlyIPv6|any
    #[arg(long, value_name = "OPTIONS")]
    dns: Option<String>,
    /// Hostnames requests may not go to (`*.domain` for all names under it)
    #[arg(long, value_name = "HOSTS", value_delimiter = ',')]
    block_hostnames: Vec<String>,
    /// IPs, CIDR blocks or first-last spans requests may not go to (repeatable)
    #[arg(long, value_name = "RANGE", value_delimiter = ',')]
    blacklist_ip: Vec<String>,
    /// Addresses to send from, handed out to VUs in turn (IPs, CIDR blocks or first-last spans)
    #[arg(long, value_name = "RANGES", value_delimiter = ',')]
    local_ips: Vec<String>,
    /// A new connection for every request
    #[arg(long)]
    no_connection_reuse: bool,
    /// Close each VU's connections between its iterations
    #[arg(long)]
    no_vu_connection_reuse: bool,
    /// Pad iterations shorter than this with a sleep, e.g. 1s
    #[arg(long, value_name = "DURATION")]
    min_iteration_duration: Option<Duration>,
    /// Script requests that get no response raise instead of returning
    #[arg(short = 'w', long)]
    throw: bool,
    /// The system tags samples carry, e.g. status,method,url,name,check
    #[arg(long, value_name = "TAGS", value_delimiter = ',')]
    system_tags: Vec<String>,
    /// Keep each VU's cookies from one iteration to the next
    #[arg(long)]
    no_cookies_reset: bool,
    /// Print each request and response on stderr: headers, or with =full the bodies too
    #[arg(long, value_name = "full", num_args = 0..=1, default_missing_value = "headers", require_equals = true)]
    http_debug: Option<String>,
    /// One unit for every time in the summary: s, ms or us
    #[arg(long, value_name = "UNIT")]
    summary_time_unit: Option<String>,
    /// The values the summary shows for trends, e.g. avg,med,p(99),count
    #[arg(long, value_name = "STATS")]
    summary_trend_stats: Option<String>,
}

impl OptionFlags {
    fn overrides(&self) -> Overrides {
        Overrides {
            vus: self.vus,
            duration: self.duration,
            iterations: self.iterations,
            stages: (!self.stages.is_empty()).then(|| self.stages.clone()),
            max_redirects: self.max_redirects,
            insecure_skip_tls_verify: self.insecure_skip_tls_verify.then_some(true),
            user_agent: self.user_agent.clone(),
            discard_response_bodies: self.discard_response_bodies.then_some(true),
            http_version: self.http_version,
            batch: self.batch,
            batch_per_host: self.batch_per_host,
            dns: self.dns.clone(),
            block_hostnames: (!self.block_hostnames.is_empty())
                .then(|| self.block_hostnames.clone()),
            blacklist_ips: (!self.blacklist_ip.is_empty()).then(|| self.blacklist_ip.clone()),
            local_ips: (!self.local_ips.is_empty()).then(|| self.local_ips.clone()),
            no_connection_reuse: self.no_connection_reuse.then_some(true),
            no_vu_connection_reuse: self.no_vu_connection_reuse.then_some(true),
            summary_time_unit: self.summary_time_unit.clone(),
            min_iteration_duration: self.min_iteration_duration,
            throw: self.throw.then_some(true),
            system_tags: (!self.system_tags.is_empty()).then(|| self.system_tags.clone()),
            no_cookies_reset: self.no_cookies_reset.then_some(true),
            http_debug: self.http_debug.clone(),
            summary_trend_stats: self
                .summary_trend_stats
                .as_deref()
                .map(k9_core::overrides::split_list),
        }
    }
}

/// Runs the CLI with the given arguments (including the program name) and
/// returns the process exit code. Scripts can't be loaded; see [`run_with_host`].
pub fn run<I, T>(args: I) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    run_inner(args, None)
}

/// Like [`run`], loading `.py` scripts through `host`.
pub fn run_with_host<I, T>(args: I, host: &dyn ScriptHost) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    run_inner(args, Some(host))
}

fn run_inner<I, T>(args: I, host: Option<&dyn ScriptHost>) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(err) => {
            let _ = err.print();
            return if err.use_stderr() {
                exit_code::USAGE
            } else {
                exit_code::OK // --help / --version
            };
        }
    };
    match cli.command {
        Command::Worker {
            connect,
            token,
            slot,
            threads,
        } => worker::run(&connect, &token, slot, threads, host),
        Command::Agent {
            manager,
            token,
            tls_ca,
            once,
            threads,
            workers,
            weight,
            join,
        } => agent::run(
            agent::AgentFlags {
                manager,
                token,
                tls_ca,
                once,
                threads,
                workers,
                weight,
                join,
            },
            host,
        ),
        Command::Status { api } => control::command(&api.address, control::Action::Status),
        Command::Stats { api } => control::command(&api.address, control::Action::Stats),
        Command::Pause { api } => control::command(&api.address, control::Action::Pause),
        Command::Resume { api } => control::command(&api.address, control::Action::Resume),
        Command::Scale { vus, api } => control::command(&api.address, control::Action::Scale(vus)),
        Command::Stop { api } => control::command(&api.address, control::Action::Stop),
        Command::Version => {
            println!("k9 {}", k9_core::VERSION);
            exit_code::OK
        }
        Command::Convert {
            file,
            output,
            only,
            skip,
            no_static,
            no_correlate,
        } => convert::command(
            &file,
            output.as_deref(),
            &ConvertOptions {
                only,
                skip,
                no_static,
                no_correlate,
            },
        ),
        Command::New {
            file,
            template,
            url,
            force,
        } => new::command(file.as_deref(), template, &url, force),
        Command::Inspect {
            file,
            options,
            script,
        } => match load(&file, &options, &script, host) {
            Ok(loaded) => inspect(&loaded),
            Err(code) => code,
        },
        Command::Run {
            file,
            options,
            script,
            run,
        } => {
            let loaded = match load(&file, &options, &script, host) {
                Ok(loaded) => loaded,
                Err(code) => return code,
            };
            if let Some(info) = &loaded.script {
                warn_about_sync_vus(&loaded.plan, &info.sync_execs);
            }
            if run.agents.is_some() && run.workers > 1 {
                eprintln!(
                    "error: --agents runs the test on agents; don't combine it with --workers"
                );
                return exit_code::INVALID_CONFIG;
            }
            // Agents get the script's local modules with it.
            let modules = match (run.agents, &loaded.script, host) {
                (Some(_), Some(_), Some(host)) => match host.local_modules(&file) {
                    Ok(modules) => modules,
                    Err(message) => {
                        eprintln!("error: {message}");
                        return exit_code::INVALID_CONFIG;
                    }
                },
                _ => Vec::new(),
            };
            let launch = match run.agents {
                Some(count) => Some((
                    count,
                    workers::Launch::Agents(workers::AgentsSetup {
                        listen: run.listen.clone(),
                        token: run.agent_token.clone(),
                        tls: run.tls_cert.clone().zip(run.tls_key.clone()),
                        modules,
                    }),
                )),
                None => (run.workers > 1).then(|| {
                    (
                        run.workers,
                        workers::Launch::Local(
                            host.map_or_else(script::own_command, |h| h.worker_command()),
                        ),
                    )
                }),
            };
            let workers = launch.map(|(count, launch)| workers::WorkerSetup {
                count,
                launch,
                script: if loaded.script.is_some() {
                    std::path::absolute(&file)
                        .unwrap_or_else(|_| file.clone())
                        .display()
                        .to_string()
                } else {
                    String::new()
                },
                env: script.env.clone(),
                forward_samples: !run.out.is_empty(),
            });
            let (script, summary_hook) = match loaded.script {
                Some(info) => (Some(info.script), info.summary_hook),
                None => (None, None),
            };
            run::run(&file, loaded.plan, script, summary_hook, workers, &run)
        }
    }
}

/// Sync VUs per process above which K9 warns: each has a thread, and they
/// share the GIL, so the load generator rather than the target becomes the
/// bottleneck.
const SYNC_VUS_WARNING: u32 = 500;

fn warn_about_sync_vus(plan: &Plan, sync_execs: &[String]) {
    let sync_vus: u32 = plan
        .options
        .scenarios
        .values()
        .filter(|s| sync_execs.contains(&s.exec))
        .map(|s| s.executor.max_vus())
        .sum();
    if sync_vus > SYNC_VUS_WARNING {
        eprintln!(
            "WARN {sync_vus} VUs run plain def functions, each on its own thread in one \
             process; they share the GIL, so above ~{SYNC_VUS_WARNING} the results may \
             measure Python rather than the target. Use async def functions for high VU \
             counts, or --workers to spread sync VUs across processes"
        );
    }
}

/// A resolved plan, plus the script it came from (if any).
struct Loaded {
    plan: Plan,
    script: Option<ScriptInfo>,
}

struct ScriptInfo {
    script: Arc<dyn Script>,
    summary_hook: Option<Arc<dyn SummaryHook>>,
    hooks: Vec<String>,
    /// Functions that scenarios iterate that are plain `def` (a thread per VU).
    sync_execs: Vec<String>,
}

/// What `k9 inspect` prints: the detected tier and the fully resolved plan.
#[derive(Serialize)]
struct Inspection<'a> {
    /// `declarative` (no Python in the run), or `async` / `sync` for scripts,
    /// by how their iteration functions are defined.
    tier: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    hooks: Option<&'a [String]>,
    plan: &'a Plan,
}

fn inspect(loaded: &Loaded) -> i32 {
    let inspection = Inspection {
        tier: match &loaded.script {
            None => "declarative",
            Some(info) if info.sync_execs.is_empty() => "async",
            Some(_) => "sync",
        },
        hooks: loaded.script.as_ref().map(|info| info.hooks.as_slice()),
        plan: &loaded.plan,
    };
    match serde_json::to_string_pretty(&inspection) {
        Ok(json) => {
            println!("{json}");
            exit_code::OK
        }
        Err(err) => {
            eprintln!("error: could not serialize plan: {err}");
            exit_code::INVALID_CONFIG
        }
    }
}

/// Reads a plan file or loads a script, applies environment and flag
/// overrides, and resolves the plan. Prints any problem and returns the exit code.
fn load(
    file: &Path,
    flags: &OptionFlags,
    script_flags: &ScriptFlags,
    host: Option<&dyn ScriptHost>,
) -> Result<Loaded, i32> {
    let fail = |err: &dyn std::fmt::Display| {
        eprintln!("error: {}: {err}", file.display());
        exit_code::INVALID_CONFIG
    };
    let overrides = |mut config: PlanConfig| -> Result<PlanConfig, i32> {
        let env = Overrides::from_env(std::env::vars()).map_err(|e| fail(&e))?;
        config
            .apply(&flags.overrides().or(env))
            .map_err(|e| fail(&e))?;
        Ok(config)
    };

    if script::is_script(file) {
        let Some(host) = host else {
            eprintln!(
                "error: {}: Python scripts run through the k9 Python package \
                 (pip install k9-load); this binary only runs .yaml, .toml and .json plans",
                file.display()
            );
            return Err(exit_code::INVALID_CONFIG);
        };
        let loaded = host.load(file, &script_flags.env).map_err(|traceback| {
            eprintln!(
                "error: {}: the script raised while loading:\n{traceback}",
                file.display()
            );
            exit_code::SCRIPT_EXCEPTION
        })?;
        if let Some(plan) = loaded.plan {
            // Tier 1: the script built a plan; nothing of it runs in the test.
            let config: PlanConfig =
                serde_json::from_value(plan).map_err(|e| fail(&format!("invalid `plan`: {e}")))?;
            return declarative(file, overrides(config)?).map_err(|e| fail(&e));
        }
        let options: OptionsConfig = if loaded.options.is_null() {
            OptionsConfig::default()
        } else {
            serde_json::from_value(loaded.options)
                .map_err(|e| fail(&format!("invalid `options`: {e}")))?
        };
        let config = overrides(PlanConfig {
            options,
            ..PlanConfig::default()
        })?;
        let names: Vec<&str> = loaded.functions.iter().map(|f| f.name.as_str()).collect();
        let mut plan = config.resolve_script(&names).map_err(|e| fail(&e))?;
        // Files in the options (`tlsAuth`) are relative to the script.
        let dir = file.parent().filter(|d| !d.as_os_str().is_empty());
        plan.rebase_files(
            &std::path::absolute(dir.unwrap_or(Path::new("."))).map_err(|e| fail(&e))?,
        );
        let is_sync = |name: &str| {
            loaded
                .functions
                .iter()
                .any(|f| f.name == name && !f.is_async)
        };
        let mut sync_execs: Vec<String> = Vec::new();
        for (name, scenario) in &plan.options.scenarios {
            // A VU runs on the event loop or on a thread of its own, so all
            // of a scenario's functions must be of one kind.
            let entries = scenario.entry_points();
            let sync = entries.iter().filter(|(_, f)| is_sync(f)).count();
            if sync != 0 && sync != entries.len() {
                return Err(fail(&format!(
                    "scenario `{name}` mixes `async def` and plain `def` functions \
                     (its VUs run either on the event loop or on threads)"
                )));
            }
            for (_, function) in entries.into_iter().filter(|_| sync > 0) {
                if !sync_execs.iter().any(|f| f == function) {
                    sync_execs.push(function.to_owned());
                }
            }
        }
        return Ok(Loaded {
            plan,
            script: Some(ScriptInfo {
                script: loaded.script,
                summary_hook: loaded.summary_hook,
                hooks: loaded.hooks,
                sync_execs,
            }),
        });
    }

    if !script_flags.env.is_empty() {
        eprintln!(
            "WARN -e/--env only applies to scripts; {} is a plan",
            file.display()
        );
    }
    let Some(format) = Format::from_path(file) else {
        eprintln!(
            "error: {}: unsupported file type (expected .py, .yaml, .yml, .toml or .json)",
            file.display()
        );
        return Err(exit_code::INVALID_CONFIG);
    };
    let source = std::fs::read_to_string(file).map_err(|err| fail(&err))?;
    let config = PlanConfig::parse(&source, format).map_err(|e| fail(&e))?;
    declarative(file, overrides(config)?).map_err(|e| fail(&e))
}

/// Resolves a declarative plan from `file`, whose feeder files are relative
/// to the file's directory.
fn declarative(file: &Path, config: PlanConfig) -> Result<Loaded, Box<dyn std::error::Error>> {
    let mut plan = config.resolve()?;
    // `plan.yaml`'s parent is the empty path.
    let dir = file.parent().filter(|d| !d.as_os_str().is_empty());
    plan.rebase_files(&std::path::absolute(dir.unwrap_or(Path::new(".")))?);
    Ok(Loaded { plan, script: None })
}
