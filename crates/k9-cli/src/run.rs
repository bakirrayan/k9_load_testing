//! `k9 run`: header, live status line, Ctrl-C handling, end-of-test summary,
//! thresholds and outputs.

use std::collections::HashMap;
use std::io::{IsTerminal, Write};
use std::path::Path;
use std::sync::Arc;

use k9_core::engine::{
    self, EngineError, HookError, Role, RunControl, RunEvent, RunOptions, RunResult, Script,
    ScriptError,
};
use k9_core::{Plan, Segment};
use k9_metrics::threshold::Selector;
use k9_output::{
    CsvOutput, InfluxConfig, InfluxOutput, InfluxVersion, JsonOutput, OtlpConfig, OtlpOutput,
    Output, PrometheusConfig, PrometheusOutput, ReportInfo, RunHeader, ScenarioLine, StatsdConfig,
    StatsdFlavor, StatsdOutput, SummaryData, SummaryState, ThresholdResult, TrendStat, html_report,
    render_checks, render_header, render_metrics, status_line, summary_data, summary_export,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::script::{SummaryHook, SummaryOutputs};
use crate::workers::{Launch, WorkerPool, WorkerSetup};
use crate::{RunFlags, exit_code};
use crate::{bottleneck, control};

/// How many distinct request errors to list after the summary.
const MAX_ERRORS_SHOWN: usize = 10;

pub(crate) fn run(
    file: &Path,
    mut plan: Plan,
    script: Option<Arc<dyn Script>>,
    summary_hook: Option<Arc<dyn SummaryHook>>,
    workers: Option<WorkerSetup>,
    flags: &RunFlags,
) -> i32 {
    if workers.is_some() {
        let conflict = if flags.execution_segment.is_some() {
            Some(
                "--workers and --agents run every segment themselves; don't combine them with --execution-segment",
            )
        } else {
            None
        };
        if let Some(message) = conflict {
            eprintln!("error: {message}");
            return exit_code::INVALID_CONFIG;
        }
    }
    if flags.no_thresholds {
        plan.options.thresholds.clear();
    }
    let segment = match execution_segment(flags) {
        Ok(segment) => segment,
        Err(message) => {
            eprintln!("error: {message}");
            return exit_code::INVALID_CONFIG;
        }
    };
    let outputs = match create_outputs(&flags.out, &plan) {
        Ok(outputs) => outputs,
        Err(message) => {
            eprintln!("error: {message}");
            return exit_code::INVALID_CONFIG;
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("error: could not start the async runtime: {err}");
            return exit_code::GENERIC_ENGINE;
        }
    };
    runtime.block_on(run_async(
        file,
        plan,
        segment,
        script,
        summary_hook,
        workers,
        outputs,
        flags,
    ))
}

/// The part of the test to run, from `--execution-segment` and
/// `--execution-segment-sequence`.
fn execution_segment(flags: &RunFlags) -> Result<Segment, String> {
    let segment = match &flags.execution_segment {
        Some(text) => text.parse::<Segment>().map_err(|e| e.to_string())?,
        None if flags.execution_segment_sequence.is_some() => {
            return Err("--execution-segment-sequence needs --execution-segment".into());
        }
        None => return Ok(Segment::full()),
    };
    let Some(sequence) = &flags.execution_segment_sequence else {
        return Ok(segment);
    };
    let parts = Segment::sequence(sequence).map_err(|e| e.to_string())?;
    let wanted = segment.to_string();
    parts
        .iter()
        .find(|part| part.to_string() == wanted)
        .cloned()
        .ok_or_else(|| {
            let names: Vec<String> = parts.iter().map(ToString::to_string).collect();
            format!(
                "execution segment {wanted} isn't one of the sequence {sequence} (its segments: {})",
                names.join(", ")
            )
        })
}

/// Builds `--out` outputs: `json=PATH`.
fn create_outputs(specs: &[String], plan: &Plan) -> Result<Vec<Box<dyn Output>>, String> {
    let mut thresholds: HashMap<String, Vec<String>> = HashMap::new();
    for (selector, list) in &plan.options.thresholds {
        if let Ok(selector) = selector.parse::<Selector>() {
            thresholds
                .entry(selector.metric)
                .or_default()
                .extend(list.iter().map(|t| t.expression().to_owned()));
        }
    }
    let mut outputs: Vec<Box<dyn Output>> = Vec::new();
    for spec in specs {
        let (kind, arg) = match spec.split_once('=') {
            Some((kind, arg)) => (kind, Some(arg)),
            None => (spec.as_str(), None),
        };
        match (kind, arg) {
            ("json", Some(path)) if !path.is_empty() => {
                let output = JsonOutput::create(Path::new(path), thresholds.clone())
                    .map_err(|e| format!("--out {spec}: {e}"))?;
                outputs.push(Box::new(output));
            }
            ("csv", Some(path)) if !path.is_empty() => {
                let output =
                    CsvOutput::create(Path::new(path)).map_err(|e| format!("--out {spec}: {e}"))?;
                outputs.push(Box::new(output));
            }
            ("prometheus-rw" | "experimental-prometheus-rw", url) => {
                let config = PrometheusConfig::from_env(url, std::env::vars())?;
                let output =
                    PrometheusOutput::start(config).map_err(|e| format!("--out {spec}: {e}"))?;
                outputs.push(Box::new(output));
            }
            ("influxdb" | "xk6-influxdb", url) => {
                let version = if kind == "influxdb" {
                    InfluxVersion::V1
                } else {
                    InfluxVersion::V2
                };
                let config = InfluxConfig::from_env(version, url, std::env::vars())?;
                let output =
                    InfluxOutput::start(config).map_err(|e| format!("--out {spec}: {e}"))?;
                outputs.push(Box::new(output));
            }
            ("statsd" | "datadog", address) => {
                let flavor = if kind == "statsd" {
                    StatsdFlavor::Statsd
                } else {
                    StatsdFlavor::Datadog
                };
                let config = StatsdConfig::from_env(flavor, address, std::env::vars())?;
                let output =
                    StatsdOutput::start(config).map_err(|e| format!("--out {spec}: {e}"))?;
                outputs.push(Box::new(output));
            }
            ("opentelemetry" | "experimental-opentelemetry", url) => {
                let config = OtlpConfig::from_env(url, std::env::vars())?;
                let output = OtlpOutput::start(config).map_err(|e| format!("--out {spec}: {e}"))?;
                outputs.push(Box::new(output));
            }
            ("json" | "csv", _) => {
                let extension = if kind == "json" { "jsonl" } else { "csv" };
                return Err(format!(
                    "the {kind} output needs a file: --out {kind}=results.{extension}"
                ));
            }
            _ => {
                return Err(format!(
                    "unknown output `{kind}` (supported: json=PATH, csv=PATH, prometheus-rw[=URL], opentelemetry[=URL], influxdb[=URL], xk6-influxdb[=URL], statsd[=ADDR], datadog[=ADDR])"
                ));
            }
        }
    }
    Ok(outputs)
}

#[allow(clippy::too_many_arguments)]
async fn run_async(
    file: &Path,
    mut plan: Plan,
    segment: Segment,
    script: Option<Arc<dyn Script>>,
    summary_hook: Option<Arc<dyn SummaryHook>>,
    workers: Option<WorkerSetup>,
    outputs: Vec<Box<dyn Output>>,
    flags: &RunFlags,
) -> i32 {
    let worker_count = workers
        .as_ref()
        .filter(|w| matches!(w.launch, Launch::Local(_)))
        .map(|w| w.count);
    // Agents connect to an address that goes in the header: listen first.
    let setup_script = workers
        .as_ref()
        .map(|w| w.script.clone())
        .unwrap_or_default();
    let agents = match workers.as_ref().map(|w| (w.count, &w.launch)) {
        Some((count, Launch::Agents(setup))) => match setup.bind(&mut plan, &setup_script).await {
            Ok(listener) => Some((count, listener)),
            Err(message) => {
                eprintln!("error: {message}");
                return exit_code::INVALID_CONFIG;
            }
        },
        _ => None,
    };
    // Validated with the plan.
    let time_unit = plan
        .options
        .summary_time_unit
        .as_deref()
        .and_then(|u| k9_output::TimeUnit::parse(u).ok());
    let trend_stats: Vec<TrendStat> = plan
        .options
        .summary_trend_stats
        .iter()
        .filter_map(|s| TrendStat::parse(s).ok())
        .collect();
    let header = RunHeader {
        script: file.display().to_string(),
        scenarios: plan
            .options
            .scenarios
            .iter()
            .map(|(name, s)| ScenarioLine {
                name: name.clone(),
                description: s.describe(),
            })
            .collect(),
        max_vus: plan
            .options
            .scenarios
            .values()
            .map(|s| segment.share(u64::from(s.executor.max_vus())) as u32)
            .sum(),
        max_duration: plan.max_duration().as_std(),
        outputs: outputs.iter().map(|o| o.description()).collect(),
        segment: (!segment.is_full()).then(|| (segment.to_string(), segment.fraction())),
        workers: worker_count,
        agents: agents
            .as_ref()
            .map(|(count, listener)| (*count, listener.address.clone())),
        control: None,
    };
    let control = RunControl::new();
    let listener = match &flags.address {
        Some(address) => match control::bind(address).await {
            Ok(listener) => Some(listener),
            Err(message) => {
                eprintln!("error: {message}");
                return exit_code::INVALID_CONFIG;
            }
        },
        None => None,
    };
    let header = RunHeader {
        control: listener
            .as_ref()
            .and_then(|l| l.local_addr().ok())
            .map(|address| {
                let paused = if flags.paused { " (paused)" } else { "" };
                format!("http://{address}{paused}")
            }),
        ..header
    };
    print!("{}", render_header(&header));
    if let Some((_, listener)) = &agents
        && let Some(token) = &listener.generated_token
    {
        println!(
            "        agents: k9 agent --manager {} --token {token}\n",
            listener.address
        );
    }
    let _ = std::io::stdout().flush();
    // Agents run the VUs on their own machines.
    if agents.is_none() {
        for warning in bottleneck::check_at_start(u64::from(header.max_vus)) {
            bottleneck::warn(&warning, false);
        }
    }

    // The API answers from now on, before the engine has read the plan.
    control.prepare(&plan);
    if flags.paused {
        // Before the start, any run can be paused.
        let _ = control.pause();
    }
    if let Some(listener) = listener {
        control::serve(listener, control.clone());
    }
    tokio::spawn(handle_ctrl_c(control.clone()));
    // The dashboard draws on stdout and reads keys from stdin.
    let tui = flags.tui && std::io::stdout().is_terminal() && std::io::stdin().is_terminal();
    if flags.tui && !tui {
        eprintln!("WARN --tui needs a terminal; showing the usual output");
    }
    let live = !tui && !flags.quiet && std::io::stderr().is_terminal();
    if live {
        tokio::spawn(show_progress(control.clone()));
    }
    let run_done = CancellationToken::new();
    // Subscribed now, so the dashboard gets errors from the very start.
    let (events, dashboard_events) = if tui {
        (None, Some(control.events()))
    } else {
        let printer = print_events(control.events(), live, run_done.clone());
        (Some(tokio::spawn(printer)), None)
    };
    let (notes, dashboard_notes) = tokio::sync::mpsc::unbounded_channel();
    if tui {
        tokio::spawn(bottleneck::watch(run_done.clone(), move |warning| {
            let _ = notes.send(warning);
        }));
    } else {
        tokio::spawn(bottleneck::watch(run_done.clone(), move |warning| {
            bottleneck::warn(&warning, live);
        }));
    }

    let role = match workers {
        None => Role::Standalone,
        Some(setup) => {
            let agents = agents.map(|(_, listener)| listener);
            match WorkerPool::start(setup, plan.clone(), agents, &control).await {
                Ok(pool) => Role::Manager(Arc::new(pool)),
                Err(message) => {
                    run_done.cancel();
                    eprintln!("error: {message}");
                    return if control.is_stopped() {
                        exit_code::EXTERNAL_ABORT
                    } else {
                        exit_code::GENERIC_ENGINE
                    };
                }
            }
        }
    };
    let options = RunOptions {
        segment,
        outputs,
        script,
        role,
    };
    // Started once workers or agents are in, so waiting for them stays visible.
    let dashboard = dashboard_events.map(|events| {
        let options = k9_tui::Options {
            header: k9_tui::Header {
                script: header.script.clone(),
                max_vus: header.max_vus,
                max_duration: header.max_duration,
            },
            theme: k9_tui::Theme::new(
                flags.theme,
                if no_color(flags) {
                    k9_tui::ColorDepth::None
                } else {
                    k9_tui::ColorDepth::from_env()
                },
            ),
            force_exit_code: exit_code::EXTERNAL_ABORT,
        };
        tokio::spawn(k9_tui::run(
            control.clone(),
            options,
            events,
            dashboard_notes,
            run_done.clone(),
        ))
    });
    let started = std::time::SystemTime::now();
    let result = engine::run_with(&plan, control, options).await;
    run_done.cancel();
    // The terminal is back to normal before anything else is printed.
    if let Some(dashboard) = dashboard {
        match dashboard.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => eprintln!("WARN the dashboard failed: {err}"),
            Err(err) => eprintln!("WARN the dashboard failed: {err}"),
        }
    }
    if let Some(events) = events {
        let _ = events.await;
    }
    let result = match result {
        Ok(result) => result,
        Err(EngineError::Setup(err)) => {
            if live {
                eprint!("\r\x1b[2K");
            }
            eprintln!("error: {err}");
            return hook_exit_code(&err);
        }
        Err(err) => {
            eprintln!("error: {err}");
            return exit_code::INVALID_CONFIG;
        }
    };
    if live {
        eprint!("\r\x1b[2K"); // clear the live status line
    }

    let metrics = result.aggregator.metrics();
    let mut summary = SummaryData::new(
        result.aggregator.totals(),
        metrics.registry(),
        metrics.interner(),
        result.duration,
    );
    summary.thresholds = threshold_results(&result);
    summary.trend_stats = trend_stats;
    summary.time_unit = time_unit;

    let text = format!(
        "\n{}\n{}\n{}\n",
        status_line(
            result.duration,
            0,
            result.max_vus,
            result.iterations_complete,
            result.iterations_interrupted
        ),
        render_checks(&summary),
        render_metrics(&summary),
    );
    let summary_error = match &summary_hook {
        _ if flags.no_summary => None,
        None => {
            print!("{text}");
            None
        }
        Some(hook) => {
            let state = SummaryState {
                stdout_tty: std::io::stdout().is_terminal(),
                stderr_tty: live,
                no_color: no_color(flags),
            };
            match hook
                .handle_summary(summary_data(&summary, state), text.clone())
                .await
            {
                Ok(outputs) => {
                    write_summary_outputs(outputs);
                    None
                }
                Err(err) => {
                    // Like k6: fall back to the default summary.
                    print!("{text}");
                    Some(err)
                }
            }
        }
    };
    let _ = std::io::stdout().flush();

    if let Some(path) = &flags.report {
        let info = ReportInfo {
            header: &header,
            started,
            ticks: result.aggregator.ticks(),
        };
        match std::fs::write(path, html_report(&summary, &info)) {
            Ok(()) => println!("report: {}", path.display()),
            Err(e) => eprintln!("WARN could not write the report to {}: {e}", path.display()),
        }
    }
    if let Some(path) = flags.summary_export.as_ref().filter(|_| !flags.no_summary) {
        if flags.export_all_trend_stats {
            for stat in [
                "count", "avg", "min", "med", "max", "p(90)", "p(95)", "p(99)", "p(99.9)",
            ] {
                let stat = TrendStat::parse(stat).expect("a valid stat");
                if !summary.trend_stats.contains(&stat) {
                    summary.trend_stats.push(stat);
                }
            }
        }
        let json = serde_json::to_string_pretty(&summary_export(&summary))
            .expect("summary JSON serializes");
        if let Err(e) = std::fs::write(path, json + "\n") {
            eprintln!(
                "WARN could not write the summary export to {}: {e}",
                path.display()
            );
        }
    }
    report_problems(&result);
    if let Some(err) = &summary_error {
        eprintln!("ERROR handle_summary() failed: {err}");
    }

    if let Some(failure) = &result.remote_failure {
        eprintln!("ERROR {failure}");
    }

    if result.stopped {
        exit_code::EXTERNAL_ABORT
    } else if result.remote_failure.is_some() {
        exit_code::GENERIC_ENGINE
    } else if let Some(err) = &result.teardown_error {
        hook_exit_code(err)
    } else if summary_error.is_some() {
        exit_code::SCRIPT_EXCEPTION
    } else if result.thresholds_failed() {
        exit_code::THRESHOLDS_FAILED
    } else {
        exit_code::OK
    }
}

/// Writes what `handle_summary()` returned: `stdout`, `stderr`, or a file
/// (creating its directory).
fn write_summary_outputs(outputs: SummaryOutputs) {
    for (target, content) in outputs {
        let written = match target.as_str() {
            "stdout" => std::io::stdout().write_all(&content),
            "stderr" => std::io::stderr().write_all(&content),
            path => {
                let path = Path::new(path);
                match path.parent() {
                    Some(dir) if !dir.as_os_str().is_empty() => std::fs::create_dir_all(dir),
                    _ => Ok(()),
                }
                .and_then(|()| std::fs::write(path, &content))
            }
        };
        if let Err(e) = written {
            eprintln!("WARN could not write the summary to {target}: {e}");
        }
    }
}

/// Prints the first occurrence of each script exception (with its traceback)
/// and request error while the run is live; later ones are counted and listed
/// after the summary.
async fn print_events(
    mut events: broadcast::Receiver<RunEvent>,
    live: bool,
    run_done: CancellationToken,
) {
    let print = |event: RunEvent| {
        if live {
            eprint!("\r\x1b[2K"); // the status line redraws on its next tick
        }
        match event {
            RunEvent::ScriptError(ScriptError { message, details }) => {
                eprintln!("ERROR {}", details.unwrap_or(message));
            }
            RunEvent::RequestError(message) => eprintln!("WARN request failed: {message}"),
        }
    };
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Ok(event) => print(event),
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return,
            },
            _ = run_done.cancelled() => {
                while let Ok(event) = events.try_recv() {
                    print(event);
                }
                return;
            }
        }
    }
}

fn hook_exit_code(err: &HookError) -> i32 {
    match err {
        HookError::Failed { .. } => exit_code::SCRIPT_EXCEPTION,
        HookError::TimedOut { hook: "setup", .. } => exit_code::SETUP_TIMEOUT,
        HookError::TimedOut { .. } => exit_code::TEARDOWN_TIMEOUT,
    }
}

fn threshold_results(result: &RunResult) -> Vec<ThresholdResult> {
    result
        .thresholds
        .iter()
        .filter_map(|t| {
            let selector: Selector = t.selector.parse().ok()?;
            Some(ThresholdResult {
                metric: selector.metric,
                tags: selector.tags,
                expression: t.expression.clone(),
                passed: t.passed,
            })
        })
        .collect()
}

fn report_problems(result: &RunResult) {
    report_errors(
        "ERROR",
        "script raised",
        "script exceptions",
        &result.script_errors,
    );
    report_errors(
        "WARN",
        "request failed",
        "request errors",
        &result.request_errors,
    );
    if let Some(err) = &result.teardown_error {
        eprintln!("ERROR {err}");
    }
    for error in &result.output_errors {
        eprintln!("WARN output failed and was stopped: {error}");
    }
    let mut crossed: Vec<&str> = Vec::new();
    for t in result.thresholds.iter().filter(|t| !t.passed) {
        let metric = t.selector.as_str();
        if !crossed.contains(&metric) {
            crossed.push(metric);
        }
    }
    if crossed.is_empty() {
        return;
    }
    let list = crossed.join(", ");
    if result.aborted_by_threshold {
        eprintln!(
            "ERROR thresholds on metrics '{list}' were crossed; at least one has abortOnFail enabled, stopping test prematurely"
        );
    } else {
        eprintln!("ERROR thresholds on metrics '{list}' have been crossed");
    }
}

/// Lists errors by count; the first few kinds only.
fn report_errors(level: &str, what: &str, kinds: &str, errors: &[(String, u64)]) {
    for (message, count) in errors.iter().take(MAX_ERRORS_SHOWN) {
        eprintln!("{level} {what} {count}×: {message}");
    }
    if errors.len() > MAX_ERRORS_SHOWN {
        eprintln!(
            "{level} … and {} other kinds of {kinds}",
            errors.len() - MAX_ERRORS_SHOWN
        );
    }
}

/// First Ctrl-C: stop the run (interrupting iterations) but still print the
/// summary. Second: exit immediately.
async fn handle_ctrl_c(control: RunControl) {
    if tokio::signal::ctrl_c().await.is_err() {
        return;
    }
    eprintln!("\nstopping the test run; press Ctrl-C again to force exit");
    control.stop();
    if tokio::signal::ctrl_c().await.is_ok() {
        eprintln!("forced exit");
        std::process::exit(exit_code::EXTERNAL_ABORT);
    }
}

async fn show_progress(control: RunControl) {
    let mut progress = control.progress();
    while progress.changed().await.is_ok() {
        let p = *progress.borrow_and_update();
        eprint!(
            "\r\x1b[2K{}",
            status_line(
                p.elapsed,
                p.active_vus,
                p.max_vus,
                p.iterations_complete,
                p.iterations_interrupted
            )
        );
    }
}

/// `--no-color`, or `NO_COLOR` set (<https://no-color.org>).
fn no_color(flags: &RunFlags) -> bool {
    flags.no_color || std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty())
}
