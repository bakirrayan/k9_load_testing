//! `k9 run` / `k9 inspect` on scripts, through a fake [`ScriptHost`]. The real
//! host (Python) is tested end to end in `tests/python`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use k9_cli::{Function, LoadedScript, ScriptHost, SummaryHook, SummaryOutputs, exit_code};
use k9_core::engine::{BoxFuture, Script, ScriptContext, ScriptError, ScriptVu, VuInfo};

#[derive(Clone, Default)]
struct Behaviour {
    load_error: Option<&'static str>,
    setup_error: bool,
    setup_time: Duration,
    teardown_error: bool,
    teardown_time: Duration,
    sync_default: bool,
    /// `handle_summary()` returns these (target, content) pairs, or fails.
    summary: Option<Result<Vec<(String, String)>, &'static str>>,
}

struct FakeHost {
    options: serde_json::Value,
    behaviour: Behaviour,
    /// The `-e` pairs the host was given.
    env: Mutex<Option<Vec<(String, String)>>>,
    /// What `handle_summary()` was given: (data, default text).
    summary_seen: Arc<Mutex<Option<(serde_json::Value, String)>>>,
}

fn host(options: serde_json::Value, behaviour: Behaviour) -> FakeHost {
    FakeHost {
        options,
        behaviour,
        env: Mutex::new(None),
        summary_seen: Arc::default(),
    }
}

impl ScriptHost for FakeHost {
    fn load(&self, _file: &Path, env: &[(String, String)]) -> Result<LoadedScript, String> {
        *self.env.lock().unwrap() = Some(env.to_vec());
        if let Some(message) = self.behaviour.load_error {
            return Err(message.to_owned());
        }
        Ok(LoadedScript {
            plan: None,
            options: self.options.clone(),
            functions: vec![Function {
                name: "default".into(),
                is_async: !self.behaviour.sync_default,
            }],
            hooks: vec!["setup".into(), "teardown".into()],
            script: Arc::new(FakeScript(self.behaviour.clone())),
            summary_hook: self.behaviour.summary.clone().map(|outcome| {
                Arc::new(FakeSummary {
                    outcome,
                    seen: self.summary_seen.clone(),
                }) as Arc<dyn SummaryHook>
            }),
        })
    }
}

struct FakeScript(Behaviour);

impl Script for FakeScript {
    fn setup(&self, _ctx: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async move {
            tokio::time::sleep(self.0.setup_time).await;
            match self.0.setup_error {
                true => Err(ScriptError::new("KeyError: 'token'")),
                false => Ok(()),
            }
        })
    }

    fn teardown(&self, _ctx: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async move {
            tokio::time::sleep(self.0.teardown_time).await;
            match self.0.teardown_error {
                true => Err(ScriptError::new("OSError: gone")),
                false => Ok(()),
            }
        })
    }

    fn vu(&self, _vu: VuInfo, _ctx: ScriptContext) -> Box<dyn ScriptVu> {
        Box::new(FakeVu)
    }
}

struct FakeVu;

struct FakeSummary {
    outcome: Result<Vec<(String, String)>, &'static str>,
    seen: Arc<Mutex<Option<(serde_json::Value, String)>>>,
}

impl SummaryHook for FakeSummary {
    fn handle_summary(
        &self,
        data: serde_json::Value,
        text: String,
    ) -> BoxFuture<'_, Result<SummaryOutputs, ScriptError>> {
        *self.seen.lock().unwrap() = Some((data, text));
        let outcome = match &self.outcome {
            Ok(outputs) => Ok(outputs
                .iter()
                .map(|(target, content)| (target.clone(), content.clone().into_bytes()))
                .collect()),
            Err(message) => Err(ScriptError::new(*message)),
        };
        Box::pin(async move { outcome })
    }
}

impl ScriptVu for FakeVu {
    fn iteration(&mut self, _function: &str) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async { Ok(()) })
    }
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-script-tests-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn run(host: &FakeHost, args: &[&str]) -> i32 {
    let mut argv = vec!["k9"];
    argv.extend(args);
    k9_cli::run_with_host(argv, host)
}

/// Runs `test.py` with `--summary-export` and returns (exit code, iterations).
fn run_counting(host: &FakeHost, extra: &[&str]) -> (i32, Option<u64>) {
    static RUNS: AtomicU32 = AtomicU32::new(0);
    let export = temp(&format!(
        "summary-{}.json",
        RUNS.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_file(&export);
    let export_arg = export.to_str().unwrap();
    let mut args = vec!["run", "test.py", "--summary-export", export_arg];
    args.extend(extra);
    let code = run(host, &args);
    let iterations = std::fs::read_to_string(&export).ok().map(|json| {
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        json["metrics"]["iterations"]["count"].as_f64().unwrap() as u64
    });
    (code, iterations)
}

#[test]
fn runs_the_script_with_its_options() {
    let host = host(
        serde_json::json!({"vus": 2, "iterations": 6}),
        Behaviour::default(),
    );
    assert_eq!(run_counting(&host, &[]), (exit_code::OK, Some(6)));
}

#[test]
fn flags_beat_the_script_options() {
    let host = host(
        serde_json::json!({"vus": 2, "iterations": 6}),
        Behaviour::default(),
    );
    assert_eq!(run_counting(&host, &["-i", "9"]), (exit_code::OK, Some(9)));
}

#[test]
fn env_flags_are_passed_to_the_host() {
    let host = host(serde_json::Value::Null, Behaviour::default());
    let code = run(
        &host,
        &[
            "inspect",
            "test.py",
            "-e",
            "A=1",
            "--env",
            "URL=http://x/?a=b",
        ],
    );
    assert_eq!(code, exit_code::OK);
    assert_eq!(
        host.env.lock().unwrap().clone().unwrap(),
        [
            ("A".to_owned(), "1".to_owned()),
            ("URL".to_owned(), "http://x/?a=b".to_owned())
        ]
    );
}

#[test]
fn env_flags_must_be_key_value_pairs() {
    let host = host(serde_json::Value::Null, Behaviour::default());
    assert_eq!(run(&host, &["run", "test.py", "-e", "A"]), exit_code::USAGE);
}

#[test]
fn script_exceptions_exit_107() {
    let failing = |behaviour| run_counting(&host(serde_json::Value::Null, behaviour), &[]).0;
    let load_error = Behaviour {
        load_error: Some("Traceback ...\nNameError: name 'x' is not defined"),
        ..Behaviour::default()
    };
    assert_eq!(failing(load_error), exit_code::SCRIPT_EXCEPTION);
    let setup_error = Behaviour {
        setup_error: true,
        ..Behaviour::default()
    };
    assert_eq!(failing(setup_error), exit_code::SCRIPT_EXCEPTION);
    let teardown_error = Behaviour {
        teardown_error: true,
        ..Behaviour::default()
    };
    assert_eq!(failing(teardown_error), exit_code::SCRIPT_EXCEPTION);
}

#[test]
fn a_failing_teardown_still_prints_the_summary() {
    let host = host(
        serde_json::json!({"iterations": 3}),
        Behaviour {
            teardown_error: true,
            ..Behaviour::default()
        },
    );
    assert_eq!(
        run_counting(&host, &[]),
        (exit_code::SCRIPT_EXCEPTION, Some(3))
    );
}

#[test]
fn hook_timeouts_exit_100_and_101() {
    let slow_setup = host(
        serde_json::json!({"setupTimeout": "50ms"}),
        Behaviour {
            setup_time: Duration::from_secs(5),
            ..Behaviour::default()
        },
    );
    assert_eq!(run_counting(&slow_setup, &[]).0, exit_code::SETUP_TIMEOUT);
    let slow_teardown = host(
        serde_json::json!({"teardownTimeout": "50ms"}),
        Behaviour {
            teardown_time: Duration::from_secs(5),
            ..Behaviour::default()
        },
    );
    assert_eq!(
        run_counting(&slow_teardown, &[]).0,
        exit_code::TEARDOWN_TIMEOUT
    );
}

#[test]
fn invalid_script_options_exit_104() {
    let host = host(serde_json::json!({"vus": "many"}), Behaviour::default());
    assert_eq!(run(&host, &["run", "test.py"]), exit_code::INVALID_CONFIG);
    let host = self::host(
        serde_json::json!({"scenarios": {"s": {"executor": "constant-vus", "duration": "1s", "exec": "missing"}}}),
        Behaviour::default(),
    );
    assert_eq!(
        run(&host, &["inspect", "test.py"]),
        exit_code::INVALID_CONFIG
    );
}

#[test]
fn sync_iteration_functions_run() {
    let host = host(
        serde_json::json!({"iterations": 2}),
        Behaviour {
            sync_default: true,
            ..Behaviour::default()
        },
    );
    assert_eq!(run_counting(&host, &[]), (exit_code::OK, Some(2)));
    assert_eq!(run(&host, &["inspect", "test.py"]), exit_code::OK);
}

#[test]
fn the_standalone_binary_points_python_scripts_to_the_package() {
    let out = Command::new(env!("CARGO_BIN_EXE_k9"))
        .args(["run", "test.py"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(exit_code::INVALID_CONFIG));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("pip install k9-load"), "{stderr}");
}

#[test]
fn handle_summary_gets_k6_summary_data_and_its_outputs_are_written() {
    let report = temp("handled/report.json");
    let _ = std::fs::remove_dir_all(report.parent().unwrap());
    let host = host(
        serde_json::json!({"iterations": 4}),
        Behaviour {
            summary: Some(Ok(vec![(
                report.to_str().unwrap().to_owned(),
                "{\"custom\": true}".to_owned(),
            )])),
            ..Behaviour::default()
        },
    );
    assert_eq!(run_counting(&host, &[]), (exit_code::OK, Some(4)));

    let (data, text) = host
        .summary_seen
        .lock()
        .unwrap()
        .clone()
        .expect("hook called");
    assert_eq!(data["metrics"]["iterations"]["type"], "counter");
    assert_eq!(data["metrics"]["iterations"]["values"]["count"], 4.0);
    assert!(data["state"]["testRunDurationMs"].is_number());
    assert!(text.contains("iterations"), "the default summary: {text}");
    assert_eq!(
        std::fs::read_to_string(&report).unwrap(),
        "{\"custom\": true}",
        "parent directories are created"
    );
}

#[test]
fn a_failing_handle_summary_exits_107() {
    let host = host(
        serde_json::json!({"iterations": 1}),
        Behaviour {
            summary: Some(Err("Traceback ...\nKeyError: 'metrics'")),
            ..Behaviour::default()
        },
    );
    assert_eq!(
        run_counting(&host, &[]),
        (exit_code::SCRIPT_EXCEPTION, Some(1)),
        "--summary-export is still written"
    );
}
