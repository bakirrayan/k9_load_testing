use k9_core::plan::{Executor, Step, Threshold};
use k9_core::{Duration, Format, LoadError, Plan, load_plan};

fn yaml(src: &str) -> Plan {
    load_plan(src, Format::Yaml).unwrap_or_else(|e| panic!("{e}"))
}

fn yaml_err(src: &str) -> String {
    match load_plan(src, Format::Yaml) {
        Ok(plan) => panic!("expected an error, got {plan:#?}"),
        Err(e) => e.to_string(),
    }
}

const FLOW: &str = "flows:\n  default:\n    - request: { url: 'http://localhost/' }\n";

#[test]
fn example_plan_resolves() {
    let src = include_str!("../../../examples/basic.yaml");
    let plan = yaml(src);
    assert_eq!(
        plan.options.scenarios.keys().collect::<Vec<_>>(),
        ["browse", "checkout"]
    );
    let checkout = &plan.options.scenarios["checkout"];
    assert_eq!(checkout.exec, "checkout");
    assert_eq!(checkout.start_time, Duration::from_secs(30));
    assert_eq!(checkout.graceful_stop, Duration::from_secs(30), "default");
    assert!(matches!(
        checkout.executor,
        Executor::ConstantArrivalRate {
            rate: 5,
            pre_allocated_vus: 10,
            max_vus: 50,
            ..
        }
    ));
    assert!(matches!(
        &plan.options.thresholds["http_req_failed"][0],
        Threshold::Detailed {
            abort_on_fail: true,
            ..
        }
    ));
    assert!(matches!(plan.flows["default"][2], Step::Group { .. }));
}

#[test]
fn every_executor_round_trips_through_json() {
    let src = format!(
        r#"
options:
  scenarios:
    a: {{ executor: constant-vus, vus: 5, duration: 30s }}
    b: {{ executor: ramping-vus, startVUs: 2, stages: [{{ duration: 10s, target: 10 }}] }}
    c: {{ executor: per-vu-iterations, vus: 3, iterations: 7 }}
    d: {{ executor: shared-iterations, vus: 3, iterations: 100, maxDuration: 1m }}
    e: {{ executor: constant-arrival-rate, rate: 50, duration: 1m, preAllocatedVUs: 20 }}
    f: {{ executor: ramping-arrival-rate, startRate: 1, timeUnit: 1m, preAllocatedVUs: 5, maxVUs: 9, stages: [{{ duration: 1m, target: 60 }}] }}
    g: {{ executor: externally-controlled, vus: 1, maxVUs: 10, duration: 10m }}
{FLOW}"#
    );
    let plan = yaml(&src);
    let kinds: Vec<_> = plan
        .options
        .scenarios
        .values()
        .map(|s| s.executor.kind().name())
        .collect();
    assert_eq!(
        kinds,
        [
            "constant-vus",
            "ramping-vus",
            "per-vu-iterations",
            "shared-iterations",
            "constant-arrival-rate",
            "ramping-arrival-rate",
            "externally-controlled"
        ]
    );

    // The resolved plan is also what workers will receive, so it must round-trip.
    let json = serde_json::to_string(&plan).unwrap();
    let back: Plan = serde_json::from_str(&json).unwrap();
    assert_eq!(back, plan);

    // k6 spelling survives serialization.
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    let e = &value["options"]["scenarios"]["e"];
    assert_eq!(e["executor"], "constant-arrival-rate");
    assert_eq!(e["preAllocatedVUs"], 20);
    assert_eq!(e["maxVUs"], 20, "maxVUs defaults to preAllocatedVUs");
    assert_eq!(e["timeUnit"], "1s");
    assert_eq!(value["options"]["scenarios"]["b"]["startVUs"], 2);
}

#[test]
fn toml_and_json_produce_the_same_plan() {
    let from_yaml = yaml(&format!("options: {{ vus: 10, duration: 30s }}\n{FLOW}"));
    let from_toml = load_plan(
        "[options]\nvus = 10\nduration = \"30s\"\n\n[[flows.default]]\nrequest = { url = \"http://localhost/\" }\n",
        Format::Toml,
    )
    .unwrap();
    let from_json = load_plan(
        r#"{"options": {"vus": 10, "duration": 30000},
            "flows": {"default": [{"request": {"url": "http://localhost/"}}]}}"#,
        Format::Json,
    )
    .unwrap();
    assert_eq!(from_yaml, from_toml);
    assert_eq!(from_yaml, from_json);
}

#[test]
fn shortcuts_expand_like_k6() {
    let exec = |opts: &str| {
        let plan = yaml(&format!("options: {opts}\n{FLOW}"));
        assert_eq!(
            plan.options.scenarios.keys().collect::<Vec<_>>(),
            ["default"]
        );
        plan.options.scenarios["default"].executor.clone()
    };
    assert_eq!(
        exec("{ vus: 10, duration: 30s }"),
        Executor::ConstantVus {
            vus: 10,
            duration: Duration::from_secs(30)
        }
    );
    assert!(matches!(
        exec("{ vus: 2, iterations: 50 }"),
        Executor::SharedIterations {
            vus: 2,
            iterations: 50,
            ..
        }
    ));
    assert!(matches!(
        exec("{ stages: [{ duration: 10s, target: 5 }] }"),
        Executor::RampingVus { start_vus: 1, .. }
    ));
    assert!(matches!(
        exec("{}"),
        Executor::SharedIterations {
            vus: 1,
            iterations: 1,
            ..
        }
    ));
}

#[test]
fn reports_all_problems_with_paths() {
    let msg = yaml_err(
        r#"
options:
  vus: 3
  scenarios:
    "bad name": { executor: constant-vus, duration: 10s }
    fast: { executor: constant-vus, rate: 5 }
    rate: { executor: constant-arrival-rate, duration: 1m }
    shared: { executor: shared-iterations, vus: 10, iterations: 5 }
    typo: { executor: constant-vu }
    other: { executor: constant-vus, duration: 5s, exec: nope }
flows:
  default:
    - request: { url: "" }
"#,
    );
    for expected in [
        "can't be combined with `scenarios`",
        "options.scenarios.bad name: scenario names may only contain",
        "options.scenarios.fast.rate: not valid for constant-vus",
        "options.scenarios.fast.duration: required for constant-vus",
        "options.scenarios.rate.rate: required for constant-arrival-rate",
        "options.scenarios.rate.preAllocatedVUs: required",
        "options.scenarios.shared.iterations: must be at least `vus`",
        "options.scenarios.typo.executor: unknown executor `constant-vu`",
        "options.scenarios.other.exec: no flow named `nope` (defined flows: `default`)",
        "flows.default[0].url: must not be empty",
    ] {
        assert!(msg.contains(expected), "missing {expected:?} in:\n{msg}");
    }
    assert!(
        !msg.contains("fast.duration: must be"),
        "a missing field should not also be reported as out of range:\n{msg}"
    );
}

#[test]
fn unknown_keys_are_rejected() {
    let err = load_plan(&format!("options:\n  vu: 10\n{FLOW}"), Format::Yaml).unwrap_err();
    assert!(matches!(err, LoadError::Parse { .. }));
    assert!(err.to_string().contains("vu"), "{err}");

    let err = yaml_err(&format!(
        "options:\n  scenarios:\n    s: {{ executor: constant-vus, duration: 1s, preAllocatedVus: 1 }}\n{FLOW}"
    ));
    assert!(
        err.contains("preAllocatedVus"),
        "wrong capitalization should be caught: {err}"
    );
}

#[test]
fn methods_accept_lower_case() {
    let plan = yaml("flows:\n  default:\n    - request: { method: post, url: 'http://x/' }\n");
    let Step::Request(req) = &plan.flows["default"][0] else {
        panic!()
    };
    assert_eq!(serde_json::to_value(req.method).unwrap(), "POST");
}

#[test]
fn scenarios_describe_themselves_like_k6() {
    let plan = yaml(&format!(
        r#"
options:
  scenarios:
    a: {{ executor: constant-vus, vus: 10, duration: 30s }}
    b: {{ executor: shared-iterations, vus: 2, iterations: 50 }}
    c: {{ executor: per-vu-iterations, vus: 3, iterations: 4, exec: other, startTime: 5s }}
    d: {{ executor: ramping-vus, stages: [{{ duration: 30s, target: 20 }}, {{ duration: 15s, target: 0 }}] }}
    e: {{ executor: constant-arrival-rate, rate: 5, duration: 1m, preAllocatedVUs: 10, maxVUs: 50 }}
{FLOW}  other:
    - sleep: 1s
"#
    ));
    let d = |name: &str| plan.options.scenarios[name].describe();
    assert_eq!(d("a"), "10 looping VUs for 30s (gracefulStop: 30s)");
    assert_eq!(
        d("b"),
        "50 iterations shared among 2 VUs (maxDuration: 10m, gracefulStop: 30s)"
    );
    assert_eq!(
        d("c"),
        "4 iterations for each of 3 VUs (maxDuration: 10m, exec: other, startTime: 5s, gracefulStop: 30s)"
    );
    assert_eq!(
        d("d"),
        "Up to 20 looping VUs for 45s over 2 stages (gracefulRampDown: 30s, gracefulStop: 30s)"
    );
    assert_eq!(
        d("e"),
        "5.00 iterations/s for 1m (maxVUs: 10-50, gracefulStop: 30s)"
    );
}

#[test]
fn max_duration_includes_start_time_and_graceful_stops() {
    let plan = yaml(&format!(
        r#"
options:
  scenarios:
    short: {{ executor: constant-vus, duration: 10s }}
    late: {{ executor: constant-vus, duration: 20s, startTime: 15s, gracefulStop: 5s }}
    ramp: {{ executor: ramping-vus, stages: [{{ duration: 30s, target: 5 }}] }}
{FLOW}"#
    ));
    let s = &plan.options.scenarios;
    assert_eq!(s["short"].max_duration(), Duration::from_secs(40));
    assert_eq!(s["late"].max_duration(), Duration::from_secs(40));
    assert_eq!(
        s["ramp"].max_duration(),
        Duration::from_secs(90),
        "includes gracefulRampDown"
    );
    assert_eq!(plan.max_duration(), Duration::from_secs(90));
}

// --- overrides (CLI flags and K9_* environment variables) --------------------

use k9_core::Overrides;

fn with_overrides(src: &str, o: &Overrides) -> Result<Plan, String> {
    let mut config = k9_core::PlanConfig::parse(src, Format::Yaml).unwrap();
    config.apply(o).map_err(|e| e.to_string())?;
    config.resolve().map_err(|e| e.to_string())
}

fn default_executor(plan: &Plan) -> &Executor {
    assert_eq!(
        plan.options.scenarios.keys().collect::<Vec<_>>(),
        ["default"]
    );
    &plan.options.scenarios["default"].executor
}

#[test]
fn flags_override_plan_shortcuts_field_by_field() {
    let plan = with_overrides(
        &format!("options: {{ vus: 10, duration: 30s }}\n{FLOW}"),
        &Overrides {
            duration: Some(Duration::from_secs(60)),
            ..Overrides::default()
        },
    )
    .unwrap();
    assert_eq!(
        default_executor(&plan),
        &Executor::ConstantVus {
            vus: 10,
            duration: Duration::from_secs(60)
        }
    );
}

#[test]
fn execution_flags_replace_plan_scenarios() {
    let plan = with_overrides(
        &format!(
            "options:\n  scenarios:\n    a: {{ executor: constant-vus, duration: 1h }}\n{FLOW}"
        ),
        &Overrides {
            vus: Some(3),
            iterations: Some(9),
            ..Overrides::default()
        },
    )
    .unwrap();
    assert!(matches!(
        default_executor(&plan),
        Executor::SharedIterations {
            vus: 3,
            iterations: 9,
            ..
        }
    ));
}

#[test]
fn a_duration_flag_drops_plan_iterations_and_stages() {
    let plan = with_overrides(
        &format!("options: {{ vus: 2, iterations: 100 }}\n{FLOW}"),
        &Overrides {
            duration: Some(Duration::from_secs(5)),
            ..Overrides::default()
        },
    )
    .unwrap();
    assert_eq!(
        default_executor(&plan),
        &Executor::ConstantVus {
            vus: 2,
            duration: Duration::from_secs(5)
        }
    );
}

#[test]
fn vus_alone_cannot_replace_scenarios() {
    let err = with_overrides(
        &format!(
            "options:\n  scenarios:\n    a: {{ executor: constant-vus, duration: 1s }}\n{FLOW}"
        ),
        &Overrides {
            vus: Some(5),
            ..Overrides::default()
        },
    )
    .unwrap_err();
    assert!(err.contains("--vus"), "{err}");
    assert!(err.contains("--duration"), "{err}");

    // Without scenarios it just sets the VU count.
    let plan = with_overrides(
        &format!("options: {{ duration: 1s }}\n{FLOW}"),
        &Overrides {
            vus: Some(5),
            ..Overrides::default()
        },
    )
    .unwrap();
    assert!(matches!(
        default_executor(&plan),
        Executor::ConstantVus { vus: 5, .. }
    ));
}

#[test]
fn stage_flags_parse_k6_syntax() {
    let stages: Vec<k9_core::plan::Stage> = ["30s:10", "1m:0"]
        .iter()
        .map(|s| s.parse().unwrap())
        .collect();
    assert_eq!(stages[0].duration, Duration::from_secs(30));
    assert_eq!(stages[1].target, 0);
    for bad in ["30s", "x:1", "30s:-1", ":5"] {
        assert!(bad.parse::<k9_core::plan::Stage>().is_err(), "{bad}");
    }
}

#[test]
fn environment_variables_are_read_and_validated() {
    let env = |pairs: &[(&str, &str)]| {
        Overrides::from_env(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())))
    };
    let o = env(&[
        ("K9_VUS", "4"),
        ("K9_DURATION", "2m"),
        ("K9_STAGES", "10s:5,20s:0"),
        ("K9_INSECURE_SKIP_TLS_VERIFY", "true"),
        ("K9_MAX_REDIRECTS", "3"),
        ("K9_USER_AGENT", "probe/1"),
        ("K9_DISCARD_RESPONSE_BODIES", "1"),
        ("UNRELATED", "x"),
    ])
    .unwrap();
    assert_eq!(o.vus, Some(4));
    assert_eq!(o.duration, Some(Duration::from_secs(120)));
    assert_eq!(o.stages.as_ref().map(Vec::len), Some(2));
    assert_eq!(o.insecure_skip_tls_verify, Some(true));
    assert_eq!(o.max_redirects, Some(3));
    assert_eq!(o.user_agent.as_deref(), Some("probe/1"));
    assert_eq!(o.discard_response_bodies, Some(true));

    let err = env(&[("K9_VUS", "many"), ("K9_DURATION", "soon")])
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("K9_VUS") && err.contains("K9_DURATION"),
        "{err}"
    );
}

#[test]
fn higher_layers_win() {
    let cli = Overrides {
        vus: Some(1),
        ..Overrides::default()
    };
    let env = Overrides {
        vus: Some(9),
        max_redirects: Some(2),
        ..Overrides::default()
    };
    let merged = cli.or(env);
    assert_eq!((merged.vus, merged.max_redirects), (Some(1), Some(2)));
}

#[test]
fn http_options_use_k6_names_and_defaults() {
    let plan = yaml(&format!("options: {{}}\n{FLOW}"));
    let http = &plan.options.http;
    assert_eq!(http.max_redirects, 10);
    assert!(!http.insecure_skip_tls_verify);
    assert!(!http.discard_response_bodies);
    assert!(http.user_agent.starts_with("k9/"));

    let plan = yaml(&format!(
        "options: {{ insecureSkipTLSVerify: true, maxRedirects: 0, userAgent: '', discardResponseBodies: true }}\n{FLOW}"
    ));
    let http = &plan.options.http;
    assert!(http.insecure_skip_tls_verify && http.discard_response_bodies);
    assert_eq!((http.max_redirects, http.user_agent.as_str()), (0, ""));

    let overridden = with_overrides(
        &format!("options: {{ maxRedirects: 5 }}\n{FLOW}"),
        &Overrides {
            max_redirects: Some(1),
            ..Overrides::default()
        },
    )
    .unwrap();
    assert_eq!(overridden.options.http.max_redirects, 1);

    let json = serde_json::to_value(&plan.options).unwrap();
    assert_eq!(json["insecureSkipTLSVerify"], true, "k6 spelling: {json}");
}

#[test]
fn http_version_is_auto_unless_the_plan_says_1_1_2_or_3() {
    use k9_core::plan::HttpVersion;
    let plan = yaml(&format!("options: {{}}\n{FLOW}"));
    assert_eq!(plan.options.http.http_version, HttpVersion::Auto);
    for (text, version) in [
        ("'1.1'", HttpVersion::Http1),
        ("1.1", HttpVersion::Http1),
        ("'2'", HttpVersion::Http2),
        ("2", HttpVersion::Http2),
        ("'3'", HttpVersion::Http3),
        ("3", HttpVersion::Http3),
        ("'3'", HttpVersion::Http3),
        ("3", HttpVersion::Http3),
        ("auto", HttpVersion::Auto),
    ] {
        let plan = yaml(&format!("options: {{ httpVersion: {text} }}\n{FLOW}"));
        assert_eq!(plan.options.http.http_version, version, "{text}");
        let json = serde_json::to_value(&plan.options).unwrap();
        assert_eq!(json["httpVersion"], text.trim_matches('\''), "round trip");
    }
    let err = load_plan(
        &format!("options: {{ httpVersion: '4' }}\n{FLOW}"),
        Format::Yaml,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("httpVersion") || err.contains("auto"), "{err}");

    let env = Overrides::from_env([("K9_HTTP_VERSION".to_owned(), "1.1".to_owned())]).unwrap();
    let overridden =
        with_overrides(&format!("options: {{ httpVersion: '2' }}\n{FLOW}"), &env).unwrap();
    assert_eq!(overridden.options.http.http_version, HttpVersion::Http1);
    assert!(Overrides::from_env([("K9_HTTP_VERSION".to_owned(), "9".to_owned())]).is_err());
}

// --- checks and threshold syntax ----------------------------------

#[test]
fn requests_carry_native_checks() {
    let plan = yaml(
        r#"
flows:
  default:
    - request:
        url: 'http://x/'
        checks:
          "status is 200": { status: 200 }
          "created or accepted": { status: [201, 202] }
          "says hello": { bodyContains: hello }
          "fast": { durationBelow: 200ms }
"#,
    );
    let Step::Request(req) = &plan.flows["default"][0] else {
        panic!()
    };
    use k9_core::plan::{Check, StatusCheck};
    assert_eq!(req.checks.len(), 4);
    assert_eq!(
        req.checks["status is 200"],
        Check::Status(StatusCheck::One(200))
    );
    assert_eq!(
        req.checks["created or accepted"],
        Check::Status(StatusCheck::Any(vec![201, 202]))
    );
    assert_eq!(
        req.checks["says hello"],
        Check::BodyContains("hello".into())
    );
    assert_eq!(
        req.checks["fast"],
        Check::DurationBelow(Duration::from_millis(200))
    );

    let json = serde_json::to_value(&plan).unwrap();
    let back: Plan = serde_json::from_value(json).unwrap();
    assert_eq!(back, plan);
}

#[test]
fn unknown_check_kinds_are_rejected() {
    let err = load_plan(
        "flows:\n  default:\n    - request: { url: 'http://x/', checks: { c: { statusIs: 200 } } }\n",
        Format::Yaml,
    )
    .unwrap_err();
    assert!(err.to_string().contains("statusIs"), "{err}");
}

#[test]
fn threshold_syntax_is_validated_with_paths() {
    let msg = yaml_err(&format!(
        r#"
options:
  thresholds:
    http_req_duration: ["p(95)<500", "p95<500"]
    "http_reqs{{status": ["count>1"]
    checks: [{{ threshold: "rate>0.9", abortOnFail: true }}, {{ threshold: "rate>" }}]
{FLOW}"#
    ));
    for expected in [
        "options.thresholds.http_req_duration[1]: invalid threshold \"p95<500\"",
        "options.thresholds.http_reqs{status: invalid metric selector",
        "options.thresholds.checks[1]: invalid threshold \"rate>\"",
    ] {
        assert!(msg.contains(expected), "missing {expected:?} in:\n{msg}");
    }
    assert!(
        !msg.contains("p(95)<500\""),
        "valid ones aren't reported:\n{msg}"
    );
}

// --- Python scripts ------------------------------------------------------------

fn script_options(json: &str) -> k9_core::PlanConfig {
    k9_core::PlanConfig {
        options: serde_json::from_str(json).unwrap_or_else(|e| panic!("{e}")),
        ..Default::default()
    }
}

#[test]
fn script_scenarios_exec_functions_instead_of_flows() {
    let plan = script_options(
        r#"{"scenarios": {
            "home": {"executor": "constant-vus", "vus": 2, "duration": "5s"},
            "buy": {"executor": "shared-iterations", "exec": "checkout"}}}"#,
    )
    .resolve_script(&["default", "checkout"])
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(plan.options.scenarios["home"].exec, "default");
    assert_eq!(plan.options.scenarios["buy"].exec, "checkout");
    assert!(plan.flows.is_empty());
}

#[test]
fn script_exec_must_name_a_function() {
    let err = script_options(r#"{"vus": 2, "duration": "5s"}"#)
        .resolve_script(&["checkout"])
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "options.scenarios.default.exec: the script has no function `default` \
             (functions it defines: `checkout`)"
        ),
        "{err}"
    );
}

#[test]
fn setup_and_teardown_timeouts_default_to_60s() {
    let plan = script_options("{}").resolve_script(&["default"]).unwrap();
    assert_eq!(plan.options.setup_timeout, Duration::from_secs(60));
    assert_eq!(plan.options.teardown_timeout, Duration::from_secs(60));

    let plan = script_options(r#"{"setupTimeout": "5s", "teardownTimeout": 250}"#)
        .resolve_script(&["default"])
        .unwrap();
    assert_eq!(plan.options.setup_timeout, Duration::from_secs(5));
    assert_eq!(plan.options.teardown_timeout, Duration::from_millis(250));
}
