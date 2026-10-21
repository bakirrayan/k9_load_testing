//! Option precedence through the real binary: flags > K9_* env > plan > defaults.

use std::path::PathBuf;
use std::process::{Command, Output};

fn write_plan(name: &str, yaml: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-options-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.yaml"));
    std::fs::write(&path, yaml).unwrap();
    path
}

const PLAN: &str = "options: { vus: 2, duration: 30s }\nflows:\n  default:\n    - request: { url: 'http://127.0.0.1:1/' }\n";
const SCENARIOS: &str = "options:\n  scenarios:\n    a: { executor: constant-vus, duration: 1h }\nflows:\n  default:\n    - request: { url: 'http://127.0.0.1:1/' }\n";

fn inspect(plan: &PathBuf, args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_k9"));
    cmd.arg("inspect").arg(plan).args(args);
    // Keep the test independent of the developer's own K9_* variables.
    for (key, _) in std::env::vars().filter(|(k, _)| k.starts_with("K9_")) {
        cmd.env_remove(key);
    }
    cmd.envs(env.iter().copied()).output().unwrap()
}

fn default_scenario(out: &Output) -> serde_json::Value {
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    json["plan"]["options"]["scenarios"]["default"].clone()
}

#[test]
fn flags_override_the_plan() {
    let plan = write_plan("flags", PLAN);
    let s = default_scenario(&inspect(&plan, &["-u", "7", "--duration", "3s"], &[]));
    assert_eq!(
        (s["executor"].as_str(), s["vus"].as_u64()),
        (Some("constant-vus"), Some(7))
    );
    assert_eq!(s["duration"], "3s");
}

#[test]
fn environment_overrides_the_plan_and_flags_override_the_environment() {
    let plan = write_plan("env", PLAN);
    let s = default_scenario(&inspect(&plan, &[], &[("K9_DURATION", "45s")]));
    assert_eq!(
        (s["vus"].as_u64(), s["duration"].as_str()),
        (Some(2), Some("45s"))
    );

    let s = default_scenario(&inspect(&plan, &["-d", "5s"], &[("K9_DURATION", "45s")]));
    assert_eq!(s["duration"], "5s");
}

#[test]
fn stage_flags_select_ramping_vus() {
    let plan = write_plan("stages", SCENARIOS);
    let s = default_scenario(&inspect(&plan, &["--stage", "10s:5", "-s", "20s:0"], &[]));
    assert_eq!(s["executor"], "ramping-vus");
    assert_eq!(s["stages"][1]["target"], 0);
}

#[test]
fn http_flags_set_http_options() {
    let plan = write_plan("http", PLAN);
    let out = inspect(
        &plan,
        &[
            "--insecure-skip-tls-verify",
            "--max-redirects",
            "2",
            "--user-agent",
            "probe/1",
            "--http-version",
            "2",
            "--batch",
            "8",
        ],
        &[("K9_BATCH_PER_HOST", "3")],
    );
    assert_eq!(out.status.code(), Some(0));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let options = &json["plan"]["options"];
    assert_eq!(options["insecureSkipTLSVerify"], true);
    assert_eq!(options["maxRedirects"], 2);
    assert_eq!(options["userAgent"], "probe/1");
    assert_eq!(options["httpVersion"], "2");
    assert_eq!(
        (&options["batch"], &options["batchPerHost"]),
        (&8.into(), &3.into())
    );
}

#[test]
fn invalid_overrides_exit_104() {
    let scenarios = write_plan("vus-alone", SCENARIOS);
    let out = inspect(&scenarios, &["-u", "5"], &[]);
    assert_eq!(out.status.code(), Some(104));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--vus"));

    let plan = write_plan("bad-env", PLAN);
    let out = inspect(&plan, &[], &[("K9_VUS", "lots")]);
    assert_eq!(out.status.code(), Some(104));
    assert!(String::from_utf8_lossy(&out.stderr).contains("K9_VUS"));

    let out = inspect(&plan, &["--duration", "soon"], &[]);
    assert_eq!(out.status.code(), Some(2), "clap usage error");
}

#[test]
fn network_options_come_from_flags_and_env() {
    let plan = write_plan("network", PLAN);
    let out = inspect(
        &plan,
        &[
            "--dns",
            "ttl=0,select=roundRobin",
            "--block-hostnames",
            "*.internal,admin.test",
            "--blacklist-ip",
            "10.0.0.0/8",
            "--blacklist-ip",
            "192.168.0.1-192.168.0.9",
            "--no-vu-connection-reuse",
        ],
        &[
            ("K9_LOCAL_IPS", "127.0.0.2,127.0.0.3"),
            ("K9_NO_CONNECTION_REUSE", "true"),
            ("K9_DNS", "policy=onlyIPv6"),
        ],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let options = &json["plan"]["options"];
    assert_eq!(
        options["dns"],
        serde_json::json!({ "ttl": "0", "select": "roundRobin", "policy": "preferIPv4" }),
        "the flag replaces the variable"
    );
    assert_eq!(
        options["blockHostnames"],
        serde_json::json!(["*.internal", "admin.test"])
    );
    assert_eq!(
        options["blacklistIPs"],
        serde_json::json!(["10.0.0.0/8", "192.168.0.1-192.168.0.9"])
    );
    assert_eq!(
        options["localIPs"],
        serde_json::json!(["127.0.0.2", "127.0.0.3"])
    );
    assert_eq!(options["noConnectionReuse"], true);
    assert_eq!(options["noVUConnectionReuse"], true);

    let out = inspect(&plan, &["--dns", "select=sometimes"], &[]);
    assert_eq!(out.status.code(), Some(104));
    assert!(String::from_utf8_lossy(&out.stderr).contains("options.dns"));
}

#[test]
fn run_options_come_from_flags_and_env() {
    let plan = write_plan("run-options", PLAN);
    let out = inspect(
        &plan,
        &[
            "--min-iteration-duration",
            "1.5s",
            "-w",
            "--system-tags",
            "status,check",
            "--summary-time-unit",
            "ms",
        ],
        &[("K9_NO_COOKIES_RESET", "1"), ("K9_SYSTEM_TAGS", "url")],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let options = &json["plan"]["options"];
    assert_eq!(options["minIterationDuration"], "1.5s");
    assert_eq!(options["throw"], true);
    assert_eq!(
        options["systemTags"],
        serde_json::json!(["status", "check"])
    );
    assert_eq!(options["summaryTimeUnit"], "ms");
    assert_eq!(options["noCookiesReset"], true);

    for (args, env, expected) in [
        (&["--http-debug"][..], &[][..], "headers"),
        (&["--http-debug=full"], &[], "full"),
        (&[], &[("K9_HTTP_DEBUG", "full")], "full"),
    ] {
        let out = inspect(&plan, args, env);
        let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(
            json["plan"]["options"]["httpDebug"], expected,
            "{args:?} {env:?}"
        );
    }

    let out = inspect(&plan, &[], &[("K9_SUMMARY_TIME_UNIT", "minutes")]);
    assert_eq!(out.status.code(), Some(104));
    assert!(String::from_utf8_lossy(&out.stderr).contains("options.summaryTimeUnit"));
}
