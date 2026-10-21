//! k6's TLS options in plans: `tlsAuth` (client certificates from PEM text
//! or files), `tlsVersion` and `tlsCipherSuites`; the `tls_version` tag.

use k9_core::engine::{EngineError, RunControl, RunResult, run};
use k9_core::{Format, Plan, load_plan};
use k9_metrics::Value;
use k9_testserver::TestServer;

fn count(result: &RunResult, name: &str, filter: &[(&str, &str)]) -> f64 {
    let m = result.aggregator.metrics();
    let id = m.registry().get(name).unwrap();
    match result.aggregator.totals().merged(id, filter, m.interner()) {
        Some(Value::Counter(c)) => c.sum,
        Some(Value::Rate(r)) => r.trues as f64,
        None => 0.0,
        other => panic!("{other:?}"),
    }
}

/// `MTLS`/`HTTPS` → the test server's URLs; files relative to `dir`.
fn plan(yaml: &str, server: &TestServer, dir: &std::path::Path) -> Plan {
    let yaml = yaml.replace("MTLS", &server.mtls_url("")).replace(
        "HTTPS",
        &format!("https://localhost:{}", server.https_addr().port()),
    );
    let mut plan = load_plan(&yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"));
    plan.rebase_files(dir);
    plan
}

fn dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-tls-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn plans_present_client_certificates_and_limit_tls() {
    let server = TestServer::start().await.unwrap();
    let dir = dir("ok");
    let id = server.client_identity();
    std::fs::write(dir.join("client.pem"), &id.cert_pem).unwrap();
    // The key inline, as k6 scripts pass it; the certificate from a file.
    let key = id.key_pem.replace('\n', "\\n");
    let yaml = format!(
        r#"
options:
  iterations: 2
  insecureSkipTLSVerify: true
  tlsAuth:
    - {{ domains: [localhost], cert: client.pem, key: "{key}" }}
  tlsVersion: {{ min: tls1.2, max: tls1.2 }}
  tlsCipherSuites: [TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256]
flows:
  default:
    - request: {{ url: 'MTLS/ok', checks: {{ mtls: {{ status: 200 }} }} }}
    - request: {{ url: 'HTTPS/ok', checks: {{ https: {{ status: 200 }} }} }}
"#
    );
    let result = run(&plan(&yaml, &server, &dir), RunControl::new())
        .await
        .unwrap();
    assert!(
        result.request_errors.is_empty(),
        "{:?}",
        result.request_errors
    );
    assert_eq!(count(&result, "checks", &[("check", "mtls")]), 2.0);
    assert_eq!(count(&result, "checks", &[("check", "https")]), 2.0);
    assert_eq!(
        count(&result, "http_reqs", &[("tls_version", "tls1.2")]),
        4.0
    );
}

#[tokio::test]
async fn tls_mistakes_are_reported_before_the_run_starts() {
    let server = TestServer::start().await.unwrap();
    let dir = dir("bad");
    for (options, expected) in [
        ("tlsVersion: tls1.0", "tls1.0 isn't supported"),
        (
            "tlsVersion: { min: tls1.3, max: tls1.2 }",
            "min tls1.3 is above max tls1.2",
        ),
        ("tlsCipherSuites: [TLS_NOPE]", "TLS_NOPE"),
        (
            "tlsAuth: [{ cert: missing.pem, key: missing.pem }]",
            "missing.pem",
        ),
        (
            "tlsAuth: [{ cert: a, key: b, password: secret }]",
            "encrypted",
        ),
    ] {
        let yaml = format!(
            "options: {{ iterations: 1, {options} }}\nflows:\n  default:\n    - request: {{ url: 'MTLS/ok' }}\n"
        );
        let err = match load_plan(&yaml.replace("MTLS", &server.mtls_url("")), Format::Yaml) {
            Err(err) => err.to_string(),
            Ok(mut plan) => {
                plan.rebase_files(&dir);
                match run(&plan, RunControl::new()).await {
                    Err(err @ EngineError::Client(_)) => err.to_string(),
                    other => panic!("{options}: {:?}", other.err()),
                }
            }
        };
        assert!(err.contains(expected), "{options}: {err}");
    }
}
