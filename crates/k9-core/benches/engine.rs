//! The engine's own cost: whole runs of many short iterations, so the time
//! per iteration is the overhead k9 adds (scheduling, flows, metrics), plus
//! a local server's response time for the flows that send requests.

use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use k9_core::engine::{RunControl, run};
use k9_core::{Format, Plan, load_plan};
use k9_testserver::TestServer;

fn plan(options: &str, flows: &str) -> Plan {
    let yaml = format!("options: {options}\n{flows}");
    load_plan(&yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"))
}

fn engine(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let server = runtime.block_on(TestServer::start()).unwrap();
    let base = server.url("");
    let cases = [
        (
            "empty_flow",
            20_000u64,
            plan("{ vus: 10, iterations: 20000 }", "flows: { default: [] }\n"),
        ),
        (
            "one_request",
            6_400,
            plan(
                "{ vus: 64, iterations: 6400 }",
                &format!("flows:\n  default:\n    - request: {{ url: '{base}/ok' }}\n"),
            ),
        ),
        (
            // Two requests per iteration: templated body, JSON extraction,
            // a templated header and a check.
            "login_flow",
            3_200,
            plan(
                "{ vus: 64, iterations: 3200 }",
                &format!(
                    r#"feeders:
  users: {{ mode: per-vu, rows: [{{ name: a }}, {{ name: b }}] }}
flows:
  default:
    - request:
        method: POST
        url: {base}/login
        body: '{{"username": "{{{{ users.name }}}}", "password": "secret"}}'
        extract: {{ token: {{ json: $.token }} }}
    - request:
        url: {base}/me
        headers: {{ Authorization: 'Bearer {{{{ token }}}}' }}
        checks: {{ ok: {{ status: 200 }} }}
"#
                ),
            ),
        ),
    ];
    let mut group = c.benchmark_group("engine");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    for (name, iterations, plan) in &cases {
        group.throughput(Throughput::Elements(*iterations));
        group.bench_function(*name, |bench| {
            bench.to_async(&runtime).iter(|| async {
                let result = run(plan, RunControl::new()).await.unwrap();
                assert_eq!(result.iterations_complete, *iterations);
                assert!(
                    result.request_errors.is_empty(),
                    "{:?}",
                    result.request_errors
                );
            });
        });
    }
    group.finish();
}

criterion_group!(benches, engine);
criterion_main!(benches);
