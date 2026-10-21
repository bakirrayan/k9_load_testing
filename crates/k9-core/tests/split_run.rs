//! In one process: a plan run as N segments by N engines,
//! with each engine's metrics sent over the worker protocol and merged by a
//! manager, adds up to exactly what one engine records for the whole plan.

use std::collections::BTreeMap;

use k9_core::engine::{RunControl, RunResult, run_segment};
use k9_core::protocol::{FromWorker, WorkerSummary};
use k9_core::{Format, Plan, Segment, load_plan};
use k9_metrics::{Interner, Registry, Snapshot, Value};
use k9_testserver::TestServer;

fn plan(server: &TestServer) -> Plan {
    let yaml = format!(
        r#"
options:
  tags: {{ env: ci }}
  scenarios:
    shared: {{ executor: shared-iterations, vus: 5, iterations: 101 }}
    each: {{ executor: per-vu-iterations, vus: 4, iterations: 3, exec: each }}
flows:
  default:
    - group:
        name: api
        steps:
          - request:
              url: '{ok}'
              checks: {{ "status is 200": {{ status: 200 }} }}
    - request: {{ url: '{failing}', name: flaky }}
  each:
    - request: {{ url: '{ok}' }}
"#,
        ok = server.url("/ok"),
        failing = server.url("/status/503"),
    );
    load_plan(&yaml, Format::Yaml).unwrap_or_else(|e| panic!("{e}"))
}

/// A comparable view of a snapshot: (metric, tags) → what must add up
/// exactly. Gauges (`vus`) are per worker by design; trend values are timings,
/// so only their counts must match.
fn exact_parts(
    snapshot: &Snapshot,
    registry: &Registry,
    interner: &Interner,
) -> BTreeMap<String, String> {
    snapshot
        .iter()
        .filter_map(|(key, value)| {
            let tags: Vec<String> = interner
                .get(key.tags)
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            let name = format!("{}{{{}}}", registry.def(key.metric).name, tags.join(","));
            let exact = match value {
                Value::Counter(c) => format!("sum {}", c.sum),
                Value::Rate(r) => format!("{}/{}", r.trues, r.total),
                Value::Trend(t) => format!("count {}", t.count),
                Value::Gauge(_) => return None,
            };
            Some((name, exact))
        })
        .collect()
}

fn summary(result: &RunResult) -> WorkerSummary {
    let metrics = result.aggregator.metrics();
    WorkerSummary {
        iterations_complete: result.iterations_complete,
        iterations_interrupted: result.iterations_interrupted,
        script_errors: result.script_errors.clone(),
        request_errors: result.request_errors.clone(),
        metrics: result
            .aggregator
            .totals()
            .to_portable(metrics.registry(), metrics.interner()),
    }
}

#[tokio::test]
async fn segments_merged_over_the_protocol_equal_the_whole_run() {
    let server = TestServer::start().await.unwrap();
    let plan = plan(&server);

    let whole = run_segment(&plan, Segment::full(), RunControl::new(), Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(whole.iterations_complete, 101 + 12);

    // Three "workers", each sending its summary as a protocol message (the
    // wire format itself is tested in k9-grpc).
    let mut sent = Vec::new();
    let handles: Vec<_> = Segment::split(3)
        .into_iter()
        .map(|segment| {
            let plan = plan.clone();
            tokio::spawn(async move {
                run_segment(&plan, segment, RunControl::new(), Vec::new(), None)
                    .await
                    .unwrap()
            })
        })
        .collect();
    for handle in handles {
        let result = handle.await.unwrap();
        sent.push(FromWorker::Done(summary(&result)));
    }

    // The manager: its own registry and interner.
    let (registry, _) = Registry::with_builtins();
    let interner = Interner::new();
    let mut merged = Snapshot::new();
    let mut iterations = 0;
    let mut order = 0;
    for message in sent {
        let FromWorker::Done(done) = message else {
            panic!("unexpected {message:?}")
        };
        iterations += done.iterations_complete;
        order += 1;
        merged.merge(
            &done
                .metrics
                .into_snapshot(&registry, &interner, order)
                .unwrap(),
        );
    }
    assert_eq!(order, 3);
    assert_eq!(iterations, whole.iterations_complete);

    let metrics = whole.aggregator.metrics();
    let expected = exact_parts(
        whole.aggregator.totals(),
        metrics.registry(),
        metrics.interner(),
    );
    let got = exact_parts(&merged, &registry, &interner);
    assert!(expected.len() > 20, "{expected:#?}");
    assert_eq!(got, expected);
}
