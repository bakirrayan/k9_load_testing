//! Snapshots that cross a process boundary: workers send their metrics to the
//! manager by name and tags (their ids are process-local), and what the
//! manager merges equals what one process would have recorded.

use std::sync::Arc;

use k9_metrics::{
    MetricKind, Metrics, PortableError, PortableSnapshot, Registry, Snapshot, TagSetId, Value,
    ValueType,
};
use proptest::prelude::*;

/// Registers the test metrics in a given order (processes may differ).
fn metrics(reversed: bool) -> Arc<Metrics> {
    let mut defs = vec![
        ("reqs", MetricKind::Counter),
        ("depth", MetricKind::Gauge),
        ("ok", MetricKind::Rate),
        ("latency", MetricKind::Trend),
    ];
    if reversed {
        defs.reverse();
    }
    let mut registry = Registry::new();
    for (name, kind) in defs {
        registry.register(name, kind, ValueType::Default).unwrap();
    }
    Arc::new(Metrics::new(registry))
}

/// One recorded sample: (metric, tag value, value).
type SampleSpec = (usize, u8, f64);

fn record(m: &Metrics, (metric, tag, value): SampleSpec) {
    let name = ["reqs", "depth", "ok", "latency"][metric];
    let id = m.registry().get(name).unwrap();
    let tag = format!("t{tag}");
    let tags = if tag == "t0" {
        TagSetId::EMPTY
    } else {
        m.interner()
            .intern(&[("scenario", tag.as_str()), ("group", "::api")])
    };
    m.record(id, tags, value);
}

/// A series by (metric name, tags), with its value.
type Named = (String, Vec<(String, String)>, Value);

/// Every series of `snapshot` by name and tags, sorted, for comparison.
fn by_name(snapshot: &Snapshot, m: &Metrics) -> Vec<Named> {
    let mut out: Vec<_> = snapshot
        .iter()
        .map(|(key, value)| {
            let name = m.registry().def(key.metric).name.clone();
            let tags = m
                .interner()
                .get(key.tags)
                .iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect();
            (name, tags, value.clone())
        })
        .collect();
    out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    out
}

fn assert_same(merged: &Value, single: &Value) {
    match (merged, single) {
        (Value::Trend(a), Value::Trend(b)) => {
            assert_eq!((a.count, a.clamped), (b.count, b.clamped));
            assert!(
                (a.sum - b.sum).abs() <= 1e-6 * b.sum.abs().max(1.0),
                "{} vs {}",
                a.sum,
                b.sum
            );
            assert_eq!((a.min, a.max), (b.min, b.max));
            for p in [50.0, 90.0, 95.0, 99.0] {
                assert_eq!(a.percentile(p), b.percentile(p), "p({p})");
            }
        }
        (Value::Counter(a), Value::Counter(b)) => {
            assert!(
                (a.sum - b.sum).abs() <= 1e-6 * b.sum.abs().max(1.0),
                "{} vs {}",
                a.sum,
                b.sum
            );
        }
        (Value::Gauge(a), Value::Gauge(b)) => assert_eq!((a.min, a.max), (b.min, b.max)),
        (a, b) => assert_eq!(a, b),
    }
}

proptest! {
    #[test]
    fn merged_worker_snapshots_equal_one_process(
        samples in prop::collection::vec(
            ((0usize..4), (0u8..3), (0.0f64..5_000.0), (0usize..3)),
            1..300,
        )
    ) {
        let single = metrics(false);
        let workers = [metrics(false), metrics(true), metrics(false)];
        for &(metric, tag, value, worker) in &samples {
            let value = if metric == 2 { f64::from(value > 2_500.0) } else { value };
            record(&single, (metric, tag, value));
            record(&workers[worker], (metric, tag, value));
        }

        // The manager has its own registry and interner.
        let manager = metrics(true);
        let mut merged = Snapshot::new();
        for (order, worker) in workers.iter().enumerate() {
            let portable = worker.take().to_portable(worker.registry(), worker.interner());
            // What goes over the wire.
            let bytes = postcard::to_stdvec(&portable).unwrap();
            let received: PortableSnapshot = postcard::from_bytes(&bytes).unwrap();
            prop_assert_eq!(&received, &portable);
            let snapshot = received
                .into_snapshot(manager.registry(), manager.interner(), order as u64)
                .unwrap();
            merged.merge(&snapshot);
        }

        let merged = by_name(&merged, &manager);
        let single = by_name(&single.take(), &single);
        prop_assert_eq!(merged.len(), single.len());
        for ((mn, mt, mv), (sn, st, sv)) in merged.iter().zip(&single) {
            prop_assert_eq!((mn, mt), (sn, st));
            assert_same(mv, sv);
        }
    }
}

#[test]
fn gauges_keep_the_value_from_the_latest_snapshot() {
    let (a, b, manager) = (metrics(false), metrics(false), metrics(false));
    record(&a, (1, 0, 7.0));
    record(&b, (1, 0, 3.0));
    let mut merged = Snapshot::new();
    for (order, m) in [(1, &a), (2, &b)] {
        let portable = m.take().to_portable(m.registry(), m.interner());
        merged.merge(
            &portable
                .into_snapshot(manager.registry(), manager.interner(), order)
                .unwrap(),
        );
    }
    let depth = manager.registry().get("depth").unwrap();
    match merged.get(depth, TagSetId::EMPTY) {
        Some(Value::Gauge(g)) => assert_eq!((g.value, g.min, g.max), (3.0, 3.0, 7.0)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn unknown_or_mismatched_metrics_are_rejected() {
    let worker = metrics(false);
    record(&worker, (0, 0, 1.0));
    let portable = worker
        .take()
        .to_portable(worker.registry(), worker.interner());

    let empty = Metrics::new(Registry::new());
    let err = portable
        .clone()
        .into_snapshot(empty.registry(), empty.interner(), 0)
        .unwrap_err();
    assert_eq!(err, PortableError::UnknownMetric("reqs".into()));
    assert_eq!(
        err.to_string(),
        "the worker sent samples for unknown metric `reqs`"
    );

    let mut other = Registry::new();
    other
        .register("reqs", MetricKind::Trend, ValueType::Default)
        .unwrap();
    let other = Metrics::new(other);
    let err = portable
        .into_snapshot(other.registry(), other.interner(), 0)
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "the worker sent a Counter for `reqs`, which is a Trend here"
    );
}
