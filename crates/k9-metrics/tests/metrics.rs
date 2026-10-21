use std::sync::Arc;
use std::time::Duration;

use k9_metrics::{
    Aggregator, MetricError, MetricKind, Metrics, Registry, Stats, TagSetId, TrendValue, Value,
    ValueType,
};

fn trend(v: Option<&Value>) -> &TrendValue {
    match v {
        Some(Value::Trend(t)) => t,
        other => panic!("expected a trend, got {other:?}"),
    }
}

// --- registry ---------------------------------------------------------------

#[test]
fn builtins_use_k6_names_and_kinds() {
    let (registry, b) = Registry::with_builtins();
    let check = |name: &str, kind: MetricKind, vt: ValueType| {
        let id = registry
            .get(name)
            .unwrap_or_else(|| panic!("{name} missing"));
        let def = registry.def(id);
        assert_eq!((def.kind, def.value_type), (kind, vt), "{name}");
    };
    check("http_reqs", MetricKind::Counter, ValueType::Default);
    check("http_req_duration", MetricKind::Trend, ValueType::Time);
    check(
        "http_req_tls_handshaking",
        MetricKind::Trend,
        ValueType::Time,
    );
    check("http_req_failed", MetricKind::Rate, ValueType::Default);
    check("checks", MetricKind::Rate, ValueType::Default);
    check("vus", MetricKind::Gauge, ValueType::Default);
    check("data_received", MetricKind::Counter, ValueType::Data);
    check("iteration_duration", MetricKind::Trend, ValueType::Time);
    check(
        "dropped_iterations",
        MetricKind::Counter,
        ValueType::Default,
    );
    assert_eq!(registry.def(b.http_req_waiting).name, "http_req_waiting");
    // k6's 18, plus k9's coordinated-omission-corrected iteration time.
    check(
        "iteration_duration_corrected",
        MetricKind::Trend,
        ValueType::Time,
    );
    // k6's 6 WebSocket metrics.
    check("ws_sessions", MetricKind::Counter, ValueType::Default);
    check("ws_connecting", MetricKind::Trend, ValueType::Time);
    check("ws_session_duration", MetricKind::Trend, ValueType::Time);
    check("ws_msgs_sent", MetricKind::Counter, ValueType::Default);
    check("ws_msgs_received", MetricKind::Counter, ValueType::Default);
    check("ws_ping", MetricKind::Trend, ValueType::Time);
    // k6's unary gRPC metric.
    check("grpc_req_duration", MetricKind::Trend, ValueType::Time);
    // k6's gRPC stream metrics.
    check("grpc_streams", MetricKind::Counter, ValueType::Default);
    check(
        "grpc_streams_msgs_sent",
        MetricKind::Counter,
        ValueType::Default,
    );
    check(
        "grpc_streams_msgs_received",
        MetricKind::Counter,
        ValueType::Default,
    );
    assert_eq!(registry.len(), 30);
}

#[test]
fn registering_again_returns_the_same_id_unless_the_kind_differs() {
    let mut registry = Registry::new();
    let a = registry
        .register("my_trend", MetricKind::Trend, ValueType::Time)
        .unwrap();
    let again = registry
        .register("my_trend", MetricKind::Trend, ValueType::Time)
        .unwrap();
    assert_eq!(a, again);
    assert_eq!(
        registry.register("my_trend", MetricKind::Counter, ValueType::Default),
        Err(MetricError::KindMismatch {
            name: "my_trend".into(),
            existing: MetricKind::Trend,
            requested: MetricKind::Counter,
        })
    );
}

#[test]
fn metric_names_follow_k6_rules() {
    let mut registry = Registry::new();
    for ok in ["a", "_x", "http_reqs", "Latency_2"] {
        assert!(
            registry
                .register(ok, MetricKind::Counter, ValueType::Default)
                .is_ok(),
            "{ok}"
        );
    }
    let too_long = "a".repeat(129);
    for bad in ["", "1abc", "has-dash", "sp ace", "émoji", too_long.as_str()] {
        assert!(
            matches!(
                registry.register(bad, MetricKind::Counter, ValueType::Default),
                Err(MetricError::InvalidName(_))
            ),
            "{bad:?}"
        );
    }
}

// --- tags -------------------------------------------------------------------

#[test]
fn tag_sets_are_interned_regardless_of_order() {
    let metrics = Metrics::new(Registry::new());
    let tags = metrics.interner();
    let a = tags.intern(&[("method", "GET"), ("status", "200")]);
    let b = tags.intern(&[("status", "200"), ("method", "GET")]);
    let c = tags.intern(&[("method", "POST"), ("status", "200")]);
    assert_eq!(a, b);
    assert_ne!(a, c);
    assert_eq!(tags.intern(&[]), TagSetId::EMPTY);
    assert_eq!(tags.len(), 3, "empty set + two distinct sets");

    let set = tags.get(a);
    assert_eq!(
        set.iter().collect::<Vec<_>>(),
        [("method", "GET"), ("status", "200")],
        "stored sorted by key"
    );
    assert_eq!(set.get("status"), Some("200"));
    assert_eq!(set.get("url"), None);
}

#[test]
fn repeated_tag_keys_keep_the_last_value() {
    let metrics = Metrics::new(Registry::new());
    let tags = metrics.interner();
    let id = tags.intern(&[("k", "first"), ("k", "second")]);
    assert_eq!(tags.intern(&[("k", "second")]), id);
    assert_eq!(tags.get(id).len(), 1);
}

#[test]
fn tag_filters_match_subsets() {
    let metrics = Metrics::new(Registry::new());
    let tags = metrics.interner();
    let set = tags.get(tags.intern(&[("scenario", "checkout"), ("status", "200")]));
    assert!(set.matches(&[]));
    assert!(set.matches(&[("scenario", "checkout")]));
    assert!(set.matches(&[("status", "200"), ("scenario", "checkout")]));
    assert!(!set.matches(&[("scenario", "browse")]));
    assert!(!set.matches(&[("group", "x")]));
}

#[test]
fn interning_from_many_threads_agrees_on_ids() {
    let metrics = Metrics::new(Registry::new());
    let ids: Vec<Vec<TagSetId>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                s.spawn(|| {
                    (0..50)
                        .map(|i| metrics.interner().intern(&[("n", &i.to_string())]))
                        .collect()
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert!(ids.windows(2).all(|w| w[0] == w[1]));
    assert_eq!(metrics.interner().len(), 51);
}

// --- values -----------------------------------------------------------------

#[test]
fn trend_percentiles_are_accurate_to_three_significant_digits() {
    let samples: Vec<f64> = (1..=10_000).map(|i| i as f64 * 0.37).collect();
    let trend = TrendValue::from_samples(&samples);
    let mut sorted = samples.clone();
    sorted.sort_by(f64::total_cmp);
    for p in [1.0, 10.0, 50.0, 90.0, 95.0, 99.0, 99.9] {
        let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
        let exact = sorted[rank.saturating_sub(1)];
        let got = trend.percentile(p);
        let rel = (got - exact).abs() / exact;
        assert!(
            rel <= 0.001,
            "p({p}): got {got}, exact {exact}, error {rel:.5}"
        );
    }
    assert_eq!(trend.percentile(0.0), 0.37, "p(0) is the exact min");
    assert_eq!(trend.percentile(100.0), 3700.0, "p(100) is the exact max");
    assert_eq!(trend.count, 10_000);
    assert!((trend.avg() - 1850.185).abs() < 1e-9);
    assert_eq!(trend.med(), trend.percentile(50.0));
}

#[test]
fn trend_keeps_exact_stats_for_negative_samples() {
    let trend = TrendValue::from_samples(&[-10.0, 5.0, 20.0]);
    assert_eq!(trend.clamped, 1);
    assert_eq!(trend.min, -10.0);
    assert_eq!(trend.max, 20.0);
    assert_eq!(trend.avg(), 5.0);
}

#[test]
fn empty_values_report_zero() {
    let empty = TrendValue::new();
    assert_eq!(empty.avg(), 0.0);
    assert_eq!(empty.percentile(95.0), 0.0);
    assert_eq!(k9_metrics::RateValue::default().rate(), 0.0);
}

// --- recording and aggregation ---------------------------------------------

#[test]
fn each_kind_records_with_k6_semantics() {
    let mut registry = Registry::new();
    let counter = registry
        .register("c", MetricKind::Counter, ValueType::Default)
        .unwrap();
    let gauge = registry
        .register("g", MetricKind::Gauge, ValueType::Default)
        .unwrap();
    let rate = registry
        .register("r", MetricKind::Rate, ValueType::Default)
        .unwrap();
    let time = registry
        .register("t", MetricKind::Trend, ValueType::Time)
        .unwrap();
    let metrics = Arc::new(Metrics::new(registry));
    let t = TagSetId::EMPTY;

    for v in [1.0, 2.0, 3.0] {
        metrics.record(counter, t, v);
        metrics.record(gauge, t, v * 10.0);
    }
    metrics.record(gauge, t, 5.0);
    for v in [1.0, 0.0, 1.0, 1.0] {
        metrics.record(rate, t, v);
    }
    metrics.record_duration(time, t, Duration::from_micros(1500));

    let mut agg = Aggregator::new(metrics);
    agg.tick(Duration::from_secs(1));
    let totals = agg.totals();
    assert_eq!(
        totals.get(counter, t),
        Some(&Value::Counter(k9_metrics::CounterValue { sum: 6.0 }))
    );
    let Some(Value::Gauge(g)) = totals.get(gauge, t) else {
        panic!()
    };
    assert_eq!(
        (g.value, g.min, g.max),
        (5.0, 5.0, 30.0),
        "latest value, plus min and max"
    );
    let Some(Value::Rate(r)) = totals.get(rate, t) else {
        panic!()
    };
    assert_eq!((r.trues, r.total, r.rate()), (3, 4, 0.75));
    let d = trend(totals.get(time, t));
    assert_eq!(d.count, 1);
    assert_eq!(d.max, 1.5, "durations are recorded in milliseconds");
}

#[test]
fn ticks_hold_deltas_and_totals_accumulate() {
    let mut registry = Registry::new();
    let reqs = registry
        .register("reqs", MetricKind::Counter, ValueType::Default)
        .unwrap();
    let lat = registry
        .register("lat", MetricKind::Trend, ValueType::Time)
        .unwrap();
    let metrics = Arc::new(Metrics::new(registry));
    let tags = metrics.interner().intern(&[("status", "200")]);
    let key = k9_metrics::SeriesKey { metric: reqs, tags };
    let mut agg = Aggregator::new(metrics.clone());

    for _ in 0..3 {
        metrics.record(reqs, tags, 1.0);
    }
    metrics.record(lat, tags, 100.0);
    assert_eq!(
        agg.tick(Duration::from_secs(1)).stats[&key],
        Stats::Counter { sum: 3.0 }
    );

    metrics.record(reqs, tags, 1.0);
    assert_eq!(
        agg.tick(Duration::from_secs(2)).stats[&key],
        Stats::Counter { sum: 1.0 }
    );

    let empty = agg.tick(Duration::from_secs(3));
    assert!(empty.stats.is_empty(), "nothing recorded, nothing reported");

    assert_eq!(agg.ticks().len(), 3);
    assert_eq!(agg.ticks()[1].elapsed, Duration::from_secs(2));
    assert_eq!(
        agg.totals().get(reqs, tags),
        Some(&Value::Counter(k9_metrics::CounterValue { sum: 4.0 }))
    );
    assert_eq!(trend(agg.totals().get(lat, tags)).count, 1);
}

#[test]
fn merged_combines_series_matching_a_tag_filter() {
    let (registry, b) = Registry::with_builtins();
    let metrics = Arc::new(Metrics::new(registry));
    let tags = metrics.interner();
    let browse = tags.intern(&[("scenario", "browse"), ("status", "200")]);
    let checkout_ok = tags.intern(&[("scenario", "checkout"), ("status", "200")]);
    let checkout_err = tags.intern(&[("scenario", "checkout"), ("status", "500")]);
    metrics.record(b.http_req_duration, browse, 10.0);
    metrics.record(b.http_req_duration, checkout_ok, 20.0);
    metrics.record(b.http_req_duration, checkout_err, 30.0);

    let mut agg = Aggregator::new(metrics.clone());
    agg.tick(Duration::from_secs(1));
    let totals = agg.totals();
    let interner = metrics.interner();

    let all = totals.merged(b.http_req_duration, &[], interner).unwrap();
    assert_eq!(trend(Some(&all)).count, 3);
    let checkout = totals
        .merged(b.http_req_duration, &[("scenario", "checkout")], interner)
        .unwrap();
    let checkout = trend(Some(&checkout));
    assert_eq!(
        (checkout.count, checkout.min, checkout.max),
        (2, 20.0, 30.0)
    );
    assert!(
        totals
            .merged(b.http_req_duration, &[("scenario", "nope")], interner)
            .is_none()
    );
    assert!(
        totals.merged(b.http_reqs, &[], interner).is_none(),
        "never recorded"
    );
}

#[test]
fn concurrent_recording_loses_nothing() {
    let mut registry = Registry::new();
    let count = registry
        .register("count", MetricKind::Counter, ValueType::Default)
        .unwrap();
    let lat = registry
        .register("lat", MetricKind::Trend, ValueType::Time)
        .unwrap();
    let metrics = Arc::new(Metrics::new(registry));
    let tags = TagSetId::EMPTY;
    let mut agg = Aggregator::new(metrics.clone());

    const THREADS: usize = 8;
    const PER_THREAD: usize = 20_000;
    let done = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                for i in 0..PER_THREAD {
                    metrics.record(count, tags, 1.0);
                    metrics.record(lat, tags, (i % 100) as f64);
                }
                done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            });
        }
        // Tick concurrently with recording, as the engine will.
        let mut t = 0;
        while done.load(std::sync::atomic::Ordering::SeqCst) < THREADS {
            t += 1;
            agg.tick(Duration::from_millis(t));
        }
    });
    agg.tick(Duration::from_secs(60));

    let total = (THREADS * PER_THREAD) as f64;
    assert_eq!(
        agg.totals().get(count, tags),
        Some(&Value::Counter(k9_metrics::CounterValue { sum: total }))
    );
    assert_eq!(trend(agg.totals().get(lat, tags)).count, total as u64);
    let from_ticks: f64 = agg
        .ticks()
        .iter()
        .filter_map(|t| {
            match t.stats.get(&k9_metrics::SeriesKey {
                metric: count,
                tags,
            }) {
                Some(Stats::Counter { sum }) => Some(*sum),
                _ => None,
            }
        })
        .sum();
    assert_eq!(from_ticks, total, "ticks partition the totals");
}

#[test]
fn separate_metrics_instances_do_not_share_shards() {
    let mut registry = Registry::new();
    let c = registry
        .register("c", MetricKind::Counter, ValueType::Default)
        .unwrap();
    let a = Metrics::new(registry.clone());
    let b = Metrics::new(registry);
    a.record(c, TagSetId::EMPTY, 1.0);
    b.record(c, TagSetId::EMPTY, 10.0);
    let a_total = a.take();
    assert_eq!(
        a_total.get(c, TagSetId::EMPTY),
        Some(&Value::Counter(k9_metrics::CounterValue { sum: 1.0 }))
    );
}

// --- raw samples (for streaming outputs) ----------------------------

#[test]
fn raw_samples_are_captured_only_when_enabled() {
    let mut registry = Registry::new();
    let c = registry
        .register("c", MetricKind::Counter, ValueType::Default)
        .unwrap();
    let t = registry
        .register("t", MetricKind::Trend, ValueType::Time)
        .unwrap();

    let off = Arc::new(Metrics::new(registry.clone()));
    off.record(c, TagSetId::EMPTY, 1.0);
    let mut agg = Aggregator::new(off);
    agg.tick(Duration::from_secs(1));
    assert!(agg.drain_samples().is_empty());

    let on = Arc::new(Metrics::with_samples(registry));
    let tags = on.interner().intern(&[("k", "v")]);
    let before = std::time::SystemTime::now();
    std::thread::scope(|s| {
        s.spawn(|| on.record(c, tags, 2.0));
    });
    on.record(t, TagSetId::EMPTY, 12.5);
    let mut agg = Aggregator::new(on);
    agg.tick(Duration::from_secs(1));
    let samples = agg.drain_samples();
    assert_eq!(samples.len(), 2);
    assert!(
        samples.windows(2).all(|w| w[0].time <= w[1].time),
        "sorted by time"
    );
    assert!(samples.iter().all(|s| s.time >= before));
    let counter = samples.iter().find(|s| s.metric == c).unwrap();
    assert_eq!((counter.tags, counter.value), (tags, 2.0));
    assert!(agg.drain_samples().is_empty(), "drained");
    assert_eq!(
        agg.totals().get(c, tags),
        Some(&Value::Counter(k9_metrics::CounterValue { sum: 2.0 })),
        "totals are unaffected"
    );
}

#[test]
fn ticks_also_merge_each_metric_across_its_tag_sets() {
    let mut registry = Registry::new();
    let reqs = registry
        .register("reqs", MetricKind::Counter, ValueType::Default)
        .unwrap();
    let lat = registry
        .register("lat", MetricKind::Trend, ValueType::Time)
        .unwrap();
    let metrics = Arc::new(Metrics::new(registry));
    let ok = metrics.interner().intern(&[("status", "200")]);
    let bad = metrics.interner().intern(&[("status", "500")]);
    let mut agg = Aggregator::new(metrics.clone());
    metrics.record(reqs, ok, 2.0);
    metrics.record(reqs, bad, 1.0);
    // 99 fast responses and one slow one, split across tag sets: only the
    // merged histogram knows p(99) is the fast one and max the slow one.
    for _ in 0..99 {
        metrics.record(lat, ok, 10.0);
    }
    metrics.record(lat, bad, 1000.0);
    let tick = agg.tick(Duration::from_secs(1));
    assert_eq!(tick.metrics[&reqs], Stats::Counter { sum: 3.0 });
    let Stats::Trend {
        count, p99, max, ..
    } = tick.metrics[&lat]
    else {
        panic!()
    };
    assert_eq!(count, 100);
    assert!((p99 - 10.0).abs() < 0.1, "{p99}");
    assert!((max - 1000.0).abs() < 1.0, "{max}");
}
