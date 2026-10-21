//! However samples are spread across threads and
//! however often the aggregator ticks, the totals equal a single-threaded
//! reference computed directly from the samples.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use k9_metrics::{
    Aggregator, MetricId, MetricKind, Metrics, Registry, TrendValue, Value, ValueType,
};
use proptest::prelude::*;

#[derive(Debug, Clone)]
struct Sample {
    metric: usize,
    tag: usize,
    value: f64,
}

#[derive(Debug, Clone)]
struct Case {
    samples: Vec<Sample>,
    /// Sizes of consecutive chunks; each chunk is recorded on a fresh OS thread.
    chunks: Vec<usize>,
    /// After which chunks the aggregator ticks.
    tick_after: Vec<bool>,
}

fn case() -> impl Strategy<Value = Case> {
    // Integer values keep float sums exact regardless of addition order.
    let sample = (0..4usize, 0..3usize, -20i32..5000).prop_map(|(metric, tag, v)| Sample {
        metric,
        tag,
        value: v as f64,
    });
    (
        prop::collection::vec(sample, 0..400),
        prop::collection::vec(1..60usize, 1..12),
    )
        .prop_flat_map(|(samples, chunks)| {
            let n = chunks.len();
            (
                Just(samples),
                Just(chunks),
                prop::collection::vec(any::<bool>(), n),
            )
        })
        .prop_map(|(samples, chunks, tick_after)| Case {
            samples,
            chunks,
            tick_after,
        })
}

/// What the totals must be, computed without k9-metrics' recording path.
#[derive(Debug, PartialEq)]
enum Expected {
    Counter(f64),
    Gauge { last: f64, min: f64, max: f64 },
    Rate { trues: u64, total: u64 },
    Trend(TrendValue),
}

fn reference(samples: &[Sample]) -> HashMap<(usize, usize), Expected> {
    let mut trends: HashMap<(usize, usize), Vec<f64>> = HashMap::new();
    let mut out = HashMap::new();
    for s in samples {
        let key = (s.metric, s.tag);
        match s.metric {
            0 => {
                let e = out.entry(key).or_insert(Expected::Counter(0.0));
                if let Expected::Counter(sum) = e {
                    *sum += s.value;
                }
            }
            1 => {
                let e = out.entry(key).or_insert(Expected::Gauge {
                    last: s.value,
                    min: s.value,
                    max: s.value,
                });
                if let Expected::Gauge { last, min, max } = e {
                    *last = s.value;
                    *min = min.min(s.value);
                    *max = max.max(s.value);
                }
            }
            2 => {
                let e = out
                    .entry(key)
                    .or_insert(Expected::Rate { trues: 0, total: 0 });
                if let Expected::Rate { trues, total } = e {
                    *trues += u64::from(s.value != 0.0);
                    *total += 1;
                }
            }
            _ => trends.entry(key).or_default().push(s.value),
        }
    }
    for (key, values) in trends {
        out.insert(key, Expected::Trend(TrendValue::from_samples(&values)));
    }
    out
}

fn actual(value: &Value) -> Expected {
    match value {
        Value::Counter(c) => Expected::Counter(c.sum),
        Value::Gauge(g) => Expected::Gauge {
            last: g.value,
            min: g.min,
            max: g.max,
        },
        Value::Rate(r) => Expected::Rate {
            trues: r.trues,
            total: r.total,
        },
        Value::Trend(t) => Expected::Trend(t.clone()),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn totals_equal_single_threaded_reference(case in case()) {
        let mut registry = Registry::new();
        let ids: Vec<MetricId> = [
            ("c", MetricKind::Counter),
            ("g", MetricKind::Gauge),
            ("r", MetricKind::Rate),
            ("t", MetricKind::Trend),
        ]
        .into_iter()
        .map(|(name, kind)| registry.register(name, kind, ValueType::Default).unwrap())
        .collect();
        let metrics = Arc::new(Metrics::new(registry));
        let tag_ids: Vec<_> = (0..3)
            .map(|i| metrics.interner().intern(&[("t", &i.to_string())]))
            .collect();
        let mut agg = Aggregator::new(metrics.clone());

        // Record chunk by chunk, each on its own thread, in order (so "latest
        // gauge value" is well defined), ticking at arbitrary chunk boundaries.
        let mut rest = case.samples.as_slice();
        let mut elapsed = 0;
        for (i, &size) in case.chunks.iter().enumerate() {
            let (chunk, tail) = rest.split_at(size.min(rest.len()));
            rest = tail;
            std::thread::scope(|s| {
                s.spawn(|| {
                    for sample in chunk {
                        metrics.record(ids[sample.metric], tag_ids[sample.tag], sample.value);
                    }
                });
            });
            if case.tick_after[i] {
                elapsed += 1;
                agg.tick(Duration::from_secs(elapsed));
            }
        }
        // Anything past the last chunk goes on this thread.
        for sample in rest {
            metrics.record(ids[sample.metric], tag_ids[sample.tag], sample.value);
        }
        agg.tick(Duration::from_secs(elapsed + 1));

        let expected = reference(&case.samples);
        let got: HashMap<(usize, usize), Expected> = agg
            .totals()
            .iter()
            .map(|(key, value)| {
                let m = ids.iter().position(|id| *id == key.metric).unwrap();
                let t = tag_ids.iter().position(|id| *id == key.tags).unwrap();
                ((m, t), actual(value))
            })
            .collect();
        prop_assert_eq!(&got, &expected);

        // The reference above shares TrendValue with the code under test, so
        // also check trends against exact statistics from the raw samples.
        for ((m, t), value) in &got {
            let Expected::Trend(trend) = value else { continue };
            let mut raw: Vec<f64> = case
                .samples
                .iter()
                .filter(|s| s.metric == *m && s.tag == *t)
                .map(|s| s.value)
                .collect();
            prop_assert_eq!(trend.count, raw.len() as u64);
            prop_assert_eq!(trend.sum, raw.iter().sum::<f64>());
            raw.sort_by(f64::total_cmp);
            prop_assert_eq!(trend.min, raw[0]);
            prop_assert_eq!(trend.max, *raw.last().unwrap());
            // Percentiles are only exact-ish for non-negative samples; negative
            // ones are documented as sitting at 0 in the histogram.
            if trend.clamped > 0 {
                continue;
            }
            for p in [50.0, 90.0, 95.0, 99.0] {
                let rank = ((p / 100.0) * raw.len() as f64).ceil().max(1.0) as usize;
                let exact = raw[rank - 1];
                let got = trend.percentile(p);
                prop_assert!(
                    (got - exact).abs() <= exact * 0.001 + 0.001,
                    "p({}) = {}, exact {}",
                    p,
                    got,
                    exact
                );
            }
        }
    }
}
