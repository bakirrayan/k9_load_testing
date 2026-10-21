//! Hot paths of metric recording: one sample, many threads at once, tag
//! interning, and the once-a-second aggregation.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use k9_metrics::{Aggregator, Metrics, Registry};

fn request_tags(metrics: &Metrics, i: usize) -> k9_metrics::TagSetId {
    let url = format!("http://localhost/items/{}", i % 20);
    metrics.interner().intern(&[
        ("method", "GET"),
        ("name", &url),
        ("scenario", "default"),
        ("status", "200"),
        ("url", &url),
    ])
}

fn record(c: &mut Criterion) {
    let mut group = c.benchmark_group("record");
    group.throughput(Throughput::Elements(1));
    for samples in [false, true] {
        let (registry, b) = Registry::with_builtins();
        let metrics = if samples {
            Metrics::with_samples(registry)
        } else {
            Metrics::new(registry)
        };
        let tags = request_tags(&metrics, 0);
        let suffix = if samples { "/with_samples" } else { "" };
        group.bench_function(format!("counter{suffix}"), |bench| {
            bench.iter(|| metrics.record(b.http_reqs, tags, black_box(1.0)));
        });
        group.bench_function(format!("trend{suffix}"), |bench| {
            bench.iter(|| {
                metrics.record_duration(
                    b.http_req_duration,
                    tags,
                    black_box(Duration::from_micros(1234)),
                )
            });
        });
        // Keep the shard from growing without bound between benchmarks.
        let _ = metrics.take_with_samples();
    }
    group.finish();
}

/// What one request records: 8 trends, 2 counters and a rate.
fn record_request(metrics: &Metrics, b: &k9_metrics::Builtins, tags: k9_metrics::TagSetId) {
    let t = Duration::from_micros(1234);
    for metric in [
        b.http_req_duration,
        b.http_req_blocked,
        b.http_req_connecting,
        b.http_req_tls_handshaking,
        b.http_req_sending,
        b.http_req_waiting,
        b.http_req_receiving,
        b.iteration_duration,
    ] {
        metrics.record_duration(metric, tags, t);
    }
    metrics.record(b.http_reqs, tags, 1.0);
    metrics.record(b.data_received, tags, 512.0);
    metrics.record(b.http_req_failed, tags, 0.0);
}

fn contended(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_request_threads");
    const PER_THREAD: u64 = 20_000;
    for threads in [1u64, 8, 32] {
        group.throughput(Throughput::Elements(threads * PER_THREAD));
        group.bench_function(threads.to_string(), |bench| {
            bench.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (registry, b) = Registry::with_builtins();
                    let metrics = Metrics::new(registry);
                    let tags: Vec<_> = (0..threads as usize)
                        .map(|i| request_tags(&metrics, i))
                        .collect();
                    let start = Instant::now();
                    std::thread::scope(|scope| {
                        for tag in &tags {
                            let (metrics, b) = (&metrics, &b);
                            scope.spawn(move || {
                                for _ in 0..PER_THREAD {
                                    record_request(metrics, b, *tag);
                                }
                            });
                        }
                    });
                    total += start.elapsed();
                    black_box(metrics.take());
                }
                total
            });
        });
    }
    group.finish();
}

fn intern(c: &mut Criterion) {
    let (registry, _) = Registry::with_builtins();
    let metrics = Metrics::new(registry);
    request_tags(&metrics, 0);
    c.bench_function("intern/existing", |bench| {
        bench.iter(|| request_tags(&metrics, black_box(0)));
    });
}

/// One second of a busy run: 10,000 requests over 20 URLs, then the tick
/// that merges them into the totals and the per-second series.
fn aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate");
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("tick_10k_requests", |bench| {
        bench.iter_batched(
            || {
                let (registry, b) = Registry::with_builtins();
                let metrics = Arc::new(Metrics::new(registry));
                let tags: Vec<_> = (0..20).map(|i| request_tags(&metrics, i)).collect();
                for i in 0..10_000 {
                    record_request(&metrics, &b, tags[i % 20]);
                }
                Aggregator::new(metrics)
            },
            |mut aggregator| {
                aggregator.tick(Duration::from_secs(1));
                aggregator
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(benches, record, contended, intern, aggregate);
criterion_main!(benches);
