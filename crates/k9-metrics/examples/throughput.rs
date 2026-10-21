//! Rough recording throughput: many threads recording one request's worth of
//! samples (counter + 7 trends + rate) while the aggregator ticks.
//!
//!     cargo run --release -p k9-metrics --example throughput

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use k9_metrics::{Aggregator, Metrics, Registry};

const REQUESTS_PER_THREAD: u64 = 500_000;

fn main() {
    let threads = std::thread::available_parallelism().map_or(8, |n| n.get());
    let (registry, b) = Registry::with_builtins();
    let metrics = Arc::new(Metrics::new(registry));
    let tags = metrics.interner().intern(&[
        ("method", "GET"),
        ("status", "200"),
        ("scenario", "default"),
    ]);
    let trends = [
        b.http_req_duration,
        b.http_req_blocked,
        b.http_req_connecting,
        b.http_req_tls_handshaking,
        b.http_req_sending,
        b.http_req_waiting,
        b.http_req_receiving,
    ];

    let mut agg = Aggregator::new(metrics.clone());
    let done = AtomicBool::new(false);
    let started = Instant::now();
    let mut ticks = 0;
    std::thread::scope(|s| {
        let workers: Vec<_> = (0..threads)
            .map(|t| {
                let metrics = &metrics;
                s.spawn(move || {
                    for i in 0..REQUESTS_PER_THREAD {
                        metrics.record(b.http_reqs, tags, 1.0);
                        for (j, m) in trends.iter().enumerate() {
                            metrics.record(*m, tags, ((i + t as u64 + j as u64) % 500) as f64);
                        }
                        metrics.record(b.http_req_failed, tags, 0.0);
                    }
                })
            })
            .collect();
        s.spawn(|| {
            for w in workers {
                w.join().unwrap();
            }
            done.store(true, Ordering::SeqCst);
        });
        while !done.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(100));
            agg.tick(started.elapsed());
            ticks += 1;
        }
    });
    let record_time = started.elapsed();
    agg.tick(started.elapsed());

    let requests = threads as u64 * REQUESTS_PER_THREAD;
    let samples = requests * 9;
    println!("threads:            {threads}");
    println!("requests recorded:  {requests} ({samples} samples)");
    println!("wall time:          {record_time:.2?} with {ticks} ticks");
    println!(
        "throughput:         {:.1} M requests/s, {:.1} M samples/s",
        requests as f64 / record_time.as_secs_f64() / 1e6,
        samples as f64 / record_time.as_secs_f64() / 1e6
    );
}
