//! The HTTP client against a local server: one request at a time on a
//! kept-alive connection (latency), and 64 VU-like clients at once
//! (throughput). The server's own time is included in both.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use http::Method;
use k9_http::{Client, ClientConfig, Request};
use k9_testserver::TestServer;

fn client(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let server = runtime.block_on(TestServer::start()).unwrap();
    let client = Client::new(ClientConfig::default()).unwrap();
    let request = Request::new(Method::GET, &server.url("/ok")).unwrap();
    let large = Request::new(Method::GET, &server.url("/bytes/65536")).unwrap();

    let mut group = c.benchmark_group("http");
    group.throughput(Throughput::Elements(1));
    group.bench_function("sequential_keep_alive", |bench| {
        bench
            .to_async(&runtime)
            .iter(|| async { client.send(&request).await.unwrap() });
    });
    group.bench_function("sequential_64k_body", |bench| {
        bench
            .to_async(&runtime)
            .iter(|| async { client.send(&large).await.unwrap() });
    });

    const CLIENTS: usize = 64;
    const EACH: usize = 50;
    group.throughput(Throughput::Elements((CLIENTS * EACH) as u64));
    // Each "VU" has its own connection pool, as in the engine.
    let clients: Vec<Client> = (0..CLIENTS).map(|_| client.with_separate_pool()).collect();
    group.bench_function("concurrent_64_clients", |bench| {
        bench.to_async(&runtime).iter(|| async {
            let mut tasks = tokio::task::JoinSet::new();
            for client in &clients {
                let (client, request) = (client.clone(), request.clone());
                tasks.spawn(async move {
                    for _ in 0..EACH {
                        client.send(&request).await.unwrap();
                    }
                });
            }
            while let Some(done) = tasks.join_next().await {
                done.unwrap();
            }
        });
    });
    group.finish();
}

criterion_group!(benches, client);
criterion_main!(benches);
