//! HTTP/3 over QUIC, when asked for: one connection per host shared by
//! concurrent requests, per-stream timings.

use std::time::{Duration, Instant};

use http::Version;
use k9_http::{Client, ClientConfig, HttpError, HttpVersion, Request};
use k9_testserver::TestServer;

const SLACK: Duration = Duration::from_millis(50);

fn client(server: &TestServer, config: ClientConfig) -> Client {
    let mut config = ClientConfig {
        http_version: HttpVersion::Http3,
        ..config
    };
    config.extra_root_certs.push(server.cert_der().clone());
    Client::new(config).unwrap()
}

async fn get(client: &Client, url: &str) -> Result<k9_http::Response, HttpError> {
    client.send(&Request::get(url).unwrap()).await
}

#[tokio::test]
async fn http3_is_spoken_when_asked_for() {
    let server = TestServer::start().await.unwrap();
    let client = client(&server, ClientConfig::default());
    let first = get(&client, &server.https_url("/ok")).await.unwrap();
    assert_eq!(first.version, Version::HTTP_3);
    assert_eq!(first.text(), "ok");
    assert!(first.timings.connecting > Duration::ZERO, "QUIC handshake");
    assert!(first.bytes_sent > 0 && first.bytes_received > 0);

    let again = get(&client, &server.https_url("/ok")).await.unwrap();
    assert_eq!(again.version, Version::HTTP_3);
    assert_eq!(again.timings.connecting, Duration::ZERO, "reused");
    assert!(again.bytes_received > 0);
    assert_eq!(server.connections(), 1);
    assert_eq!(client.idle_connections(), 1);
}

#[tokio::test]
async fn concurrent_requests_are_streams_with_their_own_timings() {
    let server = TestServer::start().await.unwrap();
    let client = client(&server, ClientConfig::default());
    get(&client, &server.https_url("/ok")).await.unwrap();

    let started = Instant::now();
    let requests: Vec<_> = (0..20)
        .map(|i| {
            let client = client.clone();
            let url = server.https_url(&format!("/delay/{}", if i % 2 == 0 { 100 } else { 300 }));
            tokio::spawn(async move { (i, get(&client, &url).await.unwrap()) })
        })
        .collect();
    let mut done = Vec::new();
    for request in requests {
        done.push(request.await.unwrap());
    }
    assert!(started.elapsed() < Duration::from_millis(300) + 4 * SLACK);
    assert_eq!(server.connections(), 1, "one QUIC connection, many streams");
    for (i, res) in done {
        assert_eq!(res.version, Version::HTTP_3);
        let delay = Duration::from_millis(if i % 2 == 0 { 100 } else { 300 });
        let waiting = res.timings.waiting;
        assert!(
            waiting >= delay && waiting <= delay + 2 * SLACK,
            "request {i}: waiting {waiting:?}, expected about {delay:?}"
        );
    }
}

#[tokio::test]
async fn request_bodies_arrive_whole() {
    let server = TestServer::start().await.unwrap();
    let client = client(&server, ClientConfig::default());
    let mut request = Request::new(http::Method::POST, &server.https_url("/echo")).unwrap();
    request.body = bytes::Bytes::from(vec![b'x'; 200_000]);
    let res = client.send(&request).await.unwrap();
    assert_eq!(res.version, Version::HTTP_3);
    let echoed = res.json().unwrap();
    assert_eq!(echoed["body"].as_str().unwrap().len(), 200_000);
    assert!(res.bytes_sent >= 200_000, "{}", res.bytes_sent);
}

#[tokio::test]
async fn plain_http_cannot_use_http3() {
    let server = TestServer::start().await.unwrap();
    let client = client(&server, ClientConfig::default());
    let err = get(&client, &server.url("/ok")).await.expect_err("no TLS");
    assert!(err.to_string().contains("HTTP/3"), "{err}");
}

#[tokio::test]
async fn a_server_without_http3_fails_within_the_timeout() {
    let server = TestServer::start_http1().await.unwrap();
    let client = client(
        &server,
        ClientConfig {
            timeout: Duration::from_secs(1),
            ..ClientConfig::default()
        },
    );
    let started = Instant::now();
    get(&client, &server.https_url("/ok"))
        .await
        .expect_err("nothing answers QUIC");
    assert!(started.elapsed() < Duration::from_secs(2));
}
