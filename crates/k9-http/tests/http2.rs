//! HTTP/2: ALPN negotiation, prior knowledge over plain text, multiplexing on
//! one connection, and per-stream timings.

use std::time::{Duration, Instant};

use http::Version;
use k9_http::{Client, ClientConfig, HttpError, HttpVersion, Request};
use k9_testserver::TestServer;

const SLACK: Duration = Duration::from_millis(50);

fn client(server: &TestServer, http_version: HttpVersion) -> Client {
    let mut config = ClientConfig {
        http_version,
        ..ClientConfig::default()
    };
    config.extra_root_certs.push(server.cert_der().clone());
    Client::new(config).unwrap()
}

async fn get(client: &Client, url: &str) -> Result<k9_http::Response, HttpError> {
    client.send(&Request::get(url).unwrap()).await
}

#[tokio::test]
async fn https_negotiates_http2_by_default() {
    let server = TestServer::start().await.unwrap();
    let client = client(&server, HttpVersion::Auto);
    let first = get(&client, &server.https_url("/ok")).await.unwrap();
    assert_eq!(first.version, Version::HTTP_2);
    assert!(first.timings.connecting > Duration::ZERO);
    assert!(first.timings.tls_handshaking > Duration::ZERO);
    assert!(first.bytes_sent > 0 && first.bytes_received > 0);

    let again = get(&client, &server.https_url("/ok")).await.unwrap();
    assert_eq!(again.version, Version::HTTP_2);
    assert_eq!(again.timings.connecting, Duration::ZERO, "reused");
    assert_eq!(again.timings.tls_handshaking, Duration::ZERO, "reused");
    assert!(again.bytes_received > 0);
    assert_eq!(server.connections(), 1);
    assert_eq!(client.idle_connections(), 1);
}

#[tokio::test]
async fn plain_http_stays_http1_unless_http2_is_forced() {
    let server = TestServer::start().await.unwrap();
    let auto = get(&client(&server, HttpVersion::Auto), &server.url("/ok"))
        .await
        .unwrap();
    assert_eq!(auto.version, Version::HTTP_11);
    let forced = get(&client(&server, HttpVersion::Http2), &server.url("/ok"))
        .await
        .unwrap();
    assert_eq!(forced.version, Version::HTTP_2, "h2c with prior knowledge");
}

#[tokio::test]
async fn http1_can_be_forced_over_tls() {
    let server = TestServer::start().await.unwrap();
    let res = get(
        &client(&server, HttpVersion::Http1),
        &server.https_url("/ok"),
    )
    .await
    .unwrap();
    assert_eq!(res.version, Version::HTTP_11);
}

#[tokio::test]
async fn a_server_without_http2_gets_http1() {
    let server = TestServer::start_http1().await.unwrap();
    let res = get(
        &client(&server, HttpVersion::Auto),
        &server.https_url("/ok"),
    )
    .await
    .unwrap();
    assert_eq!(res.version, Version::HTTP_11);
    let err = get(
        &client(&server, HttpVersion::Http2),
        &server.https_url("/ok"),
    )
    .await
    .expect_err("the server refuses h2");
    assert!(err.to_string().contains("HTTP/2"), "{err}");
}

#[tokio::test]
async fn concurrent_requests_share_one_connection_and_keep_their_own_timings() {
    let server = TestServer::start().await.unwrap();
    let client = client(&server, HttpVersion::Auto);
    // Opens the connection, so every request below is a stream on it.
    get(&client, &server.https_url("/ok")).await.unwrap();

    let started = Instant::now();
    let requests = (0..20).map(|i| {
        let client = client.clone();
        // Half wait 100 ms, half 300 ms: each stream reports its own wait.
        let url = server.https_url(&format!("/delay/{}", if i % 2 == 0 { 100 } else { 300 }));
        tokio::spawn(async move { (i, get(&client, &url).await.unwrap()) })
    });
    let requests: Vec<_> = requests.collect();
    let mut done = Vec::new();
    for request in requests {
        done.push(request.await.unwrap());
    }
    assert!(started.elapsed() < Duration::from_millis(300) + 4 * SLACK);
    assert_eq!(server.connections(), 1, "one connection, many streams");
    let mut received = 0;
    for (i, res) in done {
        assert_eq!(res.version, Version::HTTP_2);
        let delay = Duration::from_millis(if i % 2 == 0 { 100 } else { 300 });
        let waiting = res.timings.waiting;
        assert!(
            waiting >= delay && waiting <= delay + 2 * SLACK,
            "request {i}: waiting {waiting:?}, expected about {delay:?}"
        );
        received += res.bytes_received;
    }
    assert!(received > 0);
}

#[tokio::test]
async fn http2_request_bodies_arrive_whole() {
    let server = TestServer::start().await.unwrap();
    let client = client(&server, HttpVersion::Auto);
    let mut request = Request::new(http::Method::POST, &server.https_url("/echo")).unwrap();
    request.body = bytes::Bytes::from(vec![b'x'; 200_000]);
    let res = client.send(&request).await.unwrap();
    assert_eq!(res.version, Version::HTTP_2);
    let echoed = res.json().unwrap();
    assert_eq!(echoed["body"].as_str().unwrap().len(), 200_000);
    assert!(res.bytes_sent >= 200_000, "{}", res.bytes_sent);
}

#[tokio::test]
async fn a_connection_the_server_closes_is_replaced() {
    let server = TestServer::start().await.unwrap();
    let client = client(&server, HttpVersion::Auto);
    get(&client, &server.https_url("/ok")).await.unwrap();
    // The server says GOAWAY and closes the connection. Every request keeps
    // working meanwhile, and one soon goes out on a new connection (how soon
    // depends on when the client reads the GOAWAY).
    server.close_idle_connections();
    let mut replaced = false;
    for _ in 0..40 {
        let res = get(&client, &server.https_url("/ok")).await.unwrap();
        assert_eq!(res.version, Version::HTTP_2);
        if res.timings.connecting > Duration::ZERO {
            replaced = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(replaced, "a new connection");
    assert_eq!(server.connections(), 2);
}
