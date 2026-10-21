//! k9-http against k9-testserver: timing accuracy, phase attribution,
//! connection reuse, and error reporting.

use std::time::{Duration, Instant};

use http::{HeaderValue, Method, StatusCode};
use k9_http::{Client, ClientConfig, HttpError, Request};
use k9_testserver::TestServer;

/// Slack allowed above an injected delay (scheduler jitter, loopback, CI noise).
const SLACK: Duration = Duration::from_millis(50);

async fn setup() -> (TestServer, Client) {
    let server = TestServer::start().await.unwrap();
    let client = client_trusting(&server, ClientConfig::default());
    (server, client)
}

fn client_trusting(server: &TestServer, mut config: ClientConfig) -> Client {
    config.extra_root_certs.push(server.cert_der().clone());
    Client::new(config).unwrap()
}

async fn get(client: &Client, url: &str) -> k9_http::Response {
    client.send(&Request::get(url).unwrap()).await.unwrap()
}

fn assert_between(what: &str, value: Duration, min: Duration, max: Duration) {
    assert!(
        value >= min && value <= max,
        "{what} = {value:?}, expected between {min:?} and {max:?}"
    );
}

#[tokio::test]
async fn delay_endpoint_reports_its_delay_as_waiting() {
    let (server, client) = setup().await;
    let res = get(&client, &server.url("/delay/100")).await;

    assert_eq!(res.status, StatusCode::OK);
    let delay = Duration::from_millis(100);
    assert_between("waiting", res.timings.waiting, delay, delay + SLACK);
    assert_between("duration", res.timings.duration(), delay, delay + SLACK);
}

#[tokio::test]
async fn new_connection_has_connect_phase_and_reused_one_does_not() {
    let (server, client) = setup().await;

    let first = get(&client, &server.url("/ok")).await;
    assert!(
        first.timings.connecting > Duration::ZERO,
        "{:?}",
        first.timings
    );
    assert_eq!(first.timings.tls_handshaking, Duration::ZERO, "plain HTTP");

    let second = get(&client, &server.url("/ok")).await;
    assert_eq!(
        second.timings.connecting,
        Duration::ZERO,
        "{:?}",
        second.timings
    );
    assert_eq!(second.timings.tls_handshaking, Duration::ZERO);

    assert_eq!(server.connections(), 1, "the server saw one TCP connection");
    assert_eq!(client.idle_connections(), 1);
}

#[tokio::test]
async fn https_records_tls_handshake_only_on_new_connections() {
    let (server, client) = setup().await;

    let first = get(&client, &server.https_url("/ok")).await;
    assert_eq!(first.status, StatusCode::OK);
    assert!(first.timings.connecting > Duration::ZERO);
    assert!(
        first.timings.tls_handshaking > Duration::ZERO,
        "{:?}",
        first.timings
    );

    let second = get(&client, &server.https_url("/ok")).await;
    assert_eq!(second.timings.tls_handshaking, Duration::ZERO);
    assert_eq!(server.connections(), 1);
}

#[tokio::test]
async fn untrusted_certificate_is_a_tls_error() {
    let server = TestServer::start().await.unwrap();
    let client = Client::new(ClientConfig::default()).unwrap(); // bundled roots only
    let err = client
        .send(&Request::get(&server.https_url("/ok")).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::Tls { .. }), "{err:?}");
}

#[tokio::test]
async fn hostnames_are_resolved() {
    let (server, client) = setup().await;
    let url = format!("http://localhost:{}/ok", server.http_addr().port());
    let res = get(&client, &url).await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(res.remote_addr.ip().is_loopback());
}

#[tokio::test]
async fn unresolvable_host_is_a_dns_error() {
    let (_server, client) = setup().await;
    let err = client
        .send(&Request::get("http://does-not-exist.invalid/").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::Dns { .. }), "{err:?}");
}

#[tokio::test]
async fn status_headers_and_body_are_returned() {
    let (server, client) = setup().await;
    let res = get(&client, &server.url("/status/418")).await;
    assert_eq!(res.status, StatusCode::IM_A_TEAPOT);
    assert_eq!(res.text(), "status 418");
    assert!(res.headers.contains_key(http::header::CONTENT_TYPE));
}

#[tokio::test]
async fn large_bodies_are_read_and_counted() {
    let (server, client) = setup().await;
    let res = get(&client, &server.url("/bytes/1000000")).await;
    assert_eq!(res.body.len(), 1_000_000);
    assert!(res.bytes_received > 1_000_000, "headers count too");
    assert!(res.bytes_sent > 0);
    assert!(res.timings.receiving > Duration::ZERO);
}

#[tokio::test]
async fn discard_body_still_reads_and_counts_bytes() {
    let server = TestServer::start().await.unwrap();
    let client = client_trusting(
        &server,
        ClientConfig {
            discard_body: true,
            ..ClientConfig::default()
        },
    );
    let res = get(&client, &server.url("/bytes/50000")).await;
    assert!(res.body.is_empty());
    assert!(res.bytes_received > 50_000);
    // The connection is still reusable, so the body was fully drained.
    get(&client, &server.url("/ok")).await;
    assert_eq!(server.connections(), 1);
}

#[tokio::test]
async fn method_headers_and_body_are_sent() {
    let (server, client) = setup().await;
    let mut req = Request::new(Method::POST, &server.url("/echo?x=1")).unwrap();
    req.headers
        .insert("x-k9-test", HeaderValue::from_static("yes"));
    req.body = "hello".into();

    let echo = client.send(&req).await.unwrap().json().unwrap();
    assert_eq!(echo["method"], "POST");
    assert_eq!(echo["path"], "/echo");
    assert_eq!(echo["query"], "x=1");
    assert_eq!(echo["headers"]["x-k9-test"], "yes");
    assert_eq!(echo["headers"]["content-length"], "5");
    assert_eq!(echo["body"], "hello");
    let ua = echo["headers"]["user-agent"].as_str().unwrap();
    assert!(ua.starts_with("k9/"), "{ua}");
}

#[tokio::test]
async fn timeout_is_reported_promptly() {
    let server = TestServer::start().await.unwrap();
    let client = client_trusting(
        &server,
        ClientConfig {
            timeout: Duration::from_millis(200),
            ..ClientConfig::default()
        },
    );
    let started = Instant::now();
    let err = client
        .send(&Request::get(&server.url("/delay/2000")).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::Timeout(_)), "{err:?}");
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn refused_connection_is_a_connect_error() {
    let (_server, client) = setup().await;
    // Bind then drop a listener to get a port nobody is listening on.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let err = client
        .send(&Request::get(&format!("http://127.0.0.1:{port}/")).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::Connect { .. }), "{err:?}");
}

#[tokio::test]
async fn connection_close_is_not_reused() {
    let (server, client) = setup().await;
    get(&client, &server.url("/close")).await;
    assert_eq!(client.idle_connections(), 0);
    let next = get(&client, &server.url("/ok")).await;
    assert!(next.timings.connecting > Duration::ZERO);
    assert_eq!(server.connections(), 2);
}

#[tokio::test]
async fn idle_connection_closed_by_server_is_replaced_transparently() {
    let (server, client) = setup().await;
    get(&client, &server.url("/ok")).await;
    assert_eq!(client.idle_connections(), 1);

    server.close_idle_connections();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let res = get(&client, &server.url("/ok")).await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(
        res.timings.connecting > Duration::ZERO,
        "a new connection was made"
    );
    assert_eq!(server.connections(), 2);
}

/// Same as above but without giving the client time to notice the close, so the
/// request can land on a connection that is already dead (the retry path).
#[tokio::test]
async fn request_racing_a_server_close_is_retried() {
    let (server, client) = setup().await;
    for _ in 0..20 {
        get(&client, &server.url("/ok")).await;
        server.close_idle_connections();
        let res = get(&client, &server.url("/ok")).await;
        assert_eq!(res.status, StatusCode::OK);
    }
}

#[tokio::test]
async fn cloned_clients_share_a_pool_and_concurrent_requests_use_separate_connections() {
    let (server, client) = setup().await;
    let other = client.clone();
    let url = server.url("/delay/50");
    let (a, b) = tokio::join!(get(&client, &url), get(&other, &url));
    assert_eq!((a.status, b.status), (StatusCode::OK, StatusCode::OK));
    assert_eq!(server.connections(), 2);
    assert_eq!(client.idle_connections(), 2);
    assert_eq!(other.idle_connections(), 2, "the pool is shared");
}

#[tokio::test]
async fn separate_clients_do_not_share_connections() {
    let (server, client) = setup().await;
    let other = client_trusting(&server, ClientConfig::default());
    get(&client, &server.url("/ok")).await;
    get(&other, &server.url("/ok")).await;
    assert_eq!(
        server.connections(),
        2,
        "per-VU clients get their own connections"
    );
}

#[tokio::test]
async fn stalled_server_holds_requests_until_resumed() {
    let (server, client) = setup().await;
    server.stall();
    let url = server.url("/ok");
    let pending = tokio::spawn({
        let client = client.clone();
        async move { get(&client, &url).await }
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!pending.is_finished(), "request should hang while stalled");
    server.resume();
    let res = pending.await.unwrap();
    assert!(res.timings.waiting >= Duration::from_millis(150));
}

#[tokio::test]
async fn error_rate_endpoint_fails_exactly_the_requested_share() {
    let (server, client) = setup().await;
    let mut failures = 0;
    for _ in 0..100 {
        if get(&client, &server.url("/error-rate/10"))
            .await
            .status
            .is_server_error()
        {
            failures += 1;
        }
    }
    assert_eq!(failures, 10);
}

#[test]
fn invalid_urls_are_rejected() {
    for bad in ["not a url", "ftp://example.com/", "http://"] {
        let err = Request::get(bad).unwrap_err();
        assert!(
            matches!(err, HttpError::InvalidUrl { .. }),
            "{bad}: {err:?}"
        );
    }
}
