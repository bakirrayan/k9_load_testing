//! Redirects, cookies and TLS options.

use std::time::Duration;

use http::{HeaderValue, Method, StatusCode};
use k9_http::{Client, ClientConfig, Request};
use k9_testserver::TestServer;

fn client(server: &TestServer, config: ClientConfig) -> Client {
    let mut config = config;
    config.extra_root_certs.push(server.cert_der().clone());
    Client::new(config).unwrap()
}

async fn get(client: &Client, url: &str) -> k9_http::Response {
    client.send(&Request::get(url).unwrap()).await.unwrap()
}

fn encode(s: &str) -> String {
    s.replace(':', "%3A")
        .replace('/', "%2F")
        .replace('?', "%3F")
        .replace('=', "%3D")
        .replace('&', "%26")
}

// --- redirects ---------------------------------------------------------------

#[tokio::test]
async fn follows_redirect_chains_and_reports_each_hop() {
    let server = TestServer::start().await.unwrap();
    let client = client(&server, ClientConfig::default());
    let res = get(&client, &server.url("/redirect/3")).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.text(), "redirects done");
    assert_eq!(res.url.path(), "/redirect/0");
    let hops: Vec<_> = res
        .redirects
        .iter()
        .map(|h| (h.url.path(), h.status.as_u16()))
        .collect();
    assert_eq!(
        hops,
        [
            ("/redirect/3", 302),
            ("/redirect/2", 302),
            ("/redirect/1", 302)
        ]
    );
    assert!(
        res.redirects
            .iter()
            .all(|h| h.timings.duration() > Duration::ZERO)
    );
    assert_eq!(
        server.connections(),
        1,
        "hops reuse the keep-alive connection"
    );
}

#[tokio::test]
async fn stops_after_max_redirects_and_returns_the_redirect() {
    let server = TestServer::start().await.unwrap();
    let client = client(
        &server,
        ClientConfig {
            max_redirects: 2,
            ..ClientConfig::default()
        },
    );
    let res = get(&client, &server.url("/redirect/5")).await;
    assert_eq!(res.status, StatusCode::FOUND);
    assert_eq!(res.url.path(), "/redirect/3");
    assert_eq!(res.redirects.len(), 2);

    let none = client_with_no_redirects(&server);
    let res = get(&none, &server.url("/redirect/1")).await;
    assert_eq!((res.status, res.redirects.len()), (StatusCode::FOUND, 0));
}

fn client_with_no_redirects(server: &TestServer) -> Client {
    client(
        server,
        ClientConfig {
            max_redirects: 0,
            ..ClientConfig::default()
        },
    )
}

async fn post_through_redirect(server: &TestServer, status: u16) -> serde_json::Value {
    let client = client(server, ClientConfig::default());
    let url = server.url(&format!("/redirect-to?status={status}&url=/echo"));
    let mut req = Request::new(Method::POST, &url).unwrap();
    req.body = "payload".into();
    client.send(&req).await.unwrap().json().unwrap()
}

#[tokio::test]
async fn see_other_turns_post_into_get_without_body() {
    let server = TestServer::start().await.unwrap();
    for status in [301, 302, 303] {
        let echo = post_through_redirect(&server, status).await;
        assert_eq!(echo["method"], "GET", "{status}");
        assert_eq!(echo["body"], "", "{status}");
        assert!(
            echo["headers"].get("content-length").is_none(),
            "{status}: {echo}"
        );
    }
}

#[tokio::test]
async fn temporary_and_permanent_redirects_keep_method_and_body() {
    let server = TestServer::start().await.unwrap();
    for status in [307, 308] {
        let echo = post_through_redirect(&server, status).await;
        assert_eq!(echo["method"], "POST", "{status}");
        assert_eq!(echo["body"], "payload", "{status}");
    }
}

#[tokio::test]
async fn authorization_is_dropped_when_redirecting_to_another_host() {
    let server = TestServer::start().await.unwrap();
    let client = client(&server, ClientConfig::default());
    let send = |target: String| {
        let client = client.clone();
        let url = server.url(&format!("/redirect-to?url={}", encode(&target)));
        async move {
            let mut req = Request::get(&url).unwrap();
            req.headers
                .insert("authorization", HeaderValue::from_static("Bearer secret"));
            client.send(&req).await.unwrap().json().unwrap()
        }
    };
    let port = server.http_addr().port();
    let same: serde_json::Value = send(format!("http://127.0.0.1:{port}/echo")).await;
    assert_eq!(same["headers"]["authorization"], "Bearer secret");
    let other: serde_json::Value = send(format!("http://localhost:{port}/echo")).await;
    assert!(other["headers"].get("authorization").is_none(), "{other}");
}

// --- cookies -----------------------------------------------------------------

#[tokio::test]
async fn cookies_are_stored_and_sent_per_client_pool() {
    let server = TestServer::start().await.unwrap();
    let vu1 = client(&server, ClientConfig::default());
    let vu2 = vu1.with_separate_pool();

    get(&vu1, &server.url("/cookies/set/session/abc")).await;
    let echo = get(&vu1, &server.url("/echo")).await.json().unwrap();
    assert_eq!(echo["headers"]["cookie"], "session=abc");

    let other = get(&vu2, &server.url("/echo")).await.json().unwrap();
    assert!(
        other["headers"].get("cookie").is_none(),
        "each VU has its own jar"
    );

    vu1.clear_cookies();
    let cleared = get(&vu1, &server.url("/echo")).await.json().unwrap();
    assert!(cleared["headers"].get("cookie").is_none());
}

#[tokio::test]
async fn cookies_set_on_a_redirect_hop_are_sent_on_the_next() {
    let server = TestServer::start().await.unwrap();
    let client = client(&server, ClientConfig::default());
    let res = get(&client, &server.url("/cookies/set/step/1?redirect=/echo")).await;
    assert_eq!(res.redirects.len(), 1);
    assert_eq!(res.json().unwrap()["headers"]["cookie"], "step=1");
}

// --- TLS ----------------------------------------------------------------------

#[tokio::test]
async fn insecure_skip_tls_verify_accepts_self_signed_certificates() {
    let server = TestServer::start().await.unwrap();
    let client = Client::new(ClientConfig {
        insecure_skip_tls_verify: true,
        ..ClientConfig::default()
    })
    .unwrap();
    let res = get(&client, &server.https_url("/ok")).await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(res.timings.tls_handshaking > Duration::ZERO);
}
