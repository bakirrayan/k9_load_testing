//! k6's network options: `hosts` overrides, blocked hostnames and IPs,
//! the source address (`localIPs`) and `noConnectionReuse`.

use std::net::IpAddr;

use k9_http::{
    Client, ClientConfig, HostOverride, HostPattern, HttpError, IpPool, IpRange, Request, WsRequest,
};
use k9_testserver::TestServer;

async fn get(client: &Client, url: &str) -> Result<k9_http::Response, HttpError> {
    client.send(&Request::get(url).unwrap()).await
}

fn hosts(pairs: &[(&str, &str)]) -> Vec<HostOverride> {
    pairs
        .iter()
        .map(|(from, to)| HostOverride::new(from, to).unwrap())
        .collect()
}

#[tokio::test]
async fn hosts_overrides_send_names_to_other_addresses() {
    let server = TestServer::start().await.unwrap();
    let port = server.http_addr().port();
    let client = Client::new(ClientConfig {
        hosts: hosts(&[
            ("api.k9.test", "127.0.0.1"),
            (&format!("web.k9.test:{port}"), "127.0.0.1"),
            ("moved.k9.test:80", &server.http_addr().to_string()),
            ("*.wild.k9.test", "127.0.0.1"),
        ]),
        ..ClientConfig::default()
    })
    .unwrap();
    for url in [
        format!("http://api.k9.test:{port}/ok"),
        format!("http://web.k9.test:{port}/ok"),
        "http://moved.k9.test/ok".to_owned(),
        format!("http://a.b.wild.k9.test:{port}/ok"),
    ] {
        let res = get(&client, &url)
            .await
            .unwrap_or_else(|e| panic!("{url}: {e}"));
        assert_eq!(res.status, 200, "{url}");
        assert_eq!(res.remote_addr, server.http_addr(), "{url}");
    }
    let err = get(&client, &format!("http://web.k9.test:{}/ok", port + 1))
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::Dns { .. }), "{err}");
    let err = get(&client, &format!("http://wild.k9.test:{port}/ok"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::Dns { .. }),
        "`*.` matches subdomains only: {err}"
    );

    let ws = client
        .websocket(&WsRequest::new(&format!("ws://api.k9.test:{port}/ws/echo")).unwrap())
        .await;
    assert!(ws.is_ok(), "{:?}", ws.err());
}

#[test]
fn bad_overrides_are_explained() {
    for (from, to, expected) in [
        ("a.test", "not-an-ip", "IP address"),
        ("a.test:x", "127.0.0.1", "port"),
        ("*", "127.0.0.1", "hostname"),
        ("a.test", "127.0.0.1:99999", "IP address"),
    ] {
        let err = HostOverride::new(from, to).unwrap_err();
        assert!(err.contains(expected), "{from} → {to}: {err}");
    }
}

#[tokio::test]
async fn blocked_hostnames_and_ips_fail_before_connecting() {
    let server = TestServer::start().await.unwrap();
    let port = server.http_addr().port();
    let client = Client::new(ClientConfig {
        hosts: hosts(&[("api.k9.test", "127.0.0.1"), ("ok.k9.test", "127.0.0.1")]),
        block_hostnames: vec!["*.k9.test".parse().unwrap()],
        ..ClientConfig::default()
    })
    .unwrap();
    let err = get(&client, &format!("http://api.k9.test:{port}/ok"))
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::Blocked { .. }), "{err}");
    assert!(err.to_string().contains("*.k9.test"), "{err}");
    assert!(
        get(&client, &server.url("/ok")).await.is_ok(),
        "IPs aren't hostnames"
    );

    let client = Client::new(ClientConfig {
        blocked_ips: vec!["127.0.0.0/8".parse().unwrap()],
        ..ClientConfig::default()
    })
    .unwrap();
    for url in [
        server.url("/ok"),
        format!("http://localhost:{port}/ok"),
        format!("ws://127.0.0.1:{port}/ws/echo"),
    ] {
        let err = if url.starts_with("ws") {
            match client.websocket(&WsRequest::new(&url).unwrap()).await {
                Ok(_) => panic!("{url}: not blocked"),
                Err(e) => e,
            }
        } else {
            get(&client, &url).await.unwrap_err()
        };
        let text = err.to_string();
        assert!(matches!(err, HttpError::Blocked { .. }), "{url}: {err}");
        assert!(text.contains("127.0.0.0/8"), "{url}: {text}");
    }
    assert_eq!(
        server.connections(),
        1,
        "only the allowed request connected"
    );
}

#[test]
fn host_patterns_and_ip_ranges() {
    let p: HostPattern = "*.Example.com".parse().unwrap();
    assert!(p.matches("a.example.com") && p.matches("a.b.example.com"));
    assert!(!p.matches("example.com") && !p.matches("badexample.com"));
    let exact: HostPattern = "example.com".parse().unwrap();
    assert!(exact.matches("example.com") && !exact.matches("a.example.com"));
    for bad in ["", "a.*.com", "*example.com"] {
        assert!(bad.parse::<HostPattern>().is_err(), "{bad}");
    }

    let ip = |s: &str| s.parse::<IpAddr>().unwrap();
    let cidr: IpRange = "10.0.0.0/30".parse().unwrap();
    assert!(cidr.contains(ip("10.0.0.3")) && !cidr.contains(ip("10.0.0.4")));
    assert!(!cidr.contains(ip("::1")));
    let span: IpRange = "10.0.0.5-10.0.0.7".parse().unwrap();
    assert!(span.contains(ip("10.0.0.6")) && !span.contains(ip("10.0.0.8")));
    let v6: IpRange = "fd00::/64".parse().unwrap();
    assert!(v6.contains(ip("fd00::1")) && !v6.contains(ip("fd01::1")));
    assert_eq!(cidr.to_string(), "10.0.0.0/30");
    for bad in [
        "10.0.0.0/33",
        "10.0.0.9-10.0.0.1",
        "10.0.0.1-::1",
        "x",
        "10.0.0.0/",
    ] {
        assert!(bad.parse::<IpRange>().is_err(), "{bad}");
    }

    let pool = IpPool::new(vec![cidr, span, "10.0.1.1".parse().unwrap()]);
    let firsts: Vec<String> = (0..9).map(|i| pool.nth(i).to_string()).collect();
    assert_eq!(
        firsts,
        [
            "10.0.0.0", "10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.5", "10.0.0.6", "10.0.0.7",
            "10.0.1.1", "10.0.0.0"
        ]
    );
    assert_eq!(pool.len(), 8);
}

#[tokio::test]
async fn requests_leave_from_the_local_ip() {
    let server = TestServer::start().await.unwrap();
    let base = Client::new(ClientConfig::default()).unwrap();
    for local in ["127.0.0.2", "127.0.0.3"] {
        let client = base
            .with_separate_pool()
            .with_local_ip(local.parse().unwrap());
        let res = get(&client, &server.url("/ip")).await.unwrap();
        assert_eq!(res.text(), local);
        let ws = client
            .websocket(&WsRequest::new(&server.url("/ws/echo").replace("http", "ws")).unwrap())
            .await;
        assert!(ws.is_ok(), "{:?}", ws.err());
    }
    let res = get(&base, &server.url("/ip")).await.unwrap();
    assert_eq!(res.text(), "127.0.0.1");

    let v6 = base
        .with_separate_pool()
        .with_local_ip("::1".parse().unwrap());
    let err = get(&v6, &server.url("/ip")).await.unwrap_err();
    assert!(err.to_string().contains("::1"), "{err}");
}

#[tokio::test]
async fn no_connection_reuse_opens_a_connection_per_request() {
    let server = TestServer::start().await.unwrap();
    let client = Client::new(ClientConfig {
        no_connection_reuse: true,
        ..ClientConfig::default()
    })
    .unwrap();
    for _ in 0..3 {
        get(&client, &server.url("/ok")).await.unwrap();
    }
    assert_eq!(server.connections(), 3);
    assert_eq!(client.idle_connections(), 0);

    let client = Client::new(ClientConfig::default()).unwrap();
    for _ in 0..3 {
        get(&client, &server.url("/ok")).await.unwrap();
    }
    assert_eq!(server.connections(), 4);
    client.close_connections();
    assert_eq!(client.idle_connections(), 0);
    get(&client, &server.url("/ok")).await.unwrap();
    assert_eq!(server.connections(), 5);
}
