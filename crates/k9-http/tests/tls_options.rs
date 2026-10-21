//! k6's TLS options: client certificates per domain (`tlsAuth`), protocol
//! versions (`tlsVersion`) and cipher suites (`tlsCipherSuites`), and the
//! TLS details responses report.

use k9_http::grpc::{GrpcRequest, Protos, target};
use k9_http::{Client, ClientCert, ClientConfig, HttpError, Request, TlsVersion, WsRequest};
use k9_testserver::TestServer;

fn config(server: &TestServer) -> ClientConfig {
    ClientConfig {
        extra_root_certs: vec![server.cert_der().clone()],
        ..ClientConfig::default()
    }
}

fn cert(server: &TestServer, domains: &[&str]) -> ClientCert {
    let id = server.client_identity();
    ClientCert {
        domains: domains.iter().map(ToString::to_string).collect(),
        cert_pem: id.cert_pem.clone(),
        key_pem: id.key_pem.clone(),
    }
}

async fn get(client: &Client, url: &str) -> Result<k9_http::Response, HttpError> {
    client.send(&Request::get(url).unwrap()).await
}

#[tokio::test]
async fn mutual_tls_presents_the_certificate_for_the_host() {
    let server = TestServer::start().await.unwrap();
    let url = server.mtls_url("/ok");
    let without = Client::new(config(&server)).unwrap();
    assert!(get(&without, &url).await.is_err(), "no client certificate");
    for domains in [&["localhost"][..], &[], &["*.example.com", "localhost"]] {
        let client = Client::new(ClientConfig {
            client_certs: vec![cert(&server, domains)],
            ..config(&server)
        })
        .unwrap();
        let res = get(&client, &url)
            .await
            .unwrap_or_else(|e| panic!("{domains:?}: {e}"));
        assert_eq!(res.status, 200, "{domains:?}");
    }
    let elsewhere = Client::new(ClientConfig {
        client_certs: vec![cert(&server, &["*.example.com", "other.test"])],
        ..config(&server)
    })
    .unwrap();
    assert!(get(&elsewhere, &url).await.is_err(), "not for localhost");
}

#[tokio::test]
async fn websockets_and_grpc_present_it_too() {
    let server = TestServer::start().await.unwrap();
    let client = Client::new(ClientConfig {
        client_certs: vec![cert(&server, &["localhost"])],
        ..config(&server)
    })
    .unwrap();
    let ws_url = server.mtls_url("/ws/echo").replacen("https", "wss", 1);
    let ws = client
        .websocket(&WsRequest::new(&ws_url).unwrap())
        .await
        .unwrap();
    assert_eq!(ws.status, 101);

    let proto = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../k9-testserver/proto/k9/test/echo.proto"
    );
    let method = Protos::load(&[proto], &[] as &[&str])
        .unwrap()
        .method("k9.test.Echo/Say")
        .unwrap();
    let url = target(&format!("localhost:{}", server.mtls_addr().port()), false).unwrap();
    let request = GrpcRequest::new(&url, &method, &serde_json::json!({"text": "mtls"})).unwrap();
    let res = client.grpc().grpc_invoke(&request, None).await.unwrap();
    assert_eq!(res.status, 0, "{}", res.status_message);
}

#[tokio::test]
async fn versions_and_cipher_suites_can_be_chosen() {
    let server = TestServer::start().await.unwrap();
    let url = server.https_url("/ok");
    let tls = |config: ClientConfig| async {
        let res = get(&Client::new(config).unwrap(), &url).await.unwrap();
        res.tls.expect("an HTTPS response")
    };
    let default = tls(config(&server)).await;
    assert_eq!(default.version, "tls1.3");
    assert!(
        default.cipher_suite.starts_with("TLS_AES_")
            || default.cipher_suite.starts_with("TLS_CHACHA20")
    );
    let twelve = tls(ClientConfig {
        tls_versions: Some((TlsVersion::Tls12, TlsVersion::Tls12)),
        cipher_suites: vec!["TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256".into()],
        ..config(&server)
    })
    .await;
    assert_eq!(
        (twelve.version, twelve.cipher_suite),
        ("tls1.2", "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256")
    );
    let thirteen = tls(ClientConfig {
        cipher_suites: vec!["TLS_AES_256_GCM_SHA384".into()],
        ..config(&server)
    })
    .await;
    assert_eq!(thirteen.cipher_suite, "TLS_AES_256_GCM_SHA384");
    let plain = get(&Client::new(config(&server)).unwrap(), &server.url("/ok"))
        .await
        .unwrap();
    assert!(plain.tls.is_none());
}

#[test]
fn bad_tls_options_are_configuration_errors() {
    let err = "tls1.0".parse::<TlsVersion>().unwrap_err();
    assert!(err.contains("tls1.2"), "{err}");
    assert_eq!("tls1.3".parse::<TlsVersion>(), Ok(TlsVersion::Tls13));
    let config_error = |config: ClientConfig| match Client::new(config) {
        Err(HttpError::Config(message)) => message,
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("accepted"),
    };
    let message = config_error(ClientConfig {
        cipher_suites: vec!["TLS_NOPE".into()],
        ..ClientConfig::default()
    });
    assert!(message.contains("TLS_NOPE"), "{message}");
    let message = config_error(ClientConfig {
        tls_versions: Some((TlsVersion::Tls13, TlsVersion::Tls12)),
        ..ClientConfig::default()
    });
    assert!(message.contains("tls1.3"), "{message}");
    let message = config_error(ClientConfig {
        client_certs: vec![ClientCert {
            domains: vec![],
            cert_pem: "not a certificate".into(),
            key_pem: "nor a key".into(),
        }],
        ..ClientConfig::default()
    });
    assert!(message.contains("client certificate"), "{message}");
}
