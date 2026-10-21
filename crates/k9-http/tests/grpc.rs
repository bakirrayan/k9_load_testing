//! Unary gRPC against the test server's tonic service: protos compiled at
//! run time, JSON in and out, status from trailers, one HTTP/2 connection.

use std::time::Duration;

use k9_http::grpc::{GrpcError, GrpcRequest, GrpcResponse, Protos, status_name, target};
use k9_http::{Client, ClientConfig, HttpError};
use k9_testserver::TestServer;
use serde_json::json;

const PROTO: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../k9-testserver/proto/k9/test/echo.proto"
);

fn protos() -> Protos {
    Protos::load(&[PROTO], &[] as &[&str]).unwrap()
}

fn grpc_client() -> Client {
    Client::new(ClientConfig::default()).unwrap().grpc()
}

fn say(server: &TestServer, message: serde_json::Value) -> GrpcRequest {
    let method = protos().method("k9.test.Echo/Say").unwrap();
    let url = target(&server.http_addr().to_string(), true).unwrap();
    GrpcRequest::new(&url, &method, &message).unwrap()
}

#[tokio::test]
async fn a_unary_call_round_trips_json_over_h2c() {
    let server = TestServer::start().await.unwrap();
    let mut request = say(&server, json!({"text": "hello grpc world"}));
    assert_eq!(request.url.path(), "/k9.test.Echo/Say");
    request.metadata.insert("x-user", "vu1".parse().unwrap());
    let res = grpc_client().grpc_invoke(&request, None).await.unwrap();
    assert_eq!(res.status, 0, "{}", res.status_message);
    assert_eq!(
        res.message,
        Some(json!({
            "text": "hello grpc world",
            "metadata": {"x-user": "vu1"},
            "length": "16",
            "empty": false,
            "kind": "KIND_LONG",
            "words": ["hello", "grpc", "world"],
        }))
    );
    assert_eq!(res.headers["x-served-by"], "k9-testserver");
    assert_eq!(res.trailers["grpc-status"], "0");
    assert!(res.timings.connecting > Duration::ZERO);
    assert!(res.timings.waiting > Duration::ZERO);
    assert!(res.bytes_sent > 0 && res.bytes_received > 0);
}

#[tokio::test]
async fn concurrent_calls_share_one_http2_connection() {
    let server = TestServer::start().await.unwrap();
    let client = grpc_client();
    let request = say(&server, json!({"text": "hi", "delayMs": 50}));
    // The first call connects; the next ones, concurrent, multiplex on it.
    assert_eq!(client.grpc_invoke(&request, None).await.unwrap().status, 0);
    let calls: Vec<_> = (0..5)
        .map(|_| {
            let (client, request) = (client.clone(), request.clone());
            tokio::spawn(async move { client.grpc_invoke(&request, None).await })
        })
        .collect();
    for call in calls {
        assert_eq!(call.await.unwrap().unwrap().status, 0);
    }
    assert_eq!(server.connections(), 1);
}

#[tokio::test]
async fn a_failed_call_carries_its_status_and_message() {
    let server = TestServer::start().await.unwrap();
    let request = say(&server, json!({"text": "no such user: zoë", "status": 5}));
    let res = grpc_client().grpc_invoke(&request, None).await.unwrap();
    assert_eq!(res.status, 5);
    assert_eq!(status_name(res.status), "NOT_FOUND");
    assert_eq!(res.status_message, "no such user: zoë");
    assert_eq!(res.message, None);
}

#[tokio::test]
async fn a_call_past_its_deadline_fails_with_deadline_exceeded() {
    let server = TestServer::start().await.unwrap();
    let request = say(&server, json!({"delay_ms": 500}));
    let started = std::time::Instant::now();
    let err = grpc_client()
        .grpc_invoke(&request, Some(Duration::from_millis(100)))
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_millis(400));
    assert!(matches!(err, HttpError::Timeout(_)), "{err:?}");
    let res = GrpcResponse::from_error(&err);
    assert_eq!(
        (res.status, status_name(res.status)),
        (4, "DEADLINE_EXCEEDED")
    );
    assert_eq!(
        GrpcResponse::from_error(&HttpError::WebSocket("x".into())).status,
        14
    );
}

#[tokio::test]
async fn tls_offers_h2_in_alpn() {
    let server = TestServer::start().await.unwrap();
    let client = Client::new(ClientConfig {
        extra_root_certs: vec![server.cert_der().clone()],
        ..ClientConfig::default()
    })
    .unwrap()
    .grpc();
    let method = protos().method("/k9.test.Echo/Say").unwrap();
    let url = target(&format!("localhost:{}", server.https_addr().port()), false).unwrap();
    let request = GrpcRequest::new(&url, &method, &json!({"text": "secure"})).unwrap();
    let res = client.grpc_invoke(&request, None).await.unwrap();
    assert_eq!(res.status, 0, "{}", res.status_message);
    assert!(res.timings.tls_handshaking > Duration::ZERO);
}

#[test]
fn mistakes_are_reported_before_sending() {
    let protos = protos();
    let err = protos.method("k9.test.Echo/Shout").unwrap_err();
    assert!(
        matches!(&err, GrpcError::UnknownMethod { .. })
            && err.to_string().contains("k9.test.Echo/Say"),
        "{err}"
    );
    assert!(protos.method("nope").is_err());
    let url = target("localhost:1", true).unwrap();
    let chat = protos.method("k9.test.Echo/Chat").unwrap();
    let err = GrpcRequest::new(&url, &chat, &json!({})).unwrap_err();
    assert!(err.to_string().contains("streaming"), "{err}");
    let say = protos.method("k9.test.Echo/Say").unwrap();
    let err = GrpcRequest::new(&url, &say, &json!({"txt": "hi"})).unwrap_err();
    assert!(err.to_string().contains("txt"), "{err}");
    let err = Protos::load(&["missing.proto"], &[] as &[&str]).unwrap_err();
    assert!(err.to_string().contains("missing.proto"), "{err}");
}

#[test]
fn targets_are_host_and_port_or_urls() {
    assert_eq!(target("h:50051", true).unwrap().as_str(), "http://h:50051/");
    assert_eq!(
        target("h:50051", false).unwrap().as_str(),
        "https://h:50051/"
    );
    assert_eq!(
        target("https://h:1", true).unwrap().as_str(),
        "https://h:1/"
    );
    assert!(target("ftp://h:1", true).is_err());
    assert!(target("", true).is_err());
}

#[tokio::test]
async fn reflection_finds_the_servers_methods() {
    let server = TestServer::start().await.unwrap();
    let client = grpc_client();
    let url = target(&server.http_addr().to_string(), true).unwrap();
    let protos = Protos::reflect(&client, &url, None).await.unwrap();
    assert_eq!(protos.methods(), ["k9.test.Echo/Say", "k9.test.Echo/Chat"]);
    let method = protos.method("k9.test.Echo/Say").unwrap();
    let request = GrpcRequest::new(&url, &method, &json!({"text": "reflected"})).unwrap();
    let res = client.grpc_invoke(&request, None).await.unwrap();
    assert_eq!(res.message.unwrap()["kind"], "KIND_SHORT");
}

/// A tonic server with the Echo service and, if `v1alpha`, only the older
/// reflection service.
async fn tonic_server(v1alpha: bool) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let echo = k9_testserver::grpc::EchoServer::new(k9_testserver::grpc::Echo);
    let reflection = v1alpha.then(|| k9_testserver::grpc::reflection().1);
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(echo)
            .add_optional_service(reflection)
            .serve_with_incoming(incoming),
    );
    addr
}

#[tokio::test]
async fn reflection_falls_back_to_v1alpha() {
    let url = target(&tonic_server(true).await, true).unwrap();
    let protos = Protos::reflect(&grpc_client(), &url, None).await.unwrap();
    assert!(protos.method("k9.test.Echo/Say").is_ok());
}

#[tokio::test]
async fn a_server_without_reflection_is_reported() {
    let url = target(&tonic_server(false).await, true).unwrap();
    let err = Protos::reflect(&grpc_client(), &url, None)
        .await
        .unwrap_err();
    assert!(matches!(err, GrpcError::Reflection(_)), "{err:?}");
    assert!(err.to_string().contains("UNIMPLEMENTED"), "{err}");
    let down = target("127.0.0.1:1", true).unwrap();
    let err = Protos::reflect(&grpc_client(), &down, Some(Duration::from_millis(300)))
        .await
        .unwrap_err();
    assert!(matches!(err, GrpcError::Reflection(_)), "{err:?}");
}

mod streams {
    use super::*;
    use k9_http::grpc::{GrpcEvent, GrpcStream};

    async fn chat(server: &TestServer, timeout: Option<Duration>) -> GrpcStream {
        let method = protos().method("k9.test.Echo/Chat").unwrap();
        let url = target(&server.http_addr().to_string(), true).unwrap();
        let mut metadata = http::HeaderMap::new();
        metadata.insert("x-user", "s1".parse().unwrap());
        grpc_client()
            .grpc_stream(&url, &method, metadata, timeout)
            .await
    }

    async fn message(stream: &mut GrpcStream) -> serde_json::Value {
        match stream.recv().await {
            Some(GrpcEvent::Message(message)) => message,
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_bidirectional_stream_interleaves_messages() {
        let server = TestServer::start().await.unwrap();
        let mut stream = chat(&server, None).await;
        stream.send(&json!({"text": "one"})).await.unwrap();
        assert_eq!(message(&mut stream).await["text"], "one");
        stream.send(&json!({"text": "two"})).await.unwrap();
        let two = message(&mut stream).await;
        assert_eq!(
            (&two["text"], &two["metadata"]["x-user"]),
            (&json!("two"), &json!("s1"))
        );
        stream.close_send();
        assert!(stream.send(&json!({})).await.is_err(), "closed for sending");
        match stream.recv().await {
            Some(GrpcEvent::End(end)) => {
                assert_eq!(end.status, 0, "{}", end.status_message);
                assert!(end.answered);
                assert_eq!(end.trailers["grpc-status"], "0");
                assert!(end.bytes_sent > 0 && end.bytes_received > 0);
            }
            other => panic!("{other:?}"),
        }
        assert!(stream.recv().await.is_none());
    }

    #[tokio::test]
    async fn a_stream_ends_with_the_servers_error_status() {
        let server = TestServer::start().await.unwrap();
        let mut stream = chat(&server, None).await;
        stream.send(&json!({"text": "fine"})).await.unwrap();
        stream
            .send(&json!({"text": "out of stock", "status": 9}))
            .await
            .unwrap();
        stream.send(&json!({"text": "unanswered"})).await.unwrap();
        assert_eq!(message(&mut stream).await["text"], "fine");
        match stream.recv().await {
            Some(GrpcEvent::End(end)) => {
                assert_eq!(
                    (end.status, end.status_message.as_str()),
                    (9, "out of stock")
                );
                assert!(end.answered);
            }
            other => panic!("{other:?}"),
        }
        let err = stream.send(&json!({"txt": 1})).await.unwrap_err();
        assert!(err.to_string().contains("txt"), "{err}");
    }

    #[tokio::test]
    async fn a_stream_past_its_deadline_ends_with_deadline_exceeded() {
        let server = TestServer::start().await.unwrap();
        let started = std::time::Instant::now();
        let mut stream = chat(&server, Some(Duration::from_millis(100))).await;
        stream
            .send(&json!({"text": "slow", "delayMs": 1000}))
            .await
            .unwrap();
        match stream.recv().await {
            Some(GrpcEvent::End(end)) => {
                assert_eq!(end.status, 4);
                assert!(!end.answered);
            }
            other => panic!("{other:?}"),
        }
        assert!(started.elapsed() < Duration::from_millis(600));
    }

    #[tokio::test]
    async fn a_stream_that_cant_connect_ends_unavailable() {
        let refused = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let method = protos().method("k9.test.Echo/Chat").unwrap();
        let url = target(&refused.to_string(), true).unwrap();
        let mut stream = grpc_client()
            .grpc_stream(&url, &method, http::HeaderMap::new(), None)
            .await;
        assert!(stream.send(&json!({"text": "x"})).await.is_err());
        match stream.recv().await {
            Some(GrpcEvent::End(end)) => {
                assert_eq!(end.status, 14);
                assert!(
                    !end.answered && end.status_message.contains("connect"),
                    "{end:?}"
                );
            }
            other => panic!("{other:?}"),
        }
    }
}

mod compression_and_sizes {
    use super::*;
    use k9_http::grpc::{GrpcEvent, GrpcOptions};

    fn big() -> String {
        "k9 ".repeat(7000)
    }

    #[tokio::test]
    async fn gzip_shrinks_messages_both_ways() {
        let server = TestServer::start().await.unwrap();
        let client = grpc_client();
        let plain = client
            .grpc_invoke(&say(&server, json!({"text": big()})), None)
            .await
            .unwrap();
        assert_eq!(plain.status, 0, "{}", plain.status_message);
        assert!(plain.bytes_sent > 21_000, "{}", plain.bytes_sent);
        assert!(
            plain.bytes_received < 2_000,
            "the server compresses its answers: {}",
            plain.bytes_received
        );
        assert_eq!(plain.message.unwrap()["text"], big());

        let mut request = say(&server, json!({"text": big()}));
        request.options.gzip = true;
        let res = client.grpc_invoke(&request, None).await.unwrap();
        assert_eq!(res.status, 0, "{}", res.status_message);
        assert!(res.bytes_sent < 2_000, "{}", res.bytes_sent);
        assert_eq!(res.message.unwrap()["length"], "21000");
    }

    #[tokio::test]
    async fn messages_over_the_limits_are_refused() {
        let server = TestServer::start().await.unwrap();
        let client = grpc_client();
        let mut request = say(&server, json!({"text": big()}));
        request.options.max_receive_size = Some(1_000);
        let res = client.grpc_invoke(&request, None).await.unwrap();
        assert_eq!(status_name(res.status), "RESOURCE_EXHAUSTED");
        assert!(
            res.status_message.contains("larger than max"),
            "{}",
            res.status_message
        );
        assert_eq!(res.message, None);

        let mut request = say(&server, json!({"text": big()}));
        request.options = GrpcOptions {
            max_send_size: Some(100),
            ..GrpcOptions::default()
        };
        let res = client.grpc_invoke(&request, None).await.unwrap();
        assert_eq!(status_name(res.status), "RESOURCE_EXHAUSTED");
        assert!(
            res.status_message.contains("21004 vs. 100"),
            "{}",
            res.status_message
        );
        assert_eq!(res.bytes_sent, 0, "nothing was sent");

        let mut request = say(&server, json!({"text": "small"}));
        request.options.max_send_size = Some(100);
        request.options.max_receive_size = Some(100);
        let res = client.grpc_invoke(&request, None).await.unwrap();
        assert_eq!(res.status, 0, "{}", res.status_message);
    }

    #[tokio::test]
    async fn streams_compress_and_check_sizes() {
        let server = TestServer::start().await.unwrap();
        let method = protos().method("k9.test.Echo/Chat").unwrap();
        let url = target(&server.http_addr().to_string(), true).unwrap();
        let gzip = GrpcOptions {
            gzip: true,
            ..GrpcOptions::default()
        };
        let mut stream = grpc_client()
            .grpc_stream_with(&url, &method, http::HeaderMap::new(), None, &gzip)
            .await;
        stream.send(&json!({"text": big()})).await.unwrap();
        stream.close_send();
        let Some(GrpcEvent::Message(message)) = stream.recv().await else {
            panic!("no message");
        };
        assert_eq!(message["text"], big());
        let Some(GrpcEvent::End(end)) = stream.recv().await else {
            panic!("no end");
        };
        assert_eq!(end.status, 0, "{}", end.status_message);
        assert!(end.bytes_sent < 2_000, "{}", end.bytes_sent);

        let limited = GrpcOptions {
            max_receive_size: Some(1_000),
            max_send_size: Some(30_000),
            ..GrpcOptions::default()
        };
        let mut stream = grpc_client()
            .grpc_stream_with(&url, &method, http::HeaderMap::new(), None, &limited)
            .await;
        let too_big = json!({"text": "x".repeat(40_000)});
        assert!(matches!(
            stream.send(&too_big).await,
            Err(GrpcError::TooLarge { .. })
        ));
        stream.send(&json!({"text": big()})).await.unwrap();
        let Some(GrpcEvent::End(end)) = stream.recv().await else {
            panic!("the oversized answer ends the stream");
        };
        assert_eq!(
            status_name(end.status),
            "RESOURCE_EXHAUSTED",
            "{}",
            end.status_message
        );
    }
}
