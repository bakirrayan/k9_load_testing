//! WebSockets: the upgrade handshake (timed), messages both ways, closes from
//! either side, pings, and wss.

use std::time::Duration;

use bytes::Bytes;
use http::StatusCode;
use k9_http::{Client, ClientConfig, WsEvent, WsMessage, WsRequest};
use k9_testserver::TestServer;

async fn setup() -> (TestServer, Client) {
    let server = TestServer::start().await.unwrap();
    let mut config = ClientConfig::default();
    config.extra_root_certs.push(server.cert_der().clone());
    (server, Client::new(config).unwrap())
}

fn ws_url(server: &TestServer, path: &str) -> String {
    server.url(path).replacen("http://", "ws://", 1)
}

#[tokio::test]
async fn messages_go_both_ways_after_a_timed_handshake() {
    let (server, client) = setup().await;
    let request = WsRequest::new(&ws_url(&server, "/ws/echo")).unwrap();
    let mut ws = client.websocket(&request).await.unwrap();
    assert_eq!(ws.status, StatusCode::SWITCHING_PROTOCOLS);
    assert!(ws.connecting > Duration::ZERO);
    assert!(ws.remote_addr.ip().is_loopback());

    ws.send(WsMessage::Text("hello".into())).await.unwrap();
    ws.send(WsMessage::Binary(Bytes::from_static(&[1, 2, 3])))
        .await
        .unwrap();
    assert_eq!(
        ws.recv().await.unwrap().unwrap(),
        WsEvent::Message(WsMessage::Text("hello".into()))
    );
    assert_eq!(
        ws.recv().await.unwrap().unwrap(),
        WsEvent::Message(WsMessage::Binary(Bytes::from_static(&[1, 2, 3])))
    );
    let (sent, received) = ws.take_bytes();
    assert!(sent > 0 && received > 0, "handshake and frames count");
    assert_eq!(ws.take_bytes(), (0, 0), "taken once");

    ws.close(1000, "done").await.unwrap();
    assert_eq!(
        ws.recv().await.unwrap().unwrap(),
        WsEvent::Close(Some((1000, "done".into()))),
        "the server echoes our close"
    );
    assert!(ws.recv().await.is_none());
}

#[tokio::test]
async fn the_server_can_close_first() {
    let (server, client) = setup().await;
    let request = WsRequest::new(&ws_url(&server, "/ws/echo")).unwrap();
    let mut ws = client.websocket(&request).await.unwrap();
    ws.send(WsMessage::Text("close".into())).await.unwrap();
    assert_eq!(
        ws.recv().await.unwrap().unwrap(),
        WsEvent::Close(Some((4000, "bye".into())))
    );
    assert!(ws.recv().await.is_none());
}

#[tokio::test]
async fn pushed_messages_arrive_in_order() {
    let (server, client) = setup().await;
    let request = WsRequest::new(&ws_url(&server, "/ws/push/3")).unwrap();
    let mut ws = client.websocket(&request).await.unwrap();
    let mut texts = Vec::new();
    while let Some(event) = ws.recv().await {
        match event.unwrap() {
            WsEvent::Message(WsMessage::Text(text)) => texts.push(text),
            WsEvent::Close(frame) => assert_eq!(frame.map(|f| f.0), Some(1000)),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(texts, ["1", "2", "3"]);
}

#[tokio::test]
async fn pings_report_their_round_trip() {
    let (server, client) = setup().await;
    let request = WsRequest::new(&ws_url(&server, "/ws/echo")).unwrap();
    let mut ws = client.websocket(&request).await.unwrap();
    ws.ping().await.unwrap();
    match ws.recv().await.unwrap().unwrap() {
        WsEvent::Pong(rtt) => assert!(rtt > Duration::ZERO && rtt < Duration::from_secs(1)),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn wss_uses_tls_and_http1_even_when_the_server_offers_http2() {
    let (server, client) = setup().await;
    let url = server
        .https_url("/ws/echo")
        .replacen("https://", "wss://", 1);
    let mut request = WsRequest::new(&url).unwrap();
    request
        .headers
        .insert("x-test", http::HeaderValue::from_static("1"));
    let mut ws = client.websocket(&request).await.unwrap();
    ws.send(WsMessage::Text("secure".into())).await.unwrap();
    assert_eq!(
        ws.recv().await.unwrap().unwrap(),
        WsEvent::Message(WsMessage::Text("secure".into()))
    );
}

#[tokio::test]
async fn a_refused_upgrade_reports_the_status() {
    let (server, client) = setup().await;
    let request = WsRequest::new(&ws_url(&server, "/status/403")).unwrap();
    let err = client
        .websocket(&request)
        .await
        .expect_err("not a WebSocket");
    assert!(err.to_string().contains("403"), "{err}");
    let err =
        WsRequest::new(&server.url("/ws/echo").replacen("http", "ftp", 1)).expect_err("bad scheme");
    assert!(err.to_string().contains("ws"), "{err}");
}
