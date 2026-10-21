//! WebSockets: the HTTP/1.1 upgrade (dialed and timed like a request's
//! connection, never pooled), then messages both ways on tungstenite.
//! Bytes are counted on the socket, handshake included.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::header::USER_AGENT;
use http::{HeaderMap, StatusCode};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{self, Message};
use url::Url;

use crate::connect::{Dialed, MaybeTls, dial};
use crate::io::{IoMarks, TimedStream};
use crate::{Client, HttpError, HttpVersion, pool_key};

/// A WebSocket to open: a `ws://` or `wss://` URL and extra handshake headers.
#[derive(Debug, Clone)]
pub struct WsRequest {
    pub url: Url,
    pub headers: HeaderMap,
}

impl WsRequest {
    pub fn new(url: &str) -> Result<WsRequest, HttpError> {
        let invalid = |reason: String| HttpError::InvalidUrl {
            url: url.to_owned(),
            reason,
        };
        let parsed = Url::parse(url).map_err(|e| invalid(e.to_string()))?;
        if !matches!(parsed.scheme(), "ws" | "wss") {
            return Err(invalid(format!(
                "unsupported scheme `{}` (use ws or wss)",
                parsed.scheme()
            )));
        }
        if parsed.host_str().is_none_or(str::is_empty) {
            return Err(invalid("missing host".to_owned()));
        }
        Ok(WsRequest {
            url: parsed,
            headers: HeaderMap::new(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsMessage {
    Text(String),
    Binary(Bytes),
}

/// What [`WebSocket::recv`] yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsEvent {
    Message(WsMessage),
    /// The answer to one of our [`WebSocket::ping`]s, with its round trip.
    Pong(Duration),
    /// The peer's close frame (code and reason), if it sent one. Answering
    /// it is automatic; nothing follows.
    Close(Option<(u16, String)>),
}

type Stream = WebSocketStream<TimedStream<MaybeTls>>;

/// An open WebSocket.
pub struct WebSocket {
    /// The handshake's status: always 101.
    pub status: StatusCode,
    /// The handshake response's headers.
    pub headers: HeaderMap,
    /// From the start of the DNS lookup to the handshake response (k6's
    /// `ws_connecting`).
    pub connecting: Duration,
    pub remote_addr: SocketAddr,
    pub url: Url,
    stream: Stream,
    marks: Arc<IoMarks>,
    /// Pings awaiting their pong: (payload, when sent).
    pings: VecDeque<(u64, Instant)>,
    next_ping: u64,
}

impl std::fmt::Debug for WebSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocket")
            .field("url", &self.url.as_str())
            .field("remote_addr", &self.remote_addr)
            .finish_non_exhaustive()
    }
}

impl WebSocket {
    pub async fn send(&mut self, message: WsMessage) -> Result<(), HttpError> {
        let message = match message {
            WsMessage::Text(text) => Message::text(text),
            WsMessage::Binary(data) => Message::binary(data),
        };
        self.stream.send(message).await.map_err(ws_error)
    }

    /// Sends a ping; its pong arrives from [`WebSocket::recv`] as
    /// [`WsEvent::Pong`] with the round trip.
    pub async fn ping(&mut self) -> Result<(), HttpError> {
        let id = self.next_ping;
        self.next_ping += 1;
        self.pings.push_back((id, Instant::now()));
        let payload = Bytes::copy_from_slice(&id.to_be_bytes());
        self.stream
            .send(Message::Ping(payload))
            .await
            .map_err(ws_error)
    }

    /// The next message, pong or close; `None` once the connection is closed.
    /// Cancel-safe.
    pub async fn recv(&mut self) -> Option<Result<WsEvent, HttpError>> {
        loop {
            let message = match self.stream.next().await? {
                Ok(message) => message,
                Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                    return None;
                }
                Err(e) => return Some(Err(ws_error(e))),
            };
            let event = match message {
                Message::Text(text) => WsEvent::Message(WsMessage::Text(text.to_string())),
                Message::Binary(data) => WsEvent::Message(WsMessage::Binary(data)),
                Message::Pong(payload) => match self.pong(&payload) {
                    Some(rtt) => WsEvent::Pong(rtt),
                    None => continue,
                },
                Message::Close(frame) => {
                    WsEvent::Close(frame.map(|f| (u16::from(f.code), f.reason.to_string())))
                }
                // Pings are answered by tungstenite.
                Message::Ping(_) | Message::Frame(_) => continue,
            };
            return Some(Ok(event));
        }
    }

    /// Starts the closing handshake; the peer's answer arrives from
    /// [`WebSocket::recv`] as [`WsEvent::Close`].
    pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), HttpError> {
        let frame = CloseFrame {
            code: code.into(),
            reason: reason.to_owned().into(),
        };
        match self.stream.close(Some(frame)).await {
            Ok(()) | Err(tungstenite::Error::ConnectionClosed) => Ok(()),
            Err(e) => Err(ws_error(e)),
        }
    }

    /// Socket bytes (written, read) since the last call.
    pub fn take_bytes(&self) -> (u64, u64) {
        self.marks.take_bytes()
    }

    /// The round trip of the ping `payload` answers; older unanswered pings
    /// are forgotten.
    fn pong(&mut self, payload: &[u8]) -> Option<Duration> {
        let id = u64::from_be_bytes(payload.try_into().ok()?);
        while let Some((sent_id, sent)) = self.pings.pop_front() {
            if sent_id == id {
                return Some(sent.elapsed());
            }
        }
        None
    }
}

impl Client {
    /// Opens a WebSocket (on a connection of its own), within the client's
    /// timeout.
    pub async fn websocket(&self, request: &WsRequest) -> Result<WebSocket, HttpError> {
        let timeout = self.shared.config.timeout;
        tokio::time::timeout(timeout, self.open_websocket(request))
            .await
            .map_err(|_| HttpError::Timeout(timeout))?
    }

    async fn open_websocket(&self, request: &WsRequest) -> Result<WebSocket, HttpError> {
        let started = Instant::now();
        let url = &request.url;
        let key = pool_key(url);
        let Dialed { stream, addr, .. } = dial(
            &key,
            &self.shared.resolver,
            self.local_ip,
            &self.shared.ws_tls,
            HttpVersion::Http1,
        )
        .await?;
        let marks = Arc::new(IoMarks::default());
        let io = TimedStream::new(stream, marks.clone());

        let mut handshake = url
            .as_str()
            .into_client_request()
            .map_err(|e| ws_failed(url, &e.to_string()))?;
        let headers = handshake.headers_mut();
        for (name, value) in &request.headers {
            headers.append(name, value.clone());
        }
        if let Some(ua) = &self.shared.user_agent
            && !headers.contains_key(USER_AGENT)
        {
            headers.insert(USER_AGENT, ua.clone());
        }
        let (stream, response) = tokio_tungstenite::client_async_with_config(handshake, io, None)
            .await
            .map_err(|e| match e {
                tungstenite::Error::Http(response) => ws_failed(
                    url,
                    &format!(
                        "the server answered {} instead of switching protocols",
                        response.status()
                    ),
                ),
                other => ws_failed(url, &other.to_string()),
            })?;
        Ok(WebSocket {
            status: response.status(),
            headers: response.headers().clone(),
            connecting: started.elapsed(),
            remote_addr: addr,
            url: url.clone(),
            stream,
            marks,
            pings: VecDeque::new(),
            next_ping: 0,
        })
    }
}

fn ws_failed(url: &Url, reason: &str) -> HttpError {
    HttpError::WebSocket(format!("{url}: {reason}"))
}

fn ws_error(e: tungstenite::Error) -> HttpError {
    HttpError::WebSocket(e.to_string())
}
