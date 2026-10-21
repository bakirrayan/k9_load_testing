//! Streaming calls (client, server or bidirectional): messages are sent as
//! they're written, on the client's shared HTTP/2 connection, and received
//! as they arrive, then the status ends the stream.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::{HeaderMap, StatusCode, Version};
use http_body_util::BodyExt;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use url::Url;

use super::{GrpcError, GrpcMethod, GrpcOptions, GrpcResponse, INTERNAL, payload, status_from};
use crate::body::TimedBody;
use crate::connect::Sender;
use crate::{Client, HttpError};

/// Messages written but not yet taken by the connection.
const SEND_QUEUE: usize = 16;

/// What a stream gives, in order: messages, then its end.
#[derive(Debug, Clone, PartialEq)]
pub enum GrpcEvent {
    Message(Value),
    End(GrpcEnd),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct GrpcEnd {
    /// The status code (0 is OK).
    pub status: u32,
    pub status_message: String,
    pub headers: HeaderMap,
    pub trailers: HeaderMap,
    /// Whether the server answered: false if the stream couldn't connect,
    /// broke, or passed its deadline.
    pub answered: bool,
    /// The connection's bytes while the stream was open (approximate when
    /// other streams share it).
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

impl GrpcEnd {
    fn failed(err: &HttpError) -> GrpcEnd {
        let res = GrpcResponse::from_error(err);
        GrpcEnd {
            status: res.status,
            status_message: res.status_message,
            ..GrpcEnd::default()
        }
    }
}

/// An open stream. Sending and receiving are independent: [`split`] it to
/// do both at once from different tasks.
///
/// [`split`]: GrpcStream::split
#[derive(Debug)]
pub struct GrpcStream {
    sender: GrpcSender,
    receiver: GrpcReceiver,
}

/// The sending half of a [`GrpcStream`].
#[derive(Debug)]
pub struct GrpcSender {
    method: GrpcMethod,
    options: GrpcOptions,
    tx: Option<mpsc::Sender<Bytes>>,
}

/// The receiving half of a [`GrpcStream`]; dropping it cancels the stream.
#[derive(Debug)]
pub struct GrpcReceiver {
    events: mpsc::Receiver<GrpcEvent>,
    reader: Option<JoinHandle<()>>,
}

impl GrpcStream {
    pub fn split(self) -> (GrpcSender, GrpcReceiver) {
        (self.sender, self.receiver)
    }

    pub async fn send(&mut self, message: &Value) -> Result<(), GrpcError> {
        self.sender.send(message).await
    }

    pub fn close_send(&mut self) {
        self.sender.close();
    }

    pub async fn recv(&mut self) -> Option<GrpcEvent> {
        self.receiver.recv().await
    }
}

impl GrpcSender {
    /// Encodes and sends `message`; `GrpcError::Closed` after [`close`]
    /// or once the stream ended, `GrpcError::TooLarge` (the stream goes on)
    /// for a message over `max_send_size`.
    ///
    /// [`close`]: GrpcSender::close
    pub async fn send(&mut self, message: &Value) -> Result<(), GrpcError> {
        let data = self.options.frame(&self.method.payload(message)?)?;
        let tx = self.tx.as_ref().ok_or(GrpcError::Closed)?;
        tx.send(data).await.map_err(|_| GrpcError::Closed)
    }

    /// Half-closes the stream: no more messages (the server may still answer).
    pub fn close(&mut self) {
        self.tx = None;
    }
}

impl GrpcReceiver {
    /// The next message or the end; `None` after the end.
    pub async fn recv(&mut self) -> Option<GrpcEvent> {
        let event = self.events.recv().await;
        if matches!(event, Some(GrpcEvent::End(_))) {
            self.reader = None;
        }
        event
    }
}

impl Drop for GrpcReceiver {
    fn drop(&mut self) {
        if let Some(reader) = self.reader.take() {
            reader.abort();
        }
    }
}

impl Client {
    /// Opens a stream to `method` on `target` (a base URL), on this
    /// client's shared HTTP/2 connection. `timeout` (default: the client's)
    /// is the whole stream's deadline. Never fails here: a stream that
    /// can't open ends at once with UNAVAILABLE.
    pub async fn grpc_stream(
        &self,
        target: &Url,
        method: &GrpcMethod,
        metadata: HeaderMap,
        timeout: Option<Duration>,
    ) -> GrpcStream {
        self.grpc_stream_with(target, method, metadata, timeout, &GrpcOptions::default())
            .await
    }

    /// [`Client::grpc_stream`], sending and receiving with `options`.
    pub async fn grpc_stream_with(
        &self,
        target: &Url,
        method: &GrpcMethod,
        metadata: HeaderMap,
        timeout: Option<Duration>,
        options: &GrpcOptions,
    ) -> GrpcStream {
        let timeout = timeout.unwrap_or(self.shared.config.timeout);
        let deadline = Instant::now() + timeout;
        let url = target
            .join(method.path())
            .expect("a path joins any base URL");
        let (event_tx, events) = mpsc::channel(SEND_QUEUE);
        let mut stream = GrpcStream {
            sender: GrpcSender {
                method: method.clone(),
                options: options.clone(),
                tx: None,
            },
            receiver: GrpcReceiver {
                events,
                reader: None,
            },
        };
        let fail = |err: HttpError| {
            let _ = event_tx.try_send(GrpcEvent::End(GrpcEnd::failed(&err)));
        };
        let conn = match tokio::time::timeout_at(deadline, self.h2_connection(&url)).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(err)) => {
                fail(err);
                return stream;
            }
            Err(_) => {
                fail(HttpError::Timeout(timeout));
                return stream;
            }
        };
        let Sender::H2(mut sender) = conn.sender else {
            unreachable!("h2_connection gives HTTP/2 connections");
        };
        let (body_tx, body_rx) = mpsc::channel(SEND_QUEUE);
        let request = self.grpc_request(&url, &metadata, Bytes::new(), timeout, options.gzip);
        let req = match self.build(&request, TimedBody::streaming(body_rx), Version::HTTP_2) {
            Ok(req) => req,
            Err(err) => {
                fail(err);
                return stream;
            }
        };
        let marks = conn.marks.clone();
        // Bytes from here on count for this stream.
        marks.take_bytes();
        let decoder = method.clone();
        let max_receive = options.max_receive_size;
        let reader = tokio::spawn(async move {
            let read = async {
                let res = sender.send_request(req).await?;
                read(res, &decoder, max_receive, &event_tx).await
            };
            let mut end = match tokio::time::timeout_at(deadline, read).await {
                Ok(Ok(Some(end))) => end,
                Ok(Ok(None)) => return, // nobody is listening
                Ok(Err(err)) => GrpcEnd::failed(&err),
                Err(_) => GrpcEnd::failed(&HttpError::Timeout(timeout)),
            };
            (end.bytes_sent, end.bytes_received) = marks.take_bytes();
            let _ = event_tx.send(GrpcEvent::End(end)).await;
        });
        stream.sender.tx = Some(body_tx);
        stream.receiver.reader = Some(reader);
        stream
    }
}

/// Forwards each message as it's complete, then gives the end (`None` if
/// the receiver is gone).
async fn read(
    res: http::Response<hyper::body::Incoming>,
    method: &GrpcMethod,
    max_receive: Option<usize>,
    events: &mpsc::Sender<GrpcEvent>,
) -> Result<Option<GrpcEnd>, HttpError> {
    let (parts, mut body) = res.into_parts();
    let encoding = parts
        .headers
        .get("grpc-encoding")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let mut buf = BytesMut::new();
    let mut trailers = HeaderMap::new();
    let mut broken = None;
    while let Some(frame) = body.frame().await {
        match frame?.into_data() {
            Ok(data) => {
                buf.extend_from_slice(&data);
                while let Some(next) = next_message(&mut buf) {
                    let message = payload(next.0, next.1, encoding.as_deref(), max_receive)
                        .and_then(|p| {
                            method.decode(&p).map_err(|reason| {
                                (INTERNAL, format!("invalid response message: {reason}"))
                            })
                        });
                    match message {
                        Ok(message) => {
                            if events.send(GrpcEvent::Message(message)).await.is_err() {
                                return Ok(None);
                            }
                        }
                        Err(unreadable) => {
                            broken = Some(unreadable);
                            break;
                        }
                    }
                }
                if broken.is_some() {
                    break;
                }
            }
            Err(frame) => {
                if let Ok(t) = frame.into_trailers() {
                    trailers.extend(t);
                }
            }
        }
    }
    let (status, status_message) = match broken {
        Some(unreadable) => unreadable,
        None if !buf.is_empty() && parts.status == StatusCode::OK => {
            (INTERNAL, format!("truncated message ({} bytes)", buf.len()))
        }
        None => status_from(parts.status, &parts.headers, &trailers),
    };
    Ok(Some(GrpcEnd {
        status,
        status_message,
        headers: parts.headers,
        trailers,
        answered: true,
        ..GrpcEnd::default()
    }))
}

/// Takes the next complete message out of `buf`, if there is one: its
/// compressed flag and its bytes.
fn next_message(buf: &mut BytesMut) -> Option<(u8, Bytes)> {
    let &[compressed, a, b, c, d] = buf.first_chunk::<5>()?;
    let len = u32::from_be_bytes([a, b, c, d]) as usize;
    if buf.len() < 5 + len {
        return None;
    }
    let mut message = buf.split_to(5 + len);
    Some((compressed, message.split_off(5).freeze()))
}
