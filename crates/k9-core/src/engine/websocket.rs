//! A WebSocket opened by script code, recording k6's `ws_*` metrics as it's
//! used: `ws_msgs_sent`/`ws_msgs_received` per message, `ws_ping` per pong,
//! socket bytes as `data_sent`/`data_received`, and `ws_session_duration`
//! when it ends (closed, failed or dropped).

use std::sync::Arc;
use std::time::Instant;

use k9_http::{HttpError, WebSocket, WsEvent, WsMessage};
use k9_metrics::TagSetId;

use crate::engine::vu::Shared;

pub struct WsSession {
    ws: WebSocket,
    shared: Arc<Shared>,
    tags: TagSetId,
    data_tags: TagSetId,
    started: Instant,
    ended: bool,
}

impl std::fmt::Debug for WsSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsSession").field("ws", &self.ws).finish()
    }
}

impl WsSession {
    pub(crate) fn new(
        ws: WebSocket,
        shared: Arc<Shared>,
        tags: TagSetId,
        data_tags: TagSetId,
        started: Instant,
    ) -> WsSession {
        let b = &shared.b;
        shared.metrics.record(b.ws_sessions, tags, 1.0);
        shared
            .metrics
            .record_duration(b.ws_connecting, tags, ws.connecting);
        let session = WsSession {
            ws,
            shared,
            tags,
            data_tags,
            started,
            ended: false,
        };
        session.record_data();
        session
    }

    /// The handshake's details (status, headers, URL, remote address).
    pub fn socket(&self) -> &WebSocket {
        &self.ws
    }

    pub async fn send(&mut self, message: WsMessage) -> Result<(), HttpError> {
        let sent = self.ws.send(message).await;
        match &sent {
            Ok(()) => self
                .shared
                .metrics
                .record(self.shared.b.ws_msgs_sent, self.tags, 1.0),
            Err(e) => self.failed(e),
        }
        self.record_data();
        sent
    }

    pub async fn ping(&mut self) -> Result<(), HttpError> {
        let sent = self.ws.ping().await;
        if let Err(e) = &sent {
            self.failed(e);
        }
        self.record_data();
        sent
    }

    /// The next message, pong or close; `None` once the session is over.
    pub async fn recv(&mut self) -> Option<Result<WsEvent, HttpError>> {
        let event = self.ws.recv().await;
        let (metrics, b) = (&self.shared.metrics, &self.shared.b);
        match &event {
            Some(Ok(WsEvent::Message(_))) => metrics.record(b.ws_msgs_received, self.tags, 1.0),
            Some(Ok(WsEvent::Pong(rtt))) => metrics.record_duration(b.ws_ping, self.tags, *rtt),
            Some(Ok(WsEvent::Close(_))) => {}
            Some(Err(e)) => {
                let e = e.to_string();
                self.shared.record_request_error(e);
                self.end();
            }
            None => self.end(),
        }
        self.record_data();
        event
    }

    pub async fn close(&mut self, code: u16, reason: &str) -> Result<(), HttpError> {
        let closed = self.ws.close(code, reason).await;
        if let Err(e) = &closed {
            self.failed(e);
        }
        self.record_data();
        closed
    }

    fn failed(&mut self, error: &HttpError) {
        self.shared.record_request_error(error.to_string());
        self.end();
    }

    fn record_data(&self) {
        let (sent, received) = self.ws.take_bytes();
        if sent > 0 || received > 0 {
            self.shared.record_data(self.data_tags, sent, received);
        }
    }

    fn end(&mut self) {
        if !self.ended {
            self.ended = true;
            self.shared.metrics.record_duration(
                self.shared.b.ws_session_duration,
                self.tags,
                self.started.elapsed(),
            );
        }
    }
}

impl Drop for WsSession {
    fn drop(&mut self) {
        self.record_data();
        self.end();
    }
}
