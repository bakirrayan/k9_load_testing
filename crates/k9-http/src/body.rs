//! The request body, which notes when its last byte was handed to hyper.
//!
//! On HTTP/1.1 the socket's own write marks time the sending phase. HTTP/2
//! multiplexes streams on one socket, so a stream's sending phase ends when
//! hyper has taken its last chunk instead. Chunks are cut to one frame's
//! worth so that hyper, which waits for flow-control capacity before taking
//! the next one, paces them the way the peer lets the body through.
//!
//! A streaming body (gRPC streams) takes its data from a channel instead,
//! and ends when the channel closes.

use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use tokio::sync::mpsc;

/// HTTP/2's default maximum frame size.
const CHUNK: usize = 16 * 1024;

pub(crate) struct TimedBody {
    data: Bytes,
    chunk: usize,
    sent: Arc<OnceLock<Instant>>,
    /// More data, for a streaming body; `None` once closed.
    more: Option<mpsc::Receiver<Bytes>>,
}

impl TimedBody {
    /// `chunked` cuts the body into frame-sized chunks (for HTTP/2).
    pub fn new(data: Bytes, chunked: bool) -> (TimedBody, Arc<OnceLock<Instant>>) {
        let sent = Arc::new(OnceLock::new());
        let body = TimedBody {
            data,
            chunk: if chunked { CHUNK } else { usize::MAX },
            sent: sent.clone(),
            more: None,
        };
        (body, sent)
    }

    /// A body sending what arrives on `more`, until it closes.
    pub fn streaming(more: mpsc::Receiver<Bytes>) -> TimedBody {
        TimedBody {
            data: Bytes::new(),
            chunk: CHUNK,
            sent: Arc::default(),
            more: Some(more),
        }
    }
}

impl Body for TimedBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        while self.data.is_empty() {
            let Some(more) = &mut self.more else {
                return Poll::Ready(None);
            };
            match more.poll_recv(cx) {
                Poll::Ready(Some(data)) => self.data = data,
                Poll::Ready(None) => self.more = None,
                Poll::Pending => return Poll::Pending,
            }
        }
        let n = self.chunk.min(self.data.len());
        let chunk = self.data.split_to(n);
        if self.data.is_empty() && self.more.is_none() {
            let _ = self.sent.set(Instant::now());
        }
        Poll::Ready(Some(Ok(Frame::data(chunk))))
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_empty() && self.more.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        match self.more {
            Some(_) => SizeHint::default(),
            None => SizeHint::with_exact(self.data.len() as u64),
        }
    }
}
