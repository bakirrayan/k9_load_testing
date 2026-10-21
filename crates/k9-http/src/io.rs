//! A stream wrapper that timestamps I/O, used to split a request into k6's
//! sending / waiting / receiving phases.
//!
//! It wraps the *plaintext* stream (outside TLS), so TLS records that carry no
//! application data (such as TLS 1.3 session tickets) never count as the first
//! response byte.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// I/O marks for the request currently in flight on a connection. HTTP/1.1
/// runs one request at a time per connection, so the client resets them before
/// each request and reads them after. HTTP/2 streams share the connection, so
/// only its byte counts are used: each finished stream takes what accumulated
/// since the last one did ([`IoMarks::take_bytes`]).
#[derive(Debug, Default)]
pub(crate) struct IoMarks(Mutex<Marks>);

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Marks {
    pub last_write: Option<Instant>,
    pub first_read: Option<Instant>,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

impl IoMarks {
    fn lock(&self) -> MutexGuard<'_, Marks> {
        // Marks are plain data; a panic elsewhere can't leave them inconsistent.
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn reset(&self) {
        *self.lock() = Marks::default();
    }

    pub fn get(&self) -> Marks {
        *self.lock()
    }

    /// Bytes (written, read) since the last call, which start again from zero.
    pub fn take_bytes(&self) -> (u64, u64) {
        let mut m = self.lock();
        let taken = (m.bytes_written, m.bytes_read);
        m.bytes_written = 0;
        m.bytes_read = 0;
        taken
    }

    fn wrote(&self, n: usize) {
        if n > 0 {
            let mut m = self.lock();
            m.last_write = Some(Instant::now());
            m.bytes_written += n as u64;
        }
    }

    fn read(&self, n: usize) {
        if n > 0 {
            let mut m = self.lock();
            m.first_read.get_or_insert_with(Instant::now);
            m.bytes_read += n as u64;
        }
    }
}

pub(crate) struct TimedStream<S> {
    inner: S,
    marks: Arc<IoMarks>,
}

impl<S> TimedStream<S> {
    pub fn new(inner: S, marks: Arc<IoMarks>) -> Self {
        TimedStream { inner, marks }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for TimedStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = poll {
            self.marks.read(buf.filled().len() - before);
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for TimedStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = poll {
            self.marks.wrote(n);
        }
        poll
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(n)) = poll {
            self.marks.wrote(n);
        }
        poll
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
