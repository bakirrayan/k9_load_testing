//! `k9.ws` behind the scenes. Each open WebSocket is a tokio task that owns
//! the session: Python's `send`/`ping`/`close` reach it through an inbox (each
//! answered as a `ws_done` command), and everything it receives is pushed to
//! the event loop as `ws` commands, which `k9.ws.WebSocket` queues. Received
//! messages never wait for a `recv()` call, so a timed-out or cancelled
//! `recv()` loses nothing.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use k9_core::engine::{RequestMeta, ScriptContext, WsSession};
use k9_http::{WsEvent, WsMessage, WsRequest};
use pyo3::exceptions::{PyConnectionError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::script::{Command, Queue};

pub(crate) enum WsCommand {
    Send { job: u64, message: WsMessage },
    Ping { job: u64 },
    Close { job: u64, code: u16, reason: String },
}

/// What a session tells Python, in order.
pub(crate) enum WsNote {
    Message(WsMessage),
    Pong(Duration),
    /// The peer's close frame (code, reason), if any.
    Close(Option<(u16, String)>),
    /// The session is over: cleanly (`None`) or with an error.
    End(Option<String>),
}

/// The native side of `k9.ws.WebSocket`.
#[pyclass(frozen, name = "WebSocket", module = "k9._native")]
pub(crate) struct NativeWebSocket {
    #[pyo3(get)]
    session: u64,
    #[pyo3(get)]
    status: u16,
    #[pyo3(get)]
    headers: Vec<(String, String)>,
    inbox: mpsc::UnboundedSender<WsCommand>,
    jobs: Arc<AtomicU64>,
}

impl NativeWebSocket {
    fn enqueue(&self, command: impl FnOnce(u64) -> WsCommand) -> PyResult<u64> {
        let job = self.jobs.fetch_add(1, Ordering::Relaxed);
        self.inbox
            .send(command(job))
            .map_err(|_| PyConnectionError::new_err("the WebSocket is closed"))?;
        Ok(job)
    }
}

#[pymethods]
impl NativeWebSocket {
    /// Sends text (`str`) or binary (`bytes`); answered as a `ws_done` job.
    fn send(&self, data: &Bound<'_, PyAny>) -> PyResult<u64> {
        let message = if let Ok(text) = data.cast::<PyString>() {
            WsMessage::Text(text.to_str()?.to_owned())
        } else if let Ok(bytes) = data.cast::<PyBytes>() {
            WsMessage::Binary(bytes::Bytes::copy_from_slice(bytes.as_bytes()))
        } else {
            return Err(PyTypeError::new_err(format!(
                "a WebSocket sends str or bytes, not {}",
                data.get_type().name()?
            )));
        };
        self.enqueue(|job| WsCommand::Send { job, message })
    }

    fn ping(&self) -> PyResult<u64> {
        self.enqueue(|job| WsCommand::Ping { job })
    }

    fn close(&self, code: u16, reason: String) -> PyResult<u64> {
        self.enqueue(|job| WsCommand::Close { job, code, reason })
    }
}

/// What `ws_connect` got from Python, owned for the task.
pub(crate) struct Open {
    pub request: WsRequest,
    pub group: String,
    pub name: Option<String>,
    pub tags: Vec<(String, String)>,
    pub timeout: Option<Duration>,
}

/// Opens the WebSocket on tokio; the result arrives as a `ws_open` command.
/// Returns the handle that aborts the handshake (not the session: once
/// open, the session belongs to its Python object).
pub(crate) fn connect(
    runtime: &tokio::runtime::Handle,
    ctx: ScriptContext,
    open: Open,
    job: u64,
    queue: Arc<Queue>,
    jobs: Arc<AtomicU64>,
    interrupt: CancellationToken,
) -> tokio::task::AbortHandle {
    let spawner = runtime.clone();
    runtime
        .spawn(async move {
            let tags: Vec<(&str, &str)> = open
                .tags
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let meta = RequestMeta {
                group: &open.group,
                name: open.name.as_deref(),
                tags: &tags,
                timeout: open.timeout,
            };
            let session = match ctx.websocket(&open.request, &meta).await {
                Ok(session) => session,
                Err(err) => {
                    queue.push(Command::WsOpened {
                        job,
                        result: Err(err),
                    });
                    return;
                }
            };
            let (inbox, commands) = mpsc::unbounded_channel();
            let id = jobs.fetch_add(1, Ordering::Relaxed);
            let socket = session.socket();
            let native = NativeWebSocket {
                session: id,
                status: socket.status.as_u16(),
                headers: socket
                    .headers
                    .iter()
                    .map(|(k, v)| {
                        let v = String::from_utf8_lossy(v.as_bytes()).into_owned();
                        (k.as_str().to_owned(), v)
                    })
                    .collect(),
                inbox,
                jobs,
            };
            // Pushed before the session's first note, so Python knows it first.
            queue.push(Command::WsOpened {
                job,
                result: Ok(Box::new(native)),
            });
            spawner.spawn(serve(session, id, commands, queue, interrupt));
        })
        .abort_handle()
}

async fn serve(
    mut session: WsSession,
    id: u64,
    mut commands: mpsc::UnboundedReceiver<WsCommand>,
    queue: Arc<Queue>,
    interrupt: CancellationToken,
) {
    let note = |note| {
        queue.push(Command::Ws {
            session: id,
            note: Box::new(note),
        })
    };
    let done = |job, result: Result<(), k9_http::HttpError>| {
        queue.push(Command::WsDone {
            job,
            error: result.err().map(|e| e.to_string()),
        })
    };
    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(WsCommand::Send { job, message }) => done(job, session.send(message).await),
                Some(WsCommand::Ping { job }) => done(job, session.ping().await),
                Some(WsCommand::Close { job, code, reason }) => {
                    done(job, session.close(code, &reason).await);
                }
                // The Python object is gone: say goodbye.
                None => {
                    let _ = session.close(1000, "").await;
                    return;
                }
            },
            event = session.recv() => match event {
                Some(Ok(WsEvent::Message(message))) => note(WsNote::Message(message)),
                Some(Ok(WsEvent::Pong(rtt))) => note(WsNote::Pong(rtt)),
                Some(Ok(WsEvent::Close(frame))) => note(WsNote::Close(frame)),
                Some(Err(err)) => {
                    note(WsNote::End(Some(err.to_string())));
                    break;
                }
                None => {
                    note(WsNote::End(None));
                    break;
                }
            },
            _ = interrupt.cancelled() => {
                note(WsNote::End(Some("the VU was interrupted".to_owned())));
                break;
            }
        }
    }
    drop(session);
    // Whatever Python still asked for is answered: the session is over.
    commands.close();
    while let Ok(command) = commands.try_recv() {
        let (WsCommand::Send { job, .. } | WsCommand::Ping { job } | WsCommand::Close { job, .. }) =
            command;
        queue.push(Command::WsDone {
            job,
            error: Some("the WebSocket is closed".to_owned()),
        });
    }
}

/// A note as the tuple the `ws` command carries: `(kind, a, b)`.
pub(crate) fn note_tuple<'py>(py: Python<'py>, note: WsNote) -> PyResult<Bound<'py, PyAny>> {
    let none = py.None().into_bound(py);
    let tuple = match note {
        WsNote::Message(WsMessage::Text(text)) => {
            ("message", text.into_pyobject(py)?.into_any(), none)
        }
        WsNote::Message(WsMessage::Binary(data)) => {
            ("message", PyBytes::new(py, &data).into_any(), none)
        }
        WsNote::Pong(rtt) => (
            "pong",
            (rtt.as_secs_f64() * 1e3).into_pyobject(py)?.into_any(),
            none,
        ),
        WsNote::Close(Some((code, reason))) => (
            "close",
            code.into_pyobject(py)?.into_any(),
            reason.into_pyobject(py)?.into_any(),
        ),
        WsNote::Close(None) => ("close", none.clone(), none),
        WsNote::End(error) => ("end", error.into_pyobject(py)?.into_any(), none),
    };
    tuple.into_pyobject(py).map(Bound::into_any)
}

/// A WebSocket for plain `def` code (`k9.sync.ws`): every call blocks with the
/// GIL released, and returns at once if the VU is interrupted.
#[pyclass(frozen, name = "BlockingWebSocket", module = "k9._native")]
pub(crate) struct BlockingWebSocket {
    #[pyo3(get)]
    status: u16,
    #[pyo3(get)]
    headers: Vec<(String, String)>,
    state: Arc<tokio::sync::Mutex<Blocking>>,
    runtime: tokio::runtime::Handle,
    interrupt: CancellationToken,
}

struct Blocking {
    session: Option<WsSession>,
    /// Messages that arrived while `ping` waited for its pong.
    early: std::collections::VecDeque<WsMessage>,
}

/// What `BlockingWebSocket.recv` found: `(kind, a, b)` as in [`note_tuple`],
/// or `("timeout", None, None)`.
enum Received {
    Note(WsNote),
    Timeout,
}

impl BlockingWebSocket {
    pub(crate) fn new(
        session: WsSession,
        runtime: tokio::runtime::Handle,
        interrupt: CancellationToken,
    ) -> BlockingWebSocket {
        let socket = session.socket();
        BlockingWebSocket {
            status: socket.status.as_u16(),
            headers: socket
                .headers
                .iter()
                .map(|(k, v)| {
                    let v = String::from_utf8_lossy(v.as_bytes()).into_owned();
                    (k.as_str().to_owned(), v)
                })
                .collect(),
            state: Arc::new(tokio::sync::Mutex::new(Blocking {
                session: Some(session),
                early: std::collections::VecDeque::new(),
            })),
            runtime,
            interrupt,
        }
    }

    /// Runs `work` on tokio with the GIL released; `k9.Interrupted` if the
    /// VU is interrupted first.
    fn wait<T: Send>(&self, py: Python<'_>, work: impl Future<Output = T> + Send) -> PyResult<T> {
        let interrupt = &self.interrupt;
        py.detach(|| {
            self.runtime.block_on(async {
                tokio::select! {
                    done = work => Some(done),
                    _ = interrupt.cancelled() => None,
                }
            })
        })
        .ok_or_else(|| crate::script::interrupted(py))
    }
}

fn to_note(event: Option<Result<WsEvent, k9_http::HttpError>>) -> WsNote {
    match event {
        Some(Ok(WsEvent::Message(message))) => WsNote::Message(message),
        Some(Ok(WsEvent::Pong(rtt))) => WsNote::Pong(rtt),
        Some(Ok(WsEvent::Close(frame))) => WsNote::Close(frame),
        Some(Err(err)) => WsNote::End(Some(err.to_string())),
        None => WsNote::End(None),
    }
}

#[pymethods]
impl BlockingWebSocket {
    /// Sends `str` or `bytes`; returns an error message, or `None`.
    fn send(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
        let message = if let Ok(text) = data.cast::<PyString>() {
            WsMessage::Text(text.to_str()?.to_owned())
        } else if let Ok(bytes) = data.cast::<PyBytes>() {
            WsMessage::Binary(bytes::Bytes::copy_from_slice(bytes.as_bytes()))
        } else {
            return Err(PyTypeError::new_err(format!(
                "a WebSocket sends str or bytes, not {}",
                data.get_type().name()?
            )));
        };
        let state = self.state.clone();
        self.wait(py, async move {
            match &mut state.lock().await.session {
                Some(session) => session.send(message).await.err().map(|e| e.to_string()),
                None => Some("the WebSocket is closed".to_owned()),
            }
        })
    }

    /// The next message, close or end within `timeout` seconds, as
    /// `(kind, a, b)`; pongs are skipped.
    #[pyo3(signature = (timeout=None))]
    fn recv<'py>(&self, py: Python<'py>, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let limit = timeout.map(|t| Duration::from_secs_f64(t.max(0.0)));
        let received = self.wait(py, async move {
            let mut state = state.lock().await;
            if let Some(message) = state.early.pop_front() {
                return Received::Note(WsNote::Message(message));
            }
            let Some(session) = &mut state.session else {
                return Received::Note(WsNote::End(None));
            };
            let next = async {
                loop {
                    match to_note(session.recv().await) {
                        WsNote::Pong(_) => {}
                        note => return note,
                    }
                }
            };
            let note = match limit {
                Some(limit) => match tokio::time::timeout(limit, next).await {
                    Ok(note) => note,
                    Err(_) => return Received::Timeout,
                },
                None => next.await,
            };
            if matches!(note, WsNote::End(_)) {
                state.session = None;
            }
            Received::Note(note)
        })?;
        match received {
            Received::Note(note) => note_tuple(py, note),
            Received::Timeout => ("timeout", py.None(), py.None())
                .into_pyobject(py)
                .map(Bound::into_any),
        }
    }

    /// Pings and waits for the pong: the round trip in milliseconds, or
    /// `None` if the WebSocket ended first (or `timeout` passed).
    #[pyo3(signature = (timeout=None))]
    fn ping(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<f64>> {
        let state = self.state.clone();
        let limit = Duration::from_secs_f64(timeout.unwrap_or(60.0).max(0.0));
        self.wait(py, async move {
            let mut state = state.lock().await;
            let Blocking { session, early } = &mut *state;
            let session = session.as_mut()?;
            session.ping().await.ok()?;
            let pong = async {
                loop {
                    match session.recv().await {
                        Some(Ok(WsEvent::Pong(rtt))) => return Some(rtt.as_secs_f64() * 1e3),
                        Some(Ok(WsEvent::Message(message))) => early.push_back(message),
                        _ => return None,
                    }
                }
            };
            tokio::time::timeout(limit, pong).await.ok().flatten()
        })
    }

    /// Closes and waits (up to 5 s) for the peer's answer; returns the peer's
    /// close frame `(code, reason)` if it sent one.
    fn close(&self, py: Python<'_>, code: u16, reason: String) -> PyResult<Option<(u16, String)>> {
        let state = self.state.clone();
        self.wait(py, async move {
            let mut session = state.lock().await.session.take()?;
            session.close(code, &reason).await.ok()?;
            let mut frame = None;
            let drain = async {
                while let Some(event) = session.recv().await {
                    if let Ok(WsEvent::Close(f)) = event {
                        frame = f;
                    }
                }
            };
            let _ = tokio::time::timeout(Duration::from_secs(5), drain).await;
            frame
        })
    }
}
