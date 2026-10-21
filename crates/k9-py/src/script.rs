//! Python test scripts behind the engine's [`Script`] trait.
//!
//! Async code runs on one asyncio event loop thread (owned by
//! `k9._runtime.Runtime`). Tokio's workers never wait for the GIL: VUs push
//! commands onto a [`Queue`] and await a oneshot that the loop thread
//! completes, and HTTP requests from script code run on tokio and push their
//! response back the same way. A push wakes the loop through a socket it
//! watches (one byte, only when the queue goes from drained to pending); the
//! loop then takes every pending command at once, already holding the GIL.
//!
//! Plain `def` code runs on one Python thread per VU (see `k9._runtime`) and
//! uses the blocking calls here (`request_blocking`, `sleep`), which release
//! the GIL while they wait and return as soon as the VU is interrupted.

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use k9_cli::{Function, LoadedScript, ScriptHost, SummaryHook, SummaryOutputs};
use k9_core::engine::{
    BoxFuture, MetricDecl, RequestMeta, Script, ScriptContext, ScriptError, ScriptVu, VuInfo,
};
use k9_http::HttpError;
use k9_metrics::{MetricKind, Registry, ValueType};
use pyo3::exceptions::{PyException, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyType};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// What a job sends back: its result (only `handle_summary()` has one), or the
/// exception it raised.
type Outcome = Result<Option<SummaryOutputs>, ScriptError>;

pub(crate) struct PyHost;

impl ScriptHost for PyHost {
    fn load(&self, file: &Path, env: &[(String, String)]) -> Result<LoadedScript, String> {
        Python::attach(|py| load(py, file, env)).map_err(|err| {
            Python::attach(|py| {
                let runtime = py.import("k9._runtime").ok();
                let is_load_error = runtime
                    .and_then(|m| m.getattr("LoadError").ok())
                    .is_some_and(|cls| err.get_type(py).is(&cls));
                if is_load_error {
                    err.value(py).to_string()
                } else {
                    // A bug in k9 itself: show everything.
                    let traceback = err
                        .traceback(py)
                        .and_then(|tb| tb.format().ok())
                        .unwrap_or_default();
                    format!("{traceback}{err}")
                }
            })
        })
    }

    fn local_modules(&self, file: &Path) -> Result<Vec<String>, String> {
        Python::attach(|py| {
            py.import("k9._bundle")?
                .call_method1("local_modules", (file.display().to_string(),))?
                .extract::<Vec<String>>()
        })
        .map_err(|e| format!("finding the modules {} imports: {e}", file.display()))
    }

    /// Workers are this Python running `-m k9`, so they import the same k9.
    fn worker_command(&self) -> Vec<String> {
        Python::attach(|py| {
            let executable = py
                .import("sys")
                .and_then(|sys| sys.getattr("executable"))
                .and_then(|e| e.extract::<String>());
            match executable {
                Ok(python) if !python.is_empty() => vec![python, "-m".into(), "k9".into()],
                _ => vec!["python".into(), "-m".into(), "k9".into()],
            }
        })
    }
}

fn load(py: Python<'_>, file: &Path, env: &[(String, String)]) -> PyResult<LoadedScript> {
    let loaded = py
        .import("k9._runtime")?
        .call_method1("load", (file, env.to_vec()))?;
    let options: Option<String> = loaded.getattr("options_json")?.extract()?;
    let options = match options {
        Some(json) => serde_json::from_str(&json)
            .map_err(|e| PyException::new_err(format!("options JSON: {e}")))?,
        None => serde_json::Value::Null,
    };
    let plan: Option<String> = loaded.getattr("plan_json")?.extract()?;
    let plan = plan
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(|e| PyException::new_err(format!("plan JSON: {e}")))?;
    let functions: Vec<(String, bool)> = loaded.getattr("functions")?.extract()?;
    let hooks: Vec<String> = loaded.getattr("hooks")?.extract()?;
    let metrics: Vec<(String, String, bool)> = loaded.getattr("metrics")?.extract()?;
    let metrics = metrics
        .into_iter()
        .map(|(name, kind, is_time)| {
            let value_type = if is_time {
                ValueType::Time
            } else {
                ValueType::Default
            };
            Ok(MetricDecl::new(name, metric_kind(&kind)?, value_type))
        })
        .collect::<PyResult<_>>()?;
    let runtime = loaded.getattr("runtime")?.unbind();
    let has = |hook: &str| hooks.iter().any(|h| h == hook);
    let bridge = Arc::new(Bridge::new(runtime));
    Ok(LoadedScript {
        plan,
        options,
        functions: functions
            .into_iter()
            .map(|(name, is_async)| Function { name, is_async })
            .collect(),
        script: Arc::new(PyScript {
            has_setup: has("setup"),
            has_teardown: has("teardown"),
            has_vu_init: has("vu_init"),
            metrics,
            bridge: bridge.clone(),
        }),
        summary_hook: has("handle_summary")
            .then(|| Arc::new(PySummary { bridge }) as Arc<dyn SummaryHook>),
        hooks,
    })
}

fn metric_kind(kind: &str) -> PyResult<MetricKind> {
    Ok(match kind {
        "counter" => MetricKind::Counter,
        "gauge" => MetricKind::Gauge,
        "rate" => MetricKind::Rate,
        "trend" => MetricKind::Trend,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown metric kind {other:?}"
            )));
        }
    })
}

/// Checks a custom metric declaration against k6's naming rule and the
/// built-in metrics; returns the error message, if any.
#[pyfunction]
pub(crate) fn metric_error(name: &str, kind: &str) -> PyResult<Option<String>> {
    let (mut registry, _) = Registry::with_builtins();
    Ok(registry
        .register(name, metric_kind(kind)?, ValueType::Default)
        .err()
        .map(|e| e.to_string()))
}

/// Builds a job's result for Python.
pub(crate) type ReplyValue =
    Box<dyn for<'py> FnOnce(Python<'py>) -> PyResult<Bound<'py, PyAny>> + Send>;

pub(crate) enum Command {
    Vu {
        id: u64,
        scenario: String,
        exec: String,
        env: Vec<(String, String)>,
        ctx: NativeContext,
    },
    DropVu(u64),
    /// Runs `vu_init()` for a VU.
    Init {
        job: u64,
        vu: u64,
        reply: oneshot::Sender<Outcome>,
    },
    /// Runs `function` for a VU: an iteration if `counted`, else its
    /// `onStart`/`onStop`.
    Iterate {
        job: u64,
        vu: u64,
        function: String,
        counted: bool,
        reply: oneshot::Sender<Outcome>,
    },
    Hook {
        job: u64,
        name: &'static str,
        ctx: Option<NativeContext>,
        args: Vec<String>,
        reply: oneshot::Sender<Outcome>,
    },
    /// A request from script code finished.
    HttpDone {
        job: u64,
        /// Boxed: a response is far bigger than the other commands.
        result: Box<Result<k9_http::Response, HttpError>>,
    },
    /// A gRPC call from script code finished.
    GrpcDone {
        job: u64,
        result: Box<k9_http::grpc::GrpcResponse>,
    },
    /// A job finished; its result is built on the loop thread (with the
    /// GIL) and delivered like a request's.
    Reply {
        job: u64,
        value: ReplyValue,
    },
    /// A WebSocket from `ws_connect` opened (or didn't).
    WsOpened {
        job: u64,
        result: Result<Box<crate::ws::NativeWebSocket>, HttpError>,
    },
    /// A WebSocket's `send`/`ping`/`close` finished: `error` if it failed.
    WsDone {
        job: u64,
        error: Option<String>,
    },
    /// Something happened on an open WebSocket.
    Ws {
        session: u64,
        note: Box<crate::ws::WsNote>,
    },
    Cancel(u64),
}

/// Commands waiting for the Python runtime, and the socket that wakes its
/// event loop when there are some.
pub(crate) struct Queue {
    commands: Mutex<Vec<Command>>,
    /// Set from the first push after a drain until the next drain: only that
    /// push writes a wake-up byte.
    armed: AtomicBool,
    wake: wake::Writer,
}

impl Queue {
    pub(crate) fn push(&self, command: Command) {
        self.commands
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(command);
        if !self.armed.swap(true, Ordering::AcqRel) {
            // A full socket buffer already holds a wake-up.
            let _ = (&self.wake).write(&[1]);
        }
    }

    /// Everything pushed so far. Disarms first, so a push that misses this
    /// drain wakes the loop again.
    fn take(&self) -> Vec<Command> {
        self.armed.store(false, Ordering::SeqCst);
        std::mem::take(&mut *self.commands.lock().unwrap_or_else(|p| p.into_inner()))
    }
}

/// The loop's side of the [`Queue`]: `fileno()` to watch, `take()` to drain.
#[pyclass(frozen, module = "k9._native")]
struct Commands {
    queue: Arc<Queue>,
    fd: i64,
}

#[pymethods]
impl Commands {
    /// The socket to watch for reading (bytes in it only mean "take").
    fn fileno(&self) -> i64 {
        self.fd
    }

    /// Every pending command, as tuples for `k9._runtime.Runtime`.
    fn take<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        to_python(py, self.queue.take())
    }
}

/// Hands commands to the Python runtime's event loop.
struct Bridge {
    runtime: Arc<Py<PyAny>>,
    queue: Mutex<Option<Arc<Queue>>>,
    jobs: Arc<AtomicU64>,
}

impl Bridge {
    fn new(runtime: Py<PyAny>) -> Bridge {
        Bridge {
            runtime: Arc::new(runtime),
            queue: Mutex::new(None),
            jobs: Arc::new(AtomicU64::new(1)),
        }
    }

    fn job(&self) -> u64 {
        self.jobs.fetch_add(1, Ordering::Relaxed)
    }

    /// The queue to the event loop, connected on first use (so loading a
    /// script for `k9 inspect` starts no loop).
    fn queue(&self) -> Arc<Queue> {
        let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        queue
            .get_or_insert_with(|| {
                let (writer, reader) = wake::pair().expect("create the Python wake-up socket");
                let queue = Arc::new(Queue {
                    commands: Mutex::new(Vec::new()),
                    armed: AtomicBool::new(false),
                    wake: writer,
                });
                let commands = Commands {
                    queue: queue.clone(),
                    fd: reader,
                };
                Python::attach(|py| {
                    let connected = Py::new(py, commands)
                        .and_then(|c| self.runtime.call_method1(py, "connect", (c,)));
                    if let Err(err) = connected {
                        // Replies never come; the waiting side reports it.
                        eprintln!("ERROR could not connect to the Python runtime: {err}");
                    }
                });
                queue
            })
            .clone()
    }

    fn send(&self, command: Command) {
        self.queue().push(command);
    }

    /// A context for script code, which sends requests on `ctx`'s client.
    /// Cancelling `interrupt` stops the context's blocking calls.
    fn context(&self, ctx: ScriptContext, interrupt: CancellationToken) -> NativeContext {
        NativeContext {
            ctx,
            runtime: tokio::runtime::Handle::current(),
            queue: self.queue(),
            jobs: self.jobs.clone(),
            interrupt,
        }
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        Python::attach(|py| {
            if let Err(err) = self.runtime.call_method0(py, "close") {
                eprintln!("WARN could not stop the Python event loop: {err}");
            }
        });
    }
}

/// A connected socket pair: the queue writes to one end, the event loop
/// watches the other (a Unix socket pair, or loopback TCP on Windows, where
/// asyncio's selector loop only watches sockets).
mod wake {
    use std::io;

    #[cfg(unix)]
    pub type Writer = std::os::unix::net::UnixStream;
    #[cfg(windows)]
    pub type Writer = std::net::TcpStream;

    /// (writer, reader's fd or socket handle, now owned by Python).
    #[cfg(unix)]
    pub fn pair() -> io::Result<(Writer, i64)> {
        use std::os::fd::IntoRawFd;
        let (writer, reader) = std::os::unix::net::UnixStream::pair()?;
        writer.set_nonblocking(true)?;
        reader.set_nonblocking(true)?;
        Ok((writer, i64::from(reader.into_raw_fd())))
    }

    #[cfg(windows)]
    pub fn pair() -> io::Result<(Writer, i64)> {
        use std::os::windows::io::IntoRawSocket;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let writer = std::net::TcpStream::connect(listener.local_addr()?)?;
        let (reader, _) = listener.accept()?;
        writer.set_nodelay(true)?;
        writer.set_nonblocking(true)?;
        reader.set_nonblocking(true)?;
        Ok((writer, reader.into_raw_socket() as i64))
    }
}

fn to_python(py: Python<'_>, batch: Vec<Command>) -> PyResult<Bound<'_, PyList>> {
    let list = PyList::empty(py);
    for command in batch {
        match command {
            Command::Vu {
                id,
                scenario,
                exec,
                env,
                ctx,
            } => {
                let env_dict = PyDict::new(py);
                for (key, value) in env {
                    env_dict.set_item(key, value)?;
                }
                list.append(("vu", id, scenario, exec, env_dict, Py::new(py, ctx)?))?;
            }
            Command::DropVu(id) => list.append(("drop_vu", id))?,
            Command::Init { job, vu, reply } => {
                list.append(("init", job, vu, Reply::new(py, reply)?))?
            }
            Command::Iterate {
                job,
                vu,
                function,
                counted,
                reply,
            } => list.append((
                "iterate",
                job,
                vu,
                Reply::new(py, reply)?,
                function,
                counted,
            ))?,
            Command::Hook {
                job,
                name,
                ctx,
                args,
                reply,
            } => {
                let ctx = ctx.map(|ctx| Py::new(py, ctx)).transpose()?;
                list.append(("hook", job, name, ctx, args, Reply::new(py, reply)?))?
            }
            Command::HttpDone { job, result } => {
                list.append(("http_done", job, response_tuple(py, *result)?))?
            }
            Command::GrpcDone { job, result } => {
                list.append(("http_done", job, crate::grpc::response_tuple(py, *result)?))?
            }
            Command::Reply { job, value } => list.append(("http_done", job, value(py)?))?,
            Command::WsOpened { job, result } => match result {
                Ok(native) => list.append(("ws_open", job, Py::new(py, *native)?))?,
                Err(err) => list.append(("ws_open", job, (err.to_string(), error_code(&err))))?,
            },
            Command::WsDone { job, error } => list.append(("ws_done", job, error))?,
            Command::Ws { session, note } => {
                list.append(("ws", session, crate::ws::note_tuple(py, *note)?))?
            }
            Command::Cancel(job) => list.append(("cancel", job))?,
        }
    }
    Ok(list)
}

/// A finished request as the tuple `k9.http.Response` is built from:
/// `(status, headers, body, url, timings_ms, error, error_code, proto,
/// tls_version, tls_cipher_suite)`.
fn response_tuple<'py>(
    py: Python<'py>,
    result: Result<k9_http::Response, HttpError>,
) -> PyResult<Bound<'py, PyAny>> {
    let ms = |d: Duration| d.as_secs_f64() * 1e3;
    match result {
        Ok(res) => {
            let headers: Vec<(&str, String)> = res
                .headers
                .iter()
                .map(|(k, v)| {
                    (
                        k.as_str(),
                        String::from_utf8_lossy(v.as_bytes()).into_owned(),
                    )
                })
                .collect();
            let t = res.timings;
            let timings = (
                ms(t.blocked),
                ms(t.connecting),
                ms(t.tls_handshaking),
                ms(t.sending),
                ms(t.waiting),
                ms(t.receiving),
            );
            let none: Option<&str> = None;
            (
                res.status.as_u16(),
                headers,
                PyBytes::new(py, &res.body),
                res.url.as_str(),
                timings,
                none,
                none,
                k9_http::proto(res.version),
                res.tls.map(|t| t.version),
                res.tls.map(|t| t.cipher_suite),
            )
                .into_pyobject(py)
                .map(Bound::into_any)
        }
        Err(err) => {
            let code = error_code(&err);
            let headers: Vec<(String, String)> = Vec::new();
            (
                0u16,
                headers,
                PyBytes::new(py, b""),
                "",
                (0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                err.to_string(),
                code,
                "",
                None::<&str>,
                None::<&str>,
            )
                .into_pyobject(py)
                .map(Bound::into_any)
        }
    }
}

/// `Response.error_code` / `WebSocketError.kind` for an error.
fn error_code(err: &HttpError) -> &'static str {
    match err {
        HttpError::InvalidUrl { .. } => "invalid_url",
        HttpError::Dns { .. } => "dns",
        HttpError::Connect { .. } => "connect",
        HttpError::Tls { .. } => "tls",
        HttpError::Timeout(_) => "timeout",
        HttpError::Protocol(_) | HttpError::NoHttp2 { .. } | HttpError::Http3(_) => "protocol",
        HttpError::WebSocket(_) => "websocket",
        HttpError::Config(_) => "config",
        HttpError::Decompress(_) => "decompress",
        HttpError::Blocked { .. } => "blocked",
    }
}

/// Called by the runtime (on the loop thread) when a job ends:
/// `reply(None)` on success (`reply(None, None, result)` with a result), or
/// `reply(message, details)` with the exception.
#[pyclass(frozen)]
struct Reply(Mutex<Option<oneshot::Sender<Outcome>>>);

impl Reply {
    fn new(py: Python<'_>, tx: oneshot::Sender<Outcome>) -> PyResult<Py<Reply>> {
        Py::new(py, Reply(Mutex::new(Some(tx))))
    }
}

#[pymethods]
impl Reply {
    #[pyo3(signature = (error, details=None, result=None))]
    fn __call__(
        &self,
        error: Option<String>,
        details: Option<String>,
        result: Option<Vec<(String, Vec<u8>)>>,
    ) {
        let tx = self.0.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(tx) = tx {
            let outcome = match (error, details) {
                (None, _) => Ok(result),
                (Some(message), None) => Err(ScriptError::new(message)),
                (Some(message), Some(details)) => Err(ScriptError::with_details(message, details)),
            };
            // The receiver is gone if the iteration was interrupted.
            let _ = tx.send(outcome);
        }
    }
}

/// What script code in one VU (or hook) calls into: `k9.http`, checks,
/// groups and custom metrics. Lives in Python (`k9.vu`'s state).
#[pyclass(frozen, name = "Context", module = "k9._native")]
pub(crate) struct NativeContext {
    ctx: ScriptContext,
    runtime: tokio::runtime::Handle,
    queue: Arc<Queue>,
    jobs: Arc<AtomicU64>,
    /// Cancelled when the VU's iteration (or the hook) is interrupted; from
    /// then on every call raises `k9.Interrupted`.
    interrupt: CancellationToken,
}

type Tags = Vec<(String, String)>;

fn borrowed(tags: &Tags) -> Vec<(&str, &str)> {
    tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
}

/// `k9.Interrupted`: the VU was interrupted (gracefulStop passed, Ctrl-C).
pub(crate) fn interrupted(py: Python<'_>) -> PyErr {
    let class = py
        .import("k9._errors")
        .and_then(|m| m.getattr("Interrupted"))
        .and_then(|c| c.cast_into::<PyType>().map_err(PyErr::from));
    match class {
        Ok(class) => PyErr::from_type(class, "the VU was interrupted"),
        Err(err) => err,
    }
}

impl NativeContext {
    fn check_interrupt(&self, py: Python<'_>) -> PyResult<()> {
        if self.interrupt.is_cancelled() {
            return Err(interrupted(py));
        }
        Ok(())
    }

    /// Builds a request from its Python pieces; invalid ones raise `ValueError`.
    fn build(
        method: &str,
        url: &str,
        headers: &Tags,
        body: Option<Vec<u8>>,
        timeout: Option<f64>,
    ) -> PyResult<(k9_http::Request, Option<Duration>)> {
        let invalid = |e: &dyn std::fmt::Display| PyValueError::new_err(e.to_string());
        let method = http::Method::from_bytes(method.to_ascii_uppercase().as_bytes())
            .map_err(|e| invalid(&e))?;
        let mut request = k9_http::Request::new(method, url).map_err(|e| invalid(&e))?;
        for (key, value) in headers {
            let key = http::HeaderName::from_bytes(key.as_bytes())
                .map_err(|e| invalid(&format!("header {key:?}: {e}")))?;
            let value = http::HeaderValue::from_str(value)
                .map_err(|e| invalid(&format!("header {key}: {e}")))?;
            request.headers.append(key, value);
        }
        if let Some(body) = body {
            request.body = bytes::Bytes::from(body);
        }
        let timeout = match timeout {
            Some(secs) if secs.is_finite() && secs > 0.0 => Some(Duration::from_secs_f64(secs)),
            Some(secs) => return Err(invalid(&format!("timeout must be positive, got {secs}"))),
            None => None,
        };
        Ok((request, timeout))
    }

    /// A WebSocket handshake from its Python pieces; invalid ones raise `ValueError`.
    fn ws_request(
        url: &str,
        headers: &Tags,
        timeout: Option<f64>,
    ) -> PyResult<(k9_http::WsRequest, Option<Duration>)> {
        let invalid = |e: &dyn std::fmt::Display| PyValueError::new_err(e.to_string());
        let mut request = k9_http::WsRequest::new(url).map_err(|e| invalid(&e))?;
        for (key, value) in headers {
            let key = http::HeaderName::from_bytes(key.as_bytes())
                .map_err(|e| invalid(&format!("header {key:?}: {e}")))?;
            let value = http::HeaderValue::from_str(value)
                .map_err(|e| invalid(&format!("header {key}: {e}")))?;
            request.headers.append(key, value);
        }
        let timeout = match timeout {
            Some(secs) if secs.is_finite() && secs > 0.0 => Some(Duration::from_secs_f64(secs)),
            Some(secs) => return Err(invalid(&format!("timeout must be positive, got {secs}"))),
            None => None,
        };
        Ok((request, timeout))
    }

    /// Runs `request` on tokio, recording it on this context.
    fn spawn_request(
        &self,
        request: k9_http::Request,
        timeout: Option<Duration>,
        group: String,
        name: Option<String>,
        tags: Tags,
    ) -> tokio::task::JoinHandle<Result<k9_http::Response, HttpError>> {
        let ctx = self.ctx.clone();
        self.runtime.spawn(async move {
            let tags = borrowed(&tags);
            let meta = RequestMeta {
                group: &group,
                name: name.as_deref(),
                tags: &tags,
                timeout,
            };
            ctx.request(&request, &meta).await
        })
    }

    /// Opening a stream, from its Python pieces (invalid ones raise `ValueError`).
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    fn open_stream(
        &self,
        url: &str,
        method: &crate::grpc::NativeMethod,
        metadata: &Tags,
        group: String,
        name: Option<String>,
        tags: Tags,
        timeout: Option<f64>,
        options: crate::grpc::Options,
    ) -> PyResult<impl Future<Output = k9_core::engine::GrpcStreamSession> + Send + use<>> {
        let target = url::Url::parse(url).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let (metadata, timeout) = crate::grpc::stream_parts(metadata, timeout)?;
        let (ctx, method) = (self.ctx.clone(), method.0.clone());
        Ok(async move {
            let tags = borrowed(&tags);
            let meta = RequestMeta {
                group: &group,
                name: name.as_deref(),
                tags: &tags,
                timeout,
            };
            let options = crate::grpc::options(options);
            ctx.grpc_stream(&target, &method, metadata, &options, &meta)
                .await
        })
    }

    /// Waits for `work` with the GIL released; `None` if the context was
    /// interrupted first.
    fn wait<T: Send>(&self, py: Python<'_>, work: impl Future<Output = T> + Send) -> Option<T> {
        let interrupt = &self.interrupt;
        py.detach(|| {
            self.runtime.block_on(async {
                tokio::select! {
                    done = work => Some(done),
                    _ = interrupt.cancelled() => None,
                }
            })
        })
    }
}

#[pymethods]
impl NativeContext {
    /// The group everything in this context starts from (`::setup` in setup).
    #[getter]
    fn root_group(&self) -> &str {
        self.ctx.root_group()
    }

    /// k6's `throw`: requests that get no response raise `RequestError`.
    #[getter]
    fn throw(&self) -> bool {
        self.ctx.throw()
    }

    /// Starts a request on tokio; its response arrives as an `http_done`
    /// command. Invalid requests raise `ValueError` here.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (method, url, headers, body, group, name, tags, timeout))]
    fn request(
        &self,
        py: Python<'_>,
        method: &str,
        url: &str,
        headers: Tags,
        body: Option<Vec<u8>>,
        group: String,
        name: Option<String>,
        tags: Tags,
        timeout: Option<f64>,
    ) -> PyResult<RequestHandle> {
        self.check_interrupt(py)?;
        let (request, timeout) = Self::build(method, url, &headers, body, timeout)?;
        let job = self.jobs.fetch_add(1, Ordering::Relaxed);
        let (ctx, queue) = (self.ctx.clone(), self.queue.clone());
        let task = self.runtime.spawn(async move {
            let tags = borrowed(&tags);
            let meta = RequestMeta {
                group: &group,
                name: name.as_deref(),
                tags: &tags,
                timeout,
            };
            let result = ctx.request(&request, &meta).await;
            queue.push(Command::HttpDone {
                job,
                result: Box::new(result),
            });
        });
        Ok(RequestHandle {
            job,
            task: task.abort_handle(),
        })
    }

    /// Starts opening a WebSocket on tokio; the result arrives as a `ws_open`
    /// command. Invalid URLs or headers raise `ValueError` here.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (url, headers, group, name, tags, timeout))]
    fn ws_connect(
        &self,
        py: Python<'_>,
        url: &str,
        headers: Tags,
        group: String,
        name: Option<String>,
        tags: Tags,
        timeout: Option<f64>,
    ) -> PyResult<RequestHandle> {
        self.check_interrupt(py)?;
        let (request, timeout) = Self::ws_request(url, &headers, timeout)?;
        let job = self.jobs.fetch_add(1, Ordering::Relaxed);
        let open = crate::ws::Open {
            request,
            group,
            name,
            tags,
            timeout,
        };
        let task = crate::ws::connect(
            &self.runtime,
            self.ctx.clone(),
            open,
            job,
            self.queue.clone(),
            self.jobs.clone(),
            self.interrupt.clone(),
        );
        Ok(RequestHandle { job, task })
    }

    /// Opens a WebSocket for plain `def` code, with the GIL released: a
    /// `BlockingWebSocket`, or `(message, kind)` if it didn't open.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (url, headers, group, name, tags, timeout))]
    fn ws_connect_blocking<'py>(
        &self,
        py: Python<'py>,
        url: &str,
        headers: Tags,
        group: String,
        name: Option<String>,
        tags: Tags,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.check_interrupt(py)?;
        let (ws_request, timeout) = Self::ws_request(url, &headers, timeout)?;
        let ctx = self.ctx.clone();
        let opened = self.wait(py, async move {
            let tags = borrowed(&tags);
            let meta = RequestMeta {
                group: &group,
                name: name.as_deref(),
                tags: &tags,
                timeout,
            };
            ctx.websocket(&ws_request, &meta).await
        });
        match opened {
            Some(Ok(session)) => Py::new(
                py,
                crate::ws::BlockingWebSocket::new(
                    session,
                    self.runtime.clone(),
                    self.interrupt.clone(),
                ),
            )
            .map(|ws| ws.into_bound(py).into_any()),
            Some(Err(err)) => (err.to_string(), error_code(&err))
                .into_pyobject(py)
                .map(Bound::into_any),
            None => Err(interrupted(py)),
        }
    }

    /// Starts a unary gRPC call on tokio; its response tuple arrives as an
    /// `http_done` command. Invalid calls raise `ValueError` here.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (url, method, message, metadata, group, name, tags, timeout, options))]
    fn grpc_invoke(
        &self,
        py: Python<'_>,
        url: &str,
        method: &crate::grpc::NativeMethod,
        message: &str,
        metadata: Tags,
        group: String,
        name: Option<String>,
        tags: Tags,
        timeout: Option<f64>,
        options: crate::grpc::Options,
    ) -> PyResult<RequestHandle> {
        self.check_interrupt(py)?;
        let (request, timeout) =
            crate::grpc::build(url, method, message, &metadata, timeout, options)?;
        let job = self.jobs.fetch_add(1, Ordering::Relaxed);
        let (ctx, queue) = (self.ctx.clone(), self.queue.clone());
        let task = self.runtime.spawn(async move {
            let tags = borrowed(&tags);
            let meta = RequestMeta {
                group: &group,
                name: name.as_deref(),
                tags: &tags,
                timeout,
            };
            let result = ctx.grpc_invoke(&request, &meta).await;
            queue.push(Command::GrpcDone {
                job,
                result: Box::new(result),
            });
        });
        Ok(RequestHandle {
            job,
            task: task.abort_handle(),
        })
    }

    /// Makes a unary gRPC call and waits for its response tuple, with the
    /// GIL released. Raises `k9.Interrupted` if the VU is interrupted.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (url, method, message, metadata, group, name, tags, timeout, options))]
    fn grpc_invoke_blocking<'py>(
        &self,
        py: Python<'py>,
        url: &str,
        method: &crate::grpc::NativeMethod,
        message: &str,
        metadata: Tags,
        group: String,
        name: Option<String>,
        tags: Tags,
        timeout: Option<f64>,
        options: crate::grpc::Options,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.check_interrupt(py)?;
        let (request, timeout) =
            crate::grpc::build(url, method, message, &metadata, timeout, options)?;
        let ctx = self.ctx.clone();
        let done = self.wait(py, async move {
            let tags = borrowed(&tags);
            let meta = RequestMeta {
                group: &group,
                name: name.as_deref(),
                tags: &tags,
                timeout,
            };
            ctx.grpc_invoke(&request, &meta).await
        });
        match done {
            Some(res) => crate::grpc::response_tuple(py, res),
            None => Err(interrupted(py)),
        }
    }

    /// Starts opening a gRPC stream on tokio; the native `GrpcStream`
    /// arrives as an `http_done` command. Invalid metadata raises `ValueError`.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (url, method, metadata, group, name, tags, timeout, options))]
    fn grpc_stream(
        &self,
        py: Python<'_>,
        url: &str,
        method: &crate::grpc::NativeMethod,
        metadata: Tags,
        group: String,
        name: Option<String>,
        tags: Tags,
        timeout: Option<f64>,
        options: crate::grpc::Options,
    ) -> PyResult<RequestHandle> {
        self.check_interrupt(py)?;
        let opening =
            self.open_stream(url, method, &metadata, group, name, tags, timeout, options)?;
        let job = self.jobs.fetch_add(1, Ordering::Relaxed);
        let (queue, runtime, jobs, interrupt) = (
            self.queue.clone(),
            self.runtime.clone(),
            self.jobs.clone(),
            self.interrupt.clone(),
        );
        let task = self.runtime.spawn(async move {
            let session = opening.await;
            let stream = crate::grpc::NativeGrpcStream::new(
                session,
                runtime,
                queue.clone(),
                jobs,
                interrupt,
            );
            queue.push(Command::Reply {
                job,
                value: Box::new(move |py| Py::new(py, stream).map(|s| s.into_bound(py).into_any())),
            });
        });
        Ok(RequestHandle {
            job,
            task: task.abort_handle(),
        })
    }

    /// Opens a gRPC stream for plain `def` code, with the GIL released.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (url, method, metadata, group, name, tags, timeout, options))]
    fn grpc_stream_blocking(
        &self,
        py: Python<'_>,
        url: &str,
        method: &crate::grpc::NativeMethod,
        metadata: Tags,
        group: String,
        name: Option<String>,
        tags: Tags,
        timeout: Option<f64>,
        options: crate::grpc::Options,
    ) -> PyResult<crate::grpc::NativeGrpcStream> {
        self.check_interrupt(py)?;
        let opening =
            self.open_stream(url, method, &metadata, group, name, tags, timeout, options)?;
        match self.wait(py, opening) {
            Some(session) => Ok(crate::grpc::NativeGrpcStream::new(
                session,
                self.runtime.clone(),
                self.queue.clone(),
                self.jobs.clone(),
                self.interrupt.clone(),
            )),
            None => Err(interrupted(py)),
        }
    }

    /// Sends a request and waits for its response (as the tuple
    /// `k9.http.Response` is built from), with the GIL released. Raises
    /// `k9.Interrupted` (and aborts the request) if the VU is interrupted.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (method, url, headers, body, group, name, tags, timeout))]
    fn request_blocking<'py>(
        &self,
        py: Python<'py>,
        method: &str,
        url: &str,
        headers: Tags,
        body: Option<Vec<u8>>,
        group: String,
        name: Option<String>,
        tags: Tags,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.check_interrupt(py)?;
        let (request, timeout) = Self::build(method, url, &headers, body, timeout)?;
        let mut task = self.spawn_request(request, timeout, group, name, tags);
        match self.wait(py, &mut task) {
            Some(Ok(result)) => response_tuple(py, result),
            Some(Err(join)) => Err(PyException::new_err(format!("request task failed: {join}"))),
            None => {
                task.abort();
                Err(interrupted(py))
            }
        }
    }

    /// Sleeps with the GIL released; raises `k9.Interrupted` if the VU is
    /// interrupted meanwhile.
    fn sleep(&self, py: Python<'_>, seconds: f64) -> PyResult<()> {
        self.check_interrupt(py)?;
        let duration = Duration::try_from_secs_f64(seconds.max(0.0))
            .map_err(|e| PyValueError::new_err(format!("sleep({seconds}): {e}")))?;
        // Created inside `wait` (a timer needs the runtime's context).
        match self.wait(py, async move { tokio::time::sleep(duration).await }) {
            Some(()) => Ok(()),
            None => Err(interrupted(py)),
        }
    }

    fn check(
        &self,
        py: Python<'_>,
        name: &str,
        passed: bool,
        group: &str,
        tags: Tags,
    ) -> PyResult<()> {
        self.check_interrupt(py)?;
        self.ctx.check(name, passed, group, &borrowed(&tags));
        Ok(())
    }

    fn group_duration(&self, py: Python<'_>, group: &str, seconds: f64) -> PyResult<()> {
        self.check_interrupt(py)?;
        self.ctx
            .group_duration(group, Duration::from_secs_f64(seconds.max(0.0)));
        Ok(())
    }

    fn add(
        &self,
        py: Python<'_>,
        metric: &str,
        value: f64,
        group: &str,
        tags: Tags,
    ) -> PyResult<()> {
        self.check_interrupt(py)?;
        self.ctx
            .add(metric, value, group, &borrowed(&tags))
            .map_err(|e| PyKeyError::new_err(e.to_string()))
    }
}

/// An in-flight request started by [`NativeContext::request`].
#[pyclass(frozen, module = "k9._native")]
pub(crate) struct RequestHandle {
    #[pyo3(get)]
    job: u64,
    task: tokio::task::AbortHandle,
}

#[pymethods]
impl RequestHandle {
    /// Stops the request (the awaiting Python task was cancelled).
    fn abort(&self) {
        self.task.abort();
    }
}

/// Runs a job in Python; dropping the future cancels it: the asyncio task is
/// cancelled, and `interrupt` (the job's context) stops its blocking calls.
fn run_job(
    bridge: Arc<Bridge>,
    interrupt: Option<CancellationToken>,
    command: impl FnOnce(u64, oneshot::Sender<Outcome>) -> Command,
) -> BoxFuture<'static, Outcome> {
    let job = bridge.job();
    let (tx, rx) = oneshot::channel();
    bridge.send(command(job, tx));
    let mut cancel = CancelOnDrop(Some((bridge, job, interrupt)));
    Box::pin(async move {
        let outcome = rx.await;
        cancel.disarm();
        outcome.unwrap_or_else(|_| Err(ScriptError::new("the Python runtime stopped")))
    })
}

fn no_result(outcome: BoxFuture<'static, Outcome>) -> BoxFuture<'static, Result<(), ScriptError>> {
    Box::pin(async move { outcome.await.map(|_| ()) })
}

struct CancelOnDrop(Option<(Arc<Bridge>, u64, Option<CancellationToken>)>);

impl CancelOnDrop {
    /// The job finished; nothing to cancel.
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some((bridge, job, interrupt)) = self.0.take() {
            if let Some(interrupt) = interrupt {
                interrupt.cancel();
            }
            bridge.send(Command::Cancel(job));
        }
    }
}

struct PyScript {
    bridge: Arc<Bridge>,
    has_setup: bool,
    has_teardown: bool,
    has_vu_init: bool,
    metrics: Vec<MetricDecl>,
}

impl PyScript {
    fn hook(
        &self,
        name: &'static str,
        ctx: ScriptContext,
    ) -> BoxFuture<'_, Result<(), ScriptError>> {
        let interrupt = CancellationToken::new();
        let ctx = self.bridge.context(ctx, interrupt.clone());
        no_result(run_job(
            self.bridge.clone(),
            Some(interrupt),
            |job, reply| Command::Hook {
                job,
                name,
                ctx: Some(ctx),
                args: Vec::new(),
                reply,
            },
        ))
    }
}

impl Script for PyScript {
    fn metrics(&self) -> Vec<MetricDecl> {
        self.metrics.clone()
    }

    fn setup_data(&self) -> Option<String> {
        Python::attach(|py| {
            self.bridge
                .runtime
                .getattr(py, "data_json")
                .and_then(|data| data.extract::<String>(py))
                .ok()
        })
    }

    fn set_setup_data(&self, json: String) {
        Python::attach(|py| {
            if let Err(err) = self.bridge.runtime.setattr(py, "data_json", json) {
                eprintln!("WARN could not pass setup() data to the script: {err}");
            }
        });
    }

    fn setup(&self, ctx: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        if !self.has_setup {
            return Box::pin(async { Ok(()) });
        }
        self.hook("setup", ctx)
    }

    fn teardown(&self, ctx: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        if !self.has_teardown {
            return Box::pin(async { Ok(()) });
        }
        self.hook("teardown", ctx)
    }

    fn vu(&self, vu: VuInfo, ctx: ScriptContext) -> Box<dyn ScriptVu> {
        // An interrupted iteration ends the VU, so one token serves them all.
        let interrupt = CancellationToken::new();
        self.bridge.send(Command::Vu {
            id: vu.id,
            scenario: vu.scenario,
            exec: vu.exec,
            env: vu.env.into_iter().collect(),
            ctx: self.bridge.context(ctx, interrupt.clone()),
        });
        Box::new(PyVu {
            id: vu.id,
            has_init: self.has_vu_init,
            bridge: self.bridge.clone(),
            interrupt,
        })
    }
}

struct PyVu {
    id: u64,
    has_init: bool,
    bridge: Arc<Bridge>,
    interrupt: CancellationToken,
}

impl ScriptVu for PyVu {
    fn init(&mut self) -> BoxFuture<'_, Result<(), ScriptError>> {
        if !self.has_init {
            return Box::pin(async { Ok(()) });
        }
        let vu = self.id;
        let interrupt = Some(self.interrupt.clone());
        no_result(run_job(
            self.bridge.clone(),
            interrupt,
            move |job, reply| Command::Init { job, vu, reply },
        ))
    }

    fn iteration(&mut self, function: &str) -> BoxFuture<'_, Result<(), ScriptError>> {
        self.run(function, true)
    }

    fn task(&mut self, function: &str) -> BoxFuture<'_, Result<(), ScriptError>> {
        self.run(function, false)
    }
}

impl PyVu {
    fn run(&mut self, function: &str, counted: bool) -> BoxFuture<'_, Result<(), ScriptError>> {
        let vu = self.id;
        let function = function.to_owned();
        let interrupt = Some(self.interrupt.clone());
        no_result(run_job(
            self.bridge.clone(),
            interrupt,
            move |job, reply| Command::Iterate {
                job,
                vu,
                function,
                counted,
                reply,
            },
        ))
    }
}

impl Drop for PyVu {
    fn drop(&mut self) {
        self.bridge.send(Command::DropVu(self.id));
    }
}

/// The script's `handle_summary(data)`, run on the loop like a hook.
struct PySummary {
    bridge: Arc<Bridge>,
}

impl SummaryHook for PySummary {
    fn handle_summary(
        &self,
        data: serde_json::Value,
        text: String,
    ) -> BoxFuture<'_, Result<SummaryOutputs, ScriptError>> {
        let outcome = run_job(self.bridge.clone(), None, |job, reply| Command::Hook {
            job,
            name: "handle_summary",
            ctx: None,
            args: vec![data.to_string(), text],
            reply,
        });
        Box::pin(async move { Ok(outcome.await?.unwrap_or_default()) })
    }
}
