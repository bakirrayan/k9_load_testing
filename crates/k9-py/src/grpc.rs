//! `k9.grpc`'s native half: compiled protos, method handles, targets, and
//! the tuple a call's `k9.grpc.Response` is built from.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use k9_core::engine::{GrpcStreamSession, StreamReceiver, StreamSender};
use k9_http::grpc::{
    GrpcError, GrpcEvent, GrpcMethod, GrpcOptions, GrpcRequest, GrpcResponse, Protos, target,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::script::{Command, Queue, interrupted};

/// Services and messages compiled from `.proto` files.
#[pyclass(frozen, name = "Protos", module = "k9._native")]
pub(crate) struct NativeProtos(Protos);

#[pymethods]
impl NativeProtos {
    /// Compiles `files` (and their imports) from `import_paths` (default:
    /// each file's directory); `ValueError` if they don't compile.
    #[staticmethod]
    fn load(py: Python<'_>, files: Vec<String>, import_paths: Vec<String>) -> PyResult<Self> {
        py.detach(|| Protos::load(&files, &import_paths))
            .map(NativeProtos)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Asks the server at `url` (a target's base URL) for its services,
    /// blocking with the GIL released; `ValueError` if it can't.
    #[staticmethod]
    #[pyo3(signature = (url, timeout))]
    fn reflect(py: Python<'_>, url: &str, timeout: Option<f64>) -> PyResult<Self> {
        let error = |e: &dyn std::fmt::Display| PyValueError::new_err(e.to_string());
        let url = url::Url::parse(url).map_err(|e| error(&e))?;
        let timeout = timeout.map(Duration::from_secs_f64);
        // A thread of its own: this may be called where a runtime is running.
        let found = py.detach(|| {
            std::thread::scope(|s| {
                s.spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| e.to_string())?;
                    runtime.block_on(async {
                        let client = k9_http::Client::new(Default::default())
                            .map_err(|e| e.to_string())?
                            .grpc();
                        Protos::reflect(&client, &url, timeout)
                            .await
                            .map_err(|e| e.to_string())
                    })
                })
                .join()
                .unwrap_or_else(|_| Err("reflection panicked".to_owned()))
            })
        });
        found.map(NativeProtos).map_err(|e| error(&e))
    }

    /// Every method, as `package.Service/Method`.
    fn methods(&self) -> Vec<String> {
        self.0.methods()
    }

    /// A method by name; `ValueError` if there's none.
    fn method(&self, name: &str) -> PyResult<NativeMethod> {
        self.0
            .method(name)
            .map(NativeMethod)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
}

#[pyclass(frozen, name = "GrpcMethod", module = "k9._native")]
pub(crate) struct NativeMethod(pub(crate) GrpcMethod);

#[pymethods]
impl NativeMethod {
    /// `/package.Service/Method`.
    #[getter]
    fn path(&self) -> &str {
        self.0.path()
    }
}

/// A target's base URL (`host:port` or an http(s) URL); `ValueError` if invalid.
#[pyfunction]
pub(crate) fn grpc_target(address: &str, plaintext: bool) -> PyResult<String> {
    target(address, plaintext)
        .map(String::from)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Stream metadata and timeout from their Python pieces; invalid ones raise `ValueError`.
pub(crate) fn stream_parts(
    metadata: &[(String, String)],
    timeout: Option<f64>,
) -> PyResult<(http::HeaderMap, Option<Duration>)> {
    let invalid = |e: &dyn std::fmt::Display| PyValueError::new_err(e.to_string());
    let mut map = http::HeaderMap::new();
    for (key, value) in metadata {
        let key = http::HeaderName::from_bytes(key.as_bytes())
            .map_err(|e| invalid(&format!("metadata {key:?}: {e}")))?;
        let value = http::HeaderValue::from_str(value)
            .map_err(|e| invalid(&format!("metadata {key}: {e}")))?;
        map.append(key, value);
    }
    let timeout = match timeout {
        Some(secs) if secs.is_finite() && secs > 0.0 => Some(Duration::from_secs_f64(secs)),
        Some(secs) => return Err(invalid(&format!("timeout must be positive, got {secs}"))),
        None => None,
    };
    Ok((map, timeout))
}

/// A client's options from Python: (gzip, max send size, max receive size).
pub(crate) type Options = (bool, Option<usize>, Option<usize>);

pub(crate) fn options((gzip, max_send_size, max_receive_size): Options) -> GrpcOptions {
    GrpcOptions {
        gzip,
        max_send_size,
        max_receive_size,
    }
}

/// A call from its Python pieces; invalid ones raise `ValueError`.
pub(crate) fn build(
    url: &str,
    method: &NativeMethod,
    message: &str,
    metadata: &[(String, String)],
    timeout: Option<f64>,
    call_options: Options,
) -> PyResult<(GrpcRequest, Option<Duration>)> {
    let invalid = |e: &dyn std::fmt::Display| PyValueError::new_err(e.to_string());
    let base = url::Url::parse(url).map_err(|e| invalid(&e))?;
    let message: serde_json::Value = serde_json::from_str(message).map_err(|e| invalid(&e))?;
    let mut request = GrpcRequest::new(&base, &method.0, &message).map_err(|e| invalid(&e))?;
    request.options = options(call_options);
    for (key, value) in metadata {
        let key = http::HeaderName::from_bytes(key.as_bytes())
            .map_err(|e| invalid(&format!("metadata {key:?}: {e}")))?;
        let value = http::HeaderValue::from_str(value)
            .map_err(|e| invalid(&format!("metadata {key}: {e}")))?;
        request.metadata.append(key, value);
    }
    let timeout = match timeout {
        Some(secs) if secs.is_finite() && secs > 0.0 => Some(Duration::from_secs_f64(secs)),
        Some(secs) => return Err(invalid(&format!("timeout must be positive, got {secs}"))),
        None => None,
    };
    Ok((request, timeout))
}

/// `(status, status_message, message_json, headers, trailers, duration_ms)`.
pub(crate) fn response_tuple<'py>(
    py: Python<'py>,
    res: GrpcResponse,
) -> PyResult<Bound<'py, PyAny>> {
    let pairs = |map: &http::HeaderMap| -> Vec<(String, String)> {
        map.iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_owned(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect()
    };
    let message = res.message.as_ref().map(serde_json::Value::to_string);
    (
        res.status,
        res.status_message.as_str(),
        message,
        pairs(&res.headers),
        pairs(&res.trailers),
        res.timings.duration().as_secs_f64() * 1e3,
    )
        .into_pyobject(py)
        .map(Bound::into_any)
}

type Shared<T> = Arc<tokio::sync::Mutex<Option<T>>>;

/// An open stream, for `k9.grpc.Stream` (jobs answered through the queue)
/// and `k9.sync.grpc.Stream` (blocking calls, GIL released).
#[pyclass(frozen, name = "GrpcStream", module = "k9._native")]
pub(crate) struct NativeGrpcStream {
    sender: Shared<StreamSender>,
    receiver: Shared<StreamReceiver>,
    /// Cancelled by `cancel()` or when Python drops the stream.
    cancel: CancellationToken,
    /// The VU's: blocking calls return when it's interrupted.
    interrupt: CancellationToken,
    runtime: tokio::runtime::Handle,
    queue: Arc<Queue>,
    jobs: Arc<AtomicU64>,
}

impl NativeGrpcStream {
    pub(crate) fn new(
        session: GrpcStreamSession,
        runtime: tokio::runtime::Handle,
        queue: Arc<Queue>,
        jobs: Arc<AtomicU64>,
        interrupt: CancellationToken,
    ) -> NativeGrpcStream {
        let (sender, receiver) = session.split();
        NativeGrpcStream {
            sender: Arc::new(tokio::sync::Mutex::new(Some(sender))),
            receiver: Arc::new(tokio::sync::Mutex::new(Some(receiver))),
            cancel: CancellationToken::new(),
            interrupt,
            runtime,
            queue,
            jobs,
        }
    }

    fn sending(&self, message: &str) -> PyResult<impl Future<Output = SendError> + use<>> {
        let message: serde_json::Value =
            serde_json::from_str(message).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let sender = self.sender.clone();
        Ok(async move {
            match sender.lock().await.as_mut() {
                Some(sender) => match sender.send(&message).await {
                    Ok(()) => None,
                    Err(err @ GrpcError::Closed) => Some(("closed", err.to_string())),
                    Err(err) => Some(("invalid", err.to_string())),
                },
                None => Some(("closed", GrpcError::Closed.to_string())),
            }
        })
    }

    fn receiving(&self) -> impl Future<Output = Option<GrpcEvent>> + use<> {
        let (receiver, cancel) = (self.receiver.clone(), self.cancel.clone());
        async move {
            tokio::select! {
                event = async {
                    match receiver.lock().await.as_mut() {
                        Some(receiver) => receiver.recv().await,
                        None => None,
                    }
                } => event,
                _ = cancel.cancelled() => None,
            }
        }
    }

    /// Runs `work` on tokio, then delivers its result as job `job`.
    fn job<T: Send + 'static>(
        &self,
        work: impl Future<Output = T> + Send + 'static,
        reply: impl for<'py> FnOnce(Python<'py>, T) -> PyResult<Bound<'py, PyAny>> + Send + 'static,
    ) -> u64 {
        let job = self.jobs.fetch_add(1, Ordering::Relaxed);
        let queue = self.queue.clone();
        self.runtime.spawn(async move {
            let done = work.await;
            queue.push(Command::Reply {
                job,
                value: Box::new(move |py| reply(py, done)),
            });
        });
        job
    }

    /// Blocks on `work` with the GIL released; `k9.Interrupted` if the VU is
    /// interrupted first.
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
        .ok_or_else(|| interrupted(py))
    }
}

/// Why a send failed: `("closed" | "invalid", message)`.
type SendError = Option<(&'static str, String)>;

#[pymethods]
impl NativeGrpcStream {
    /// Sends `message` (JSON); the job's result is `None` or `(kind, message)`.
    fn send(&self, message: &str) -> PyResult<u64> {
        let sending = self.sending(message)?;
        Ok(self.job(sending, |py, error| {
            error.into_pyobject(py).map(Bound::into_any)
        }))
    }

    /// The next event; the job's result is `("message", json)`, `("end",
    /// status, status_message, headers, trailers)` or `None` after the end.
    fn recv(&self) -> u64 {
        self.job(self.receiving(), event_tuple)
    }

    fn send_blocking(&self, py: Python<'_>, message: &str) -> PyResult<SendError> {
        let sending = self.sending(message)?;
        self.wait(py, sending)
    }

    /// The next event, waiting at most `timeout` seconds: `("timeout",)` then
    /// (receiving is cancel-safe: a message arriving later isn't lost).
    #[pyo3(signature = (timeout=None))]
    fn recv_blocking<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let Some(secs) = timeout else {
            let event = self.wait(py, self.receiving())?;
            return event_tuple(py, event);
        };
        let limit = Duration::from_secs_f64(secs.max(0.0));
        // The timer is made on the runtime (inside the future), not here.
        let receiving = self.receiving();
        let timed = async move { tokio::time::timeout(limit, receiving).await };
        match self.wait(py, timed)? {
            Ok(event) => event_tuple(py, event),
            Err(_) => ("timeout",).into_pyobject(py).map(Bound::into_any),
        }
    }

    /// Half-closes the stream: no more messages.
    fn close_send(&self) {
        let sender = self.sender.clone();
        self.runtime.spawn(async move {
            if let Some(sender) = sender.lock().await.as_mut() {
                sender.close();
            }
        });
    }

    /// Ends the stream now (pending receives get `None`).
    fn cancel(&self) {
        self.cancel.cancel();
        let (sender, receiver) = (self.sender.clone(), self.receiver.clone());
        self.runtime.spawn(async move {
            sender.lock().await.take();
            receiver.lock().await.take();
        });
    }
}

impl Drop for NativeGrpcStream {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn event_tuple(py: Python<'_>, event: Option<GrpcEvent>) -> PyResult<Bound<'_, PyAny>> {
    let pairs = |map: &http::HeaderMap| -> Vec<(String, String)> {
        map.iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_owned(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect()
    };
    match event {
        Some(GrpcEvent::Message(message)) => ("message", message.to_string())
            .into_pyobject(py)
            .map(Bound::into_any),
        Some(GrpcEvent::End(end)) => (
            "end",
            end.status,
            end.status_message,
            pairs(&end.headers),
            pairs(&end.trailers),
        )
            .into_pyobject(py)
            .map(Bound::into_any),
        None => Ok(py.None().into_bound(py)),
    }
}
