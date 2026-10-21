//! gRPC for flows and scripts: loading each step's protos once before the
//! run (files or server reflection), and making a call while recording it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use k9_http::Client;
use k9_http::grpc::{
    GrpcError, GrpcEvent, GrpcMethod, GrpcReceiver, GrpcRequest, GrpcResponse, GrpcSender,
    GrpcStream, Protos, target,
};
use k9_metrics::TagSetId;

use crate::engine::EngineError;
use crate::engine::vu::Shared;
use crate::plan::{GrpcSpec, Plan, Step};

/// Every gRPC step's protos, by [`proto_key`].
pub(crate) type LoadedProtos = HashMap<String, Protos>;

/// What a step's protos come from: steps with the same files, or reflecting
/// the same target, share them.
pub(crate) fn proto_key(spec: &GrpcSpec) -> String {
    if spec.protos.is_empty() {
        format!("reflect {} {}", spec.plaintext, spec.target)
    } else {
        format!("files {:?} {:?}", spec.protos, spec.import_paths)
    }
}

/// Loads the protos of every gRPC step in `plan`, asking servers over
/// `client` (see [`Client::grpc`]) for those without files.
pub(crate) async fn load_protos(plan: &Plan, client: &Client) -> Result<LoadedProtos, EngineError> {
    fn specs<'a>(steps: &'a [Step], path: &str, out: &mut Vec<(String, &'a GrpcSpec)>) {
        for (i, step) in steps.iter().enumerate() {
            match step {
                Step::Grpc(spec) => out.push((format!("{path}[{i}]"), spec)),
                Step::Group { steps, .. } => specs(steps, &format!("{path}[{i}].steps"), out),
                _ => {}
            }
        }
    }
    let mut found = Vec::new();
    for (name, steps) in &plan.flows {
        specs(steps, &format!("flows.{name}"), &mut found);
    }
    let mut loaded = LoadedProtos::new();
    let mut grpc_client = None;
    for (location, spec) in found {
        let key = proto_key(spec);
        if loaded.contains_key(&key) {
            continue;
        }
        let invalid = |message: String| EngineError::InvalidRequest {
            location: location.clone(),
            message,
        };
        let protos = if let Some(set) = &spec.descriptor_set {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD
                .decode(set)
                .map_err(|e| invalid(format!("descriptorSet: {e}")))
                .and_then(|bytes| Protos::decode(&bytes).map_err(|e| invalid(e.to_string())))?
        } else if spec.protos.is_empty() {
            let url = target(&spec.target, spec.plaintext).map_err(|e| invalid(e.to_string()))?;
            let client = grpc_client.get_or_insert_with(|| client.grpc());
            Protos::reflect(client, &url, spec.timeout.map(|t| t.as_std()))
                .await
                .map_err(|e| invalid(e.to_string()))?
        } else {
            Protos::load(&spec.protos, &spec.import_paths).map_err(|e| invalid(e.to_string()))?
        };
        loaded.insert(key, protos);
    }
    Ok(loaded)
}

/// How a call is tagged.
pub(crate) struct CallTags<'a> {
    /// The context's tags, then the call's own.
    pub tags: Vec<(&'a str, &'a str)>,
    /// Default: the method's path.
    pub name: Option<&'a str>,
    pub group: &'a str,
    /// For `data_sent`/`data_received`.
    pub data_tags: TagSetId,
}

/// k9's `rpc_type` tag for a method.
pub(crate) fn rpc_type(method: &GrpcMethod) -> &'static str {
    match (method.is_client_streaming(), method.is_server_streaming()) {
        (false, false) => "unary",
        (true, false) => "client_streaming",
        (false, true) => "server_streaming",
        (true, true) => "bidi_streaming",
    }
}

/// A stream opened by script code or a flow, recording k6's metrics as it's
/// used: `grpc_streams` when opened, `grpc_streams_msgs_sent` and
/// `grpc_streams_msgs_received` per message, and `data_sent`/`data_received`
/// at the end. A stream the server never answered counts as a request error.
#[derive(Debug)]
pub struct GrpcStreamSession {
    sender: StreamSender,
    receiver: StreamReceiver,
}

/// The sending half of a [`GrpcStreamSession`].
#[derive(Debug)]
pub struct StreamSender {
    inner: GrpcSender,
    recorder: Recorder,
}

/// The receiving half of a [`GrpcStreamSession`]; dropping it cancels the stream.
#[derive(Debug)]
pub struct StreamReceiver {
    inner: GrpcReceiver,
    recorder: Recorder,
    /// For `grpc_stream_duration`, recorded once: at the end, or when the
    /// stream is dropped (cancelled) before it.
    opened: Instant,
    ended: bool,
}

#[derive(Clone)]
struct Recorder {
    shared: Arc<Shared>,
    tags: TagSetId,
    data_tags: TagSetId,
}

impl std::fmt::Debug for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recorder").finish_non_exhaustive()
    }
}

impl GrpcStreamSession {
    pub fn split(self) -> (StreamSender, StreamReceiver) {
        (self.sender, self.receiver)
    }

    pub async fn send(&mut self, message: &serde_json::Value) -> Result<(), GrpcError> {
        self.sender.send(message).await
    }

    pub fn close_send(&mut self) {
        self.sender.close();
    }

    pub async fn recv(&mut self) -> Option<GrpcEvent> {
        self.receiver.recv().await
    }
}

impl StreamSender {
    pub async fn send(&mut self, message: &serde_json::Value) -> Result<(), GrpcError> {
        self.inner.send(message).await?;
        let r = &self.recorder;
        r.shared
            .metrics
            .record(r.shared.b.grpc_streams_msgs_sent, r.tags, 1.0);
        Ok(())
    }

    /// Half-closes the stream: no more messages.
    pub fn close(&mut self) {
        self.inner.close();
    }
}

impl StreamReceiver {
    /// The next message, or the stream's end; `None` after the end.
    pub async fn recv(&mut self) -> Option<GrpcEvent> {
        let event = self.inner.recv().await;
        let r = &self.recorder;
        match &event {
            Some(GrpcEvent::Message(_)) => {
                r.shared
                    .metrics
                    .record(r.shared.b.grpc_streams_msgs_received, r.tags, 1.0)
            }
            Some(GrpcEvent::End(end)) => {
                self.ended = true;
                r.shared.metrics.record_duration(
                    r.shared.b.grpc_stream_duration,
                    r.tags,
                    self.opened.elapsed(),
                );
                if !end.answered {
                    r.shared.record_request_error(end.status_message.clone());
                }
                if end.bytes_sent > 0 || end.bytes_received > 0 {
                    r.shared
                        .record_data(r.data_tags, end.bytes_sent, end.bytes_received);
                }
            }
            None => {}
        }
        event
    }
}

impl Drop for StreamReceiver {
    fn drop(&mut self) {
        if !self.ended {
            let r = &self.recorder;
            r.shared.metrics.record_duration(
                r.shared.b.grpc_stream_duration,
                r.tags,
                self.opened.elapsed(),
            );
        }
    }
}

/// Opens a stream on `client`, recording `grpc_streams` tagged `url`,
/// `method`, `name`, `rpc_type` and `group` (the tags its messages get too).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn open_stream(
    shared: &Arc<Shared>,
    client: &Client,
    target: &url::Url,
    method: &GrpcMethod,
    metadata: http::HeaderMap,
    timeout: Option<Duration>,
    options: &k9_http::grpc::GrpcOptions,
    call: CallTags<'_>,
) -> GrpcStreamSession {
    let opened = Instant::now();
    let stream: GrpcStream = client
        .grpc_stream_with(target, method, metadata, timeout, options)
        .await;
    let path = method.path();
    let url = target.join(path).expect("a path joins any base URL");
    let mut tags = call.tags;
    tags.extend([
        ("url", url.as_str()),
        ("method", path),
        ("name", call.name.unwrap_or(path)),
        ("rpc_type", rpc_type(method)),
        ("group", call.group),
    ]);
    let tags = shared.intern(&tags);
    shared.metrics.record(shared.b.grpc_streams, tags, 1.0);
    let recorder = Recorder {
        shared: shared.clone(),
        tags,
        data_tags: call.data_tags,
    };
    let (sender, receiver) = stream.split();
    GrpcStreamSession {
        sender: StreamSender {
            inner: sender,
            recorder: recorder.clone(),
        },
        receiver: StreamReceiver {
            inner: receiver,
            recorder,
            opened,
            ended: false,
        },
    }
}

/// Makes a unary call and records `grpc_req_duration`, tagged `url`,
/// `method`, `name`, `rpc_type`, `status` and `group`. A call that gets no
/// answer ends with DEADLINE_EXCEEDED or UNAVAILABLE, timed until it gave
/// up, and counts as a request error. Returns the response and, if one
/// came, its duration.
pub(crate) async fn invoke(
    shared: &Shared,
    client: &Client,
    request: &GrpcRequest,
    timeout: Option<Duration>,
    call: CallTags<'_>,
) -> (GrpcResponse, Option<Duration>) {
    let started = Instant::now();
    let (res, answered) = match client.grpc_invoke(request, timeout).await {
        Ok(res) => {
            let duration = res.timings.duration();
            (res, Some(duration))
        }
        Err(err) => {
            shared.record_request_error(err.to_string());
            (GrpcResponse::from_error(&err), None)
        }
    };
    let path = request.method.path();
    let status = res.status.to_string();
    let mut tags = call.tags;
    tags.extend([
        ("url", request.url.as_str()),
        ("method", path),
        ("name", call.name.unwrap_or(path)),
        ("rpc_type", rpc_type(&request.method)),
        ("group", call.group),
        ("status", status.as_str()),
    ]);
    let tags = shared.intern(&tags);
    shared.metrics.record_duration(
        shared.b.grpc_req_duration,
        tags,
        answered.unwrap_or_else(|| started.elapsed()),
    );
    if res.bytes_sent > 0 || res.bytes_received > 0 {
        shared.record_data(call.data_tags, res.bytes_sent, res.bytes_received);
    }
    (res, answered)
}
