//! [`ScriptContext`]: what script code can do while a VU or hook runs. Requests
//! go out on the VU's own client (its connection pool and cookie jar) and are
//! recorded exactly like a declarative flow's; checks, groups and custom
//! metrics carry the VU's tags.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use k9_http::grpc::{GrpcMethod, GrpcRequest, GrpcResponse};
use k9_http::{HttpError, Request, Response, WsRequest};
use k9_metrics::TagSetId;

use crate::engine::grpc::{self, CallTags, GrpcStreamSession};
use crate::engine::vu::{Shared, is_expected, tls_version};
use crate::engine::websocket::WsSession;

/// Per-request options from script code.
#[derive(Debug, Clone, Copy, Default)]
pub struct RequestMeta<'a> {
    /// The `group` tag: the group path the request was made in, e.g. `::login`.
    pub group: &'a str,
    /// The `name` tag; defaults to the URL that answered.
    pub name: Option<&'a str>,
    /// Extra tags for this request's samples.
    pub tags: &'a [(&'a str, &'a str)],
    /// Overrides the client's timeout for this request.
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown metric `{0}`")]
pub struct UnknownMetric(pub String);

/// Handed to a script for each VU and hook. Cheap to clone.
#[derive(Clone)]
pub struct ScriptContext(Arc<Inner>);

struct Inner {
    shared: Arc<Shared>,
    /// Plan tags, plus the scenario's tags and name for a VU.
    tags: Vec<(String, String)>,
    /// `tags` + the root group, for `data_sent`/`data_received`.
    data_tags: TagSetId,
    client: k9_http::Client,
    /// HTTP/2 only, made from `client` on the first gRPC call.
    grpc: OnceLock<k9_http::Client>,
    root_group: &'static str,
}

impl ScriptContext {
    pub(crate) fn new(
        shared: Arc<Shared>,
        tags: Vec<(String, String)>,
        client: k9_http::Client,
        root_group: &'static str,
    ) -> ScriptContext {
        let mut data: Vec<(&str, &str)> = pairs(&tags).collect();
        data.push(("group", root_group));
        let data_tags = shared.intern(&data);
        ScriptContext(Arc::new(Inner {
            shared,
            tags,
            data_tags,
            client,
            grpc: OnceLock::new(),
            root_group,
        }))
    }

    /// k6's `throw`: requests that get no response raise.
    pub fn throw(&self) -> bool {
        self.0.shared.flows.throw
    }

    /// The group path everything in this context starts from: `""` in a VU,
    /// `::setup` / `::teardown` in those hooks (as in k6).
    pub fn root_group(&self) -> &str {
        self.0.root_group
    }

    /// Sends `request` and records it (each followed redirect as its own
    /// sample, like k6). A request that gets no response is recorded with
    /// status 0 and counted as a request error.
    pub async fn request(
        &self,
        request: &Request,
        meta: &RequestMeta<'_>,
    ) -> Result<Response, HttpError> {
        let inner = &*self.0;
        let result = match meta.timeout {
            Some(timeout) => inner.client.send_with_timeout(request, timeout).await,
            None => inner.client.send(request).await,
        };
        let shared = &inner.shared;
        let method = request.method.as_str();
        match &result {
            Ok(res) => {
                for hop in &res.redirects {
                    let url = hop.url.as_str();
                    let status = hop.status.as_u16();
                    let proto = k9_http::proto(hop.version);
                    let tags = self.request_tags(
                        meta,
                        hop.method.as_str(),
                        url,
                        url,
                        status,
                        proto,
                        tls_version(hop.tls),
                    );
                    shared.record_exchange(tags, status, &hop.timings);
                    shared.record_data(inner.data_tags, hop.bytes_sent, hop.bytes_received);
                }
                let url = res.url.as_str();
                let status = res.status.as_u16();
                let name = meta.name.unwrap_or(url);
                let proto = k9_http::proto(res.version);
                let tags =
                    self.request_tags(meta, method, url, name, status, proto, tls_version(res.tls));
                shared.record_exchange(tags, status, &res.timings);
                shared.record_data(inner.data_tags, res.bytes_sent, res.bytes_received);
            }
            Err(err) => {
                let url = request.url.as_str();
                let tags =
                    self.request_tags(meta, method, url, meta.name.unwrap_or(url), 0, "", "");
                shared.record_failure(tags, err.to_string());
            }
        }
        result
    }

    /// Records one check result in `checks` (k6's `check()`).
    pub fn check(&self, name: &str, passed: bool, group: &str, tags: &[(&str, &str)]) {
        let shared = &self.0.shared;
        let tags = self.tags_with(tags, &[("group", group), ("check", name)]);
        shared
            .metrics
            .record(shared.b.checks, tags, f64::from(passed));
    }

    /// Records how long a `group` block took.
    pub fn group_duration(&self, group: &str, duration: Duration) {
        let shared = &self.0.shared;
        let tags = self.tags_with(&[], &[("group", group)]);
        shared
            .metrics
            .record_duration(shared.b.group_duration, tags, duration);
    }

    /// Adds a sample to a metric the script declared (or a built-in).
    pub fn add(
        &self,
        metric: &str,
        value: f64,
        group: &str,
        tags: &[(&str, &str)],
    ) -> Result<(), UnknownMetric> {
        let metrics = &self.0.shared.metrics;
        let id = metrics
            .registry()
            .get(metric)
            .ok_or_else(|| UnknownMetric(metric.to_owned()))?;
        let tags = self.tags_with(tags, &[("group", group)]);
        metrics.record(id, tags, value);
        Ok(())
    }

    /// Opens a WebSocket on a connection of its own, recording `ws_sessions`
    /// and `ws_connecting` (then the session's own metrics as it's used). It's
    /// tagged `url`, `name` (`meta.name` or the URL), `status` and `group`,
    /// plus `meta.tags`. A failed handshake counts as a request error.
    pub async fn websocket(
        &self,
        request: &WsRequest,
        meta: &RequestMeta<'_>,
    ) -> Result<WsSession, HttpError> {
        let inner = &self.0;
        let started = std::time::Instant::now();
        let opened = match meta.timeout {
            Some(timeout) => tokio::time::timeout(timeout, inner.client.websocket(request))
                .await
                .unwrap_or(Err(HttpError::Timeout(timeout))),
            None => inner.client.websocket(request).await,
        };
        let ws = match opened {
            Ok(ws) => ws,
            Err(err) => {
                inner.shared.record_request_error(err.to_string());
                return Err(err);
            }
        };
        let url = request.url.as_str();
        let status = ws.status.as_u16().to_string();
        let system = [
            ("url", url),
            ("name", meta.name.unwrap_or(url)),
            ("group", meta.group),
            ("status", status.as_str()),
        ];
        let tags = self.tags_with(meta.tags, &system);
        Ok(WsSession::new(
            ws,
            inner.shared.clone(),
            tags,
            inner.data_tags,
            started,
        ))
    }

    /// Makes a unary gRPC call on the VU's gRPC client (HTTP/2, connections of
    /// its own), recording `grpc_req_duration` tagged `url`, `method` (the
    /// method's path), `name` (`meta.name` or the path), `rpc_type`, `status`
    /// (the gRPC code) and `group`, plus `meta.tags`. A call that gets no
    /// answer ends with DEADLINE_EXCEEDED or UNAVAILABLE (timed until it
    /// gave up) and counts as a request error.
    pub async fn grpc_invoke(&self, request: &GrpcRequest, meta: &RequestMeta<'_>) -> GrpcResponse {
        let inner = &*self.0;
        let client = inner.grpc.get_or_init(|| inner.client.grpc());
        let mut tags: Vec<(&str, &str)> = pairs(&inner.tags).collect();
        tags.extend_from_slice(meta.tags);
        let call = CallTags {
            tags,
            name: meta.name,
            group: meta.group,
            data_tags: inner.data_tags,
        };
        grpc::invoke(&inner.shared, client, request, meta.timeout, call)
            .await
            .0
    }

    /// Opens a stream (client, server or bidirectional) to `method` on
    /// `target` over the VU's gRPC client, recording k6's `grpc_streams*`
    /// metrics as it's used (see [`GrpcStreamSession`]), tagged like
    /// [`ScriptContext::grpc_invoke`]'s calls minus `status`.
    pub async fn grpc_stream(
        &self,
        target: &url::Url,
        method: &GrpcMethod,
        metadata: http::HeaderMap,
        options: &k9_http::grpc::GrpcOptions,
        meta: &RequestMeta<'_>,
    ) -> GrpcStreamSession {
        let inner = &*self.0;
        let client = inner.grpc.get_or_init(|| inner.client.grpc());
        let mut tags: Vec<(&str, &str)> = pairs(&inner.tags).collect();
        tags.extend_from_slice(meta.tags);
        let call = CallTags {
            tags,
            name: meta.name,
            group: meta.group,
            data_tags: inner.data_tags,
        };
        grpc::open_stream(
            &inner.shared,
            client,
            target,
            method,
            metadata,
            meta.timeout,
            options,
            call,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    fn request_tags(
        &self,
        meta: &RequestMeta<'_>,
        method: &str,
        url: &str,
        name: &str,
        status: u16,
        proto: &str,
        tls: &str,
    ) -> TagSetId {
        let status_str = status.to_string();
        let mut system = vec![
            ("method", method),
            ("url", url),
            ("name", name),
            ("group", meta.group),
            ("status", status_str.as_str()),
            (
                "expected_response",
                if is_expected(status) { "true" } else { "false" },
            ),
        ];
        // No `proto` without a response, and no `tls_version` without TLS.
        if !proto.is_empty() {
            system.push(("proto", proto));
        }
        if !tls.is_empty() {
            system.push(("tls_version", tls));
        }
        self.tags_with(meta.tags, &system)
    }

    /// The context's tags, then `user` tags, then `system` tags (later ones win).
    fn tags_with(&self, user: &[(&str, &str)], system: &[(&str, &str)]) -> TagSetId {
        let mut tags: Vec<(&str, &str)> = pairs(&self.0.tags).collect();
        tags.extend_from_slice(user);
        tags.extend_from_slice(system);
        self.0.shared.intern(&tags)
    }
}

fn pairs(tags: &[(String, String)]) -> impl Iterator<Item = (&str, &str)> {
    tags.iter().map(|(k, v)| (k.as_str(), v.as_str()))
}
