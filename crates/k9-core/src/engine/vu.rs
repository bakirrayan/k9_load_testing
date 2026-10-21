//! A virtual user: runs its scenario's flow and records k6's metrics.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use k9_http::Client;
use k9_metrics::{Builtins, Metrics, TagSetId};
use tokio::sync::broadcast;

use crate::engine::RunEvent;
use crate::engine::arrival::Omitted;
use crate::engine::compile::{
    CCheck, CGrpc, CMessage, CRequest, CStep, CText, CWebSocket, CWsStep, Flows, build_form,
};
use crate::engine::grpc::{self, CallTags};
use crate::engine::script::{ScriptError, ScriptVu};
use crate::engine::template::RenderCx;
use crate::engine::template::Template;
use crate::engine::websocket::WsSession;
use crate::plan::Check;

/// State shared by every VU of a run.
pub(crate) struct Shared {
    pub metrics: Arc<Metrics>,
    pub b: Builtins,
    pub flows: Flows,
    /// Exceptions raised by script iterations, by message.
    pub script_errors: Mutex<HashMap<String, u64>>,
    pub active_vus: AtomicU32,
    /// Most VUs active at once so far.
    pub peak_vus: AtomicU32,
    pub complete: AtomicU64,
    pub interrupted: AtomicU64,
    /// Failed requests (no response), by message.
    pub errors: Mutex<HashMap<String, u64>>,
    /// The first occurrence of each error goes out here as it happens.
    pub events: broadcast::Sender<RunEvent>,
    /// Pausing and scaling.
    pub live: Arc<super::live::Live>,
}

impl Shared {
    /// Interns `tags`, leaving out the system tags the plan turned off.
    pub fn intern(&self, tags: &[(&str, &str)]) -> TagSetId {
        self.flows.system_tags.intern(self.metrics.interner(), tags)
    }

    /// Records one request/response exchange in k6's `http_req_*` metrics.
    pub fn record_exchange(&self, tags: TagSetId, status: u16, t: &k9_http::Timings) {
        let (metrics, b) = (&self.metrics, &self.b);
        metrics.record(b.http_reqs, tags, 1.0);
        metrics.record_duration(b.http_req_duration, tags, t.duration());
        metrics.record_duration(b.http_req_blocked, tags, t.blocked);
        metrics.record_duration(b.http_req_connecting, tags, t.connecting);
        metrics.record_duration(b.http_req_tls_handshaking, tags, t.tls_handshaking);
        metrics.record_duration(b.http_req_sending, tags, t.sending);
        metrics.record_duration(b.http_req_waiting, tags, t.waiting);
        metrics.record_duration(b.http_req_receiving, tags, t.receiving);
        metrics.record(b.http_req_failed, tags, f64::from(!is_expected(status)));
    }

    pub fn record_data(&self, tags: TagSetId, sent: u64, received: u64) {
        self.metrics.record(self.b.data_sent, tags, sent as f64);
        self.metrics
            .record(self.b.data_received, tags, received as f64);
    }

    /// Records a request that got no response (tagged `status:0`).
    pub fn record_failure(&self, tags: TagSetId, error: String) {
        self.metrics.record(self.b.http_reqs, tags, 1.0);
        self.metrics.record(self.b.http_req_failed, tags, 1.0);
        self.record_request_error(error);
    }

    /// Counts a request (or WebSocket) error, published live the first time.
    pub fn record_request_error(&self, error: String) {
        if count(&self.errors, error.clone()) {
            let _ = self.events.send(RunEvent::RequestError(error));
        }
    }

    /// Counts an exception raised by script code.
    pub fn record_script_error(&self, error: ScriptError) {
        if count(&self.script_errors, error.message.clone()) {
            let _ = self.events.send(RunEvent::ScriptError(error));
        }
    }
}

/// Counts one more `message`; true if it's the first.
fn count(errors: &Mutex<HashMap<String, u64>>, message: String) -> bool {
    let mut errors = errors.lock().unwrap_or_else(|p| p.into_inner());
    let n = errors.entry(message).or_default();
    *n += 1;
    *n == 1
}

/// One of a scenario's weighted transactions.
pub(crate) struct Transaction {
    /// The flow (or script function) it runs.
    pub name: String,
    pub weight: u32,
    /// `iteration_tags` + `transaction:<name>`.
    pub iteration_tags: TagSetId,
}

/// Per-scenario tagging context.
pub(crate) struct ScenarioCtx {
    pub name: String,
    pub env: indexmap::IndexMap<String, String>,
    /// The flow (or script function) each iteration runs.
    pub flow: String,
    /// Plan tags, scenario tags and `scenario:<name>`.
    pub tags: Vec<(String, String)>,
    /// `tags` + `group:""`, for iteration-level metrics.
    pub iteration_tags: TagSetId,
    /// When set, each iteration runs one of these instead of `flow`.
    pub transactions: Vec<Transaction>,
    pub on_start: Option<String>,
    pub on_stop: Option<String>,
}

pub(crate) struct Vu {
    id: u64,
    /// Iterations this VU has started (k6's `__ITER` of the current one).
    iter: u64,
    /// Values extractors have set, by [`Flows::vars`] index.
    vars: Vec<Option<String>>,
    /// The current iteration's row from each feeder, once used.
    rows: Vec<Option<usize>>,
    /// Transactions (indexes) left in the current shuffled round.
    deck: Vec<usize>,
    shared: Arc<Shared>,
    scenario: Arc<ScenarioCtx>,
    client: Client,
    /// HTTP/2 only, made from `client` at the first gRPC step.
    grpc_client: Option<Client>,
    /// Set when a script (not a flow) drives this VU.
    script: Option<Box<dyn ScriptVu>>,
    /// Interned request tags by (request, status), so steady-state requests
    /// don't touch the interner.
    request_tags: HashMap<(usize, u16, &'static str, &'static str), TagSetId>,
    group_tags: HashMap<usize, TagSetId>,
    /// Interned `checks` tags by (request, check).
    check_tags: HashMap<(usize, usize), TagSetId>,
    /// Interned `checks` tags by [`CCheck::id`] (WebSocket steps).
    ws_check_tags: HashMap<usize, TagSetId>,
}

/// Why a WebSocket `receive` got no message.
enum Missed {
    Timeout(std::time::Duration),
    Closed,
    /// The session failed (already recorded as an error).
    Failed,
}

impl Vu {
    pub fn new(
        id: u64,
        shared: Arc<Shared>,
        scenario: Arc<ScenarioCtx>,
        client: Client,
        script: Option<Box<dyn ScriptVu>>,
    ) -> Vu {
        Vu {
            id,
            iter: 0,
            vars: vec![None; shared.flows.vars.len()],
            rows: vec![None; shared.flows.feeders.len()],
            deck: Vec::new(),
            shared,
            scenario,
            client,
            grpc_client: None,
            script,
            request_tags: HashMap::new(),
            group_tags: HashMap::new(),
            check_tags: HashMap::new(),
            ws_check_tags: HashMap::new(),
        }
    }

    /// Runs the script's `vu_init()`, if the VU runs a script. False if it
    /// failed (the error is recorded), in which case the VU must stop.
    /// Then runs the scenario's `onStart`, whose failure is recorded but
    /// doesn't stop the VU.
    pub async fn init(&mut self) -> bool {
        if let Some(script) = &mut self.script
            && let Err(err) = script.init().await
        {
            self.shared.record_script_error(err);
            return false;
        }
        if let Some(on_start) = self.scenario.on_start.clone() {
            self.task(&on_start).await;
        }
        true
    }

    /// Runs the scenario's `onStop`, if it has one, as the VU stops gracefully.
    pub async fn stop(&mut self) {
        if let Some(on_stop) = self.scenario.on_stop.clone() {
            self.task(&on_stop).await;
        }
    }

    /// Runs an `onStart`/`onStop` flow or function: not an iteration.
    async fn task(&mut self, name: &str) {
        let shared = self.shared.clone();
        let result = match &mut self.script {
            Some(script) => script.task(name).await.map_err(Some),
            None => {
                self.rows.fill(None);
                self.run_steps(&shared.flows.flows[name])
                    .await
                    .map_err(|e| e.map(ScriptError::new))
            }
        };
        if let Err(Some(err)) = result {
            shared.record_script_error(err);
        }
    }

    /// The next transaction from a shuffled round holding each one as many
    /// times as its weight, so every VU runs the exact mix.
    fn next_transaction(&mut self) -> Option<usize> {
        let transactions = &self.scenario.transactions;
        if transactions.is_empty() {
            return None;
        }
        if self.deck.is_empty() {
            self.deck = deal(transactions.iter().map(|t| t.weight));
        }
        self.deck.pop()
    }

    /// Runs one iteration of the scenario's flow and records it. Dropping the
    /// future interrupts the iteration (nothing is recorded for it).
    /// `due`: for arrival-rate iterations, when it was due, and the
    /// scenario's dropped iterations (one is counted per iteration that ends,
    /// as if it had waited for this VU).
    pub async fn iteration(&mut self, due: Option<(std::time::Instant, &Omitted)>) {
        let started = Instant::now();
        // k6 starts every iteration with an empty cookie jar; with `onStart`
        // (e.g. a login), the VU keeps its session, as a Goose user does.
        if self.scenario.on_start.is_none() && !self.shared.flows.no_cookies_reset {
            self.client.clear_cookies();
        }
        if self.shared.flows.no_vu_connection_reuse {
            self.client.close_connections();
        }
        let shared = self.shared.clone();
        let scenario = self.scenario.clone();
        let (exec, tags) = match self.next_transaction() {
            Some(i) => {
                let t = &scenario.transactions[i];
                (t.name.as_str(), t.iteration_tags)
            }
            None => (scenario.flow.as_str(), scenario.iteration_tags),
        };
        if let Some(script) = &mut self.script {
            // Like k6, an iteration that raised still counts as complete.
            if let Err(err) = script.iteration(exec).await {
                shared.record_script_error(err);
            }
        } else {
            self.rows.fill(None);
            let flow = &shared.flows.flows[exec];
            // A step that can't go on (e.g. an extractor found nothing) ends
            // the iteration, which still counts as complete, like a script's.
            if let Err(Some(message)) = self.run_steps(flow).await {
                shared.record_script_error(ScriptError::new(message));
            }
        }
        if let Some(min) = shared.flows.min_iteration_duration {
            let left = min.saturating_sub(started.elapsed());
            if !left.is_zero() {
                tokio::time::sleep(left).await;
            }
        }
        self.iter += 1;
        let metrics = &shared.metrics;
        metrics.record(shared.b.iterations, tags, 1.0);
        let raw = started.elapsed();
        metrics.record_duration(shared.b.iteration_duration, tags, raw);
        if let Some((due, omitted)) = due {
            let end = Instant::now();
            let corrected = shared.b.iteration_duration_corrected;
            metrics.record_duration(corrected, tags, end.saturating_duration_since(due));
            if let Some(dropped) = omitted.pop() {
                metrics.record_duration(
                    corrected,
                    tags,
                    end.saturating_duration_since(dropped) + raw,
                );
            }
        }
    }

    /// `Err` stops the iteration, with a message to report if `Some`.
    fn run_steps<'a>(
        &'a mut self,
        steps: &'a [CStep],
    ) -> Pin<Box<dyn Future<Output = Result<(), Option<String>>> + Send + 'a>> {
        Box::pin(async move {
            for step in steps {
                match step {
                    CStep::Request(i) => self.request(*i).await?,
                    CStep::WebSocket(i) => self.websocket(*i).await?,
                    CStep::Grpc(i) => self.grpc(*i).await?,
                    CStep::Batch(indexes) => self.batch(indexes).await?,
                    CStep::Sleep(d) => tokio::time::sleep(*d).await,
                    CStep::Group(g, inner) => {
                        let started = Instant::now();
                        let result = self.run_steps(inner).await;
                        let tags = self.group_tags(*g);
                        self.shared.metrics.record_duration(
                            self.shared.b.group_duration,
                            tags,
                            started.elapsed(),
                        );
                        result?;
                    }
                }
            }
            Ok(())
        })
    }

    /// The request with its templates filled in.
    fn render(&mut self, req: &CRequest) -> Result<k9_http::Request, String> {
        let shared = self.shared.clone();
        let flows = &shared.flows;
        let mut cx = RenderCx {
            vars: &self.vars,
            var_names: &flows.vars,
            feeders: &flows.feeders,
            rows: &mut self.rows,
            vu: self.id,
            iter: self.iter,
            env: &self.scenario.env,
        };
        let fail = |e: String| format!("{}: {e}", req.location);
        let mut request = req.request.clone();
        let mut text = String::new();
        if let Some(template) = &req.url_template {
            template.render(&mut text, &mut cx).map_err(fail)?;
            request.url = k9_http::Request::new(request.method.clone(), &text)
                .map_err(|e| fail(e.to_string()))?
                .url;
        }
        for (name, template) in &req.header_templates {
            text.clear();
            template.render(&mut text, &mut cx).map_err(fail)?;
            let value = http::HeaderValue::try_from(text.as_str())
                .map_err(|e| fail(format!("header `{name}`: {e}")))?;
            request.headers.append(name.clone(), value);
        }
        if let Some(template) = &req.body_template {
            text.clear();
            template.render(&mut text, &mut cx).map_err(fail)?;
            request.body = bytes::Bytes::from(text);
        }
        if let Some(parts) = &req.form {
            let (content_type, body) = build_form(parts, |template| {
                let mut text = String::new();
                template.render(&mut text, &mut cx)?;
                Ok(text)
            })
            .map_err(fail)?;
            let content_type =
                http::HeaderValue::try_from(content_type).expect("a boundary is a token");
            request
                .headers
                .insert(http::header::CONTENT_TYPE, content_type);
            request.body = body;
        }
        if !req.compression.is_empty() && (req.body_template.is_some() || req.form.is_some()) {
            request.body = k9_http::compression::compress(&request.body, &req.compression)
                .map_err(|e| fail(e.to_string()))?;
        }
        Ok(request)
    }

    async fn request(&mut self, index: usize) -> Result<(), Option<String>> {
        let shared = self.shared.clone();
        let req = &shared.flows.requests[index];
        let rendered;
        let request = if req.is_templated() {
            rendered = self.render(req).map_err(Some)?;
            &rendered
        } else {
            &req.request
        };
        let result = self.client.send(request).await;
        self.finish_request(index, result)
    }

    /// Sends a batch's requests at once, within its limits (per host first,
    /// so a request waiting for its host holds no overall slot), then
    /// finishes each in order. The first error, if any, ends the iteration
    /// after all are recorded.
    async fn batch(&mut self, indexes: &[usize]) -> Result<(), Option<String>> {
        let shared = self.shared.clone();
        let mut requests = Vec::with_capacity(indexes.len());
        for &index in indexes {
            let req = &shared.flows.requests[index];
            requests.push(if req.is_templated() {
                std::borrow::Cow::Owned(self.render(req).map_err(Some)?)
            } else {
                std::borrow::Cow::Borrowed(&req.request)
            });
        }
        let (all, per_host) = shared.flows.batch_limits;
        let all = tokio::sync::Semaphore::new(all as usize);
        let per_host_limit = match per_host {
            0 => tokio::sync::Semaphore::MAX_PERMITS,
            n => n as usize,
        };
        let host = |r: &k9_http::Request| {
            (
                r.url.host_str().unwrap_or_default().to_owned(),
                r.url.port_or_known_default(),
            )
        };
        let hosts: HashMap<_, _> = requests
            .iter()
            .map(|r| (host(r), tokio::sync::Semaphore::new(per_host_limit)))
            .collect();
        let client = &self.client;
        let results = futures_util::future::join_all(requests.iter().map(|request| async {
            let _host = hosts[&host(request)].acquire().await;
            let _slot = all.acquire().await;
            client.send(request).await
        }))
        .await;
        drop(requests);
        let mut first_error = None;
        for (&index, result) in indexes.iter().zip(results) {
            if let Err(err) = self.finish_request(index, result) {
                first_error.get_or_insert(err);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Records a request's outcome, checks it and runs its extractors.
    fn finish_request(
        &mut self,
        index: usize,
        result: Result<k9_http::Response, k9_http::HttpError>,
    ) -> Result<(), Option<String>> {
        let shared = self.shared.clone();
        let req = &shared.flows.requests[index];
        match result {
            Ok(res) => {
                // Each followed redirect is its own request sample, tagged
                // with its own URL (as its name too), like k6.
                for hop in &res.redirects {
                    let url = hop.url.as_str();
                    let proto = k9_http::proto(hop.version);
                    let tls = tls_version(hop.tls);
                    let tags = self.tags_for(index, hop.status.as_u16(), proto, tls, url, url);
                    shared.record_exchange(tags, hop.status.as_u16(), &hop.timings);
                    shared.record_data(
                        self.scenario.iteration_tags,
                        hop.bytes_sent,
                        hop.bytes_received,
                    );
                }
                let status = res.status.as_u16();
                let proto = k9_http::proto(res.version);
                let tags = if res.redirects.is_empty() {
                    self.request_tags(index, status, proto, tls_version(res.tls))
                } else {
                    let name = req.name_for(res.url.as_str());
                    let tls = tls_version(res.tls);
                    self.tags_for(index, status, proto, tls, res.url.as_str(), name)
                };
                shared.record_exchange(tags, status, &res.timings);
                shared.record_data(
                    self.scenario.iteration_tags,
                    res.bytes_sent,
                    res.bytes_received,
                );
                self.record_checks(index, status, &res.body, Some(res.timings.duration()));
                let mut json = None;
                for extract in &req.extract {
                    let value = extract
                        .extractor
                        .extract(&res.headers, &res.body, &mut json);
                    match value {
                        Some(value) => self.vars[extract.var] = Some(value),
                        None => {
                            return Err(Some(format!(
                                "{}: extractor `{}` found nothing in the response (status {status})",
                                req.location, shared.flows.vars[extract.var]
                            )));
                        }
                    }
                }
            }
            Err(err) => {
                let tags = self.request_tags(index, 0, "", "");
                shared.record_failure(tags, err.to_string());
                // No response: every check fails (status 0, empty body, no duration).
                self.record_checks(index, 0, &[], None);
                // Nothing to extract: stop, without a second error.
                if !req.extract.is_empty() {
                    return Err(None);
                }
            }
        }
        Ok(())
    }

    /// `template` filled in.
    fn render_text(&mut self, template: &Template) -> Result<String, String> {
        let shared = self.shared.clone();
        let flows = &shared.flows;
        let mut cx = RenderCx {
            vars: &self.vars,
            var_names: &flows.vars,
            feeders: &flows.feeders,
            rows: &mut self.rows,
            vu: self.id,
            iter: self.iter,
            env: &self.scenario.env,
        };
        let mut text = String::new();
        template.render(&mut text, &mut cx)?;
        Ok(text)
    }

    fn render_ws(&mut self, ws: &CWebSocket) -> Result<k9_http::WsRequest, String> {
        let fail = |e: String| format!("{}: {e}", ws.location);
        let mut request = ws.request.clone();
        if let Some(template) = &ws.url_template {
            let url = self.render_text(template).map_err(fail)?;
            request.url = k9_http::WsRequest::new(&url)
                .map_err(|e| fail(e.to_string()))?
                .url;
        }
        for (name, template) in &ws.header_templates {
            let text = self.render_text(template).map_err(fail)?;
            let value = http::HeaderValue::try_from(text)
                .map_err(|e| fail(format!("header `{name}`: {e}")))?;
            request.headers.append(name.clone(), value);
        }
        Ok(request)
    }

    /// A WebSocket step: the handshake, its steps, then a normal close. A
    /// step that can't go on (no message, a closed socket) ends the session
    /// and the flow goes on, unless an extractor needed that message.
    async fn websocket(&mut self, index: usize) -> Result<(), Option<String>> {
        let shared = self.shared.clone();
        let spec = &shared.flows.websockets[index];
        let request = if spec.is_templated() {
            self.render_ws(spec).map_err(Some)?
        } else {
            spec.request.clone()
        };
        let started = Instant::now();
        let ws = match self.client.websocket(&request).await {
            Ok(ws) => ws,
            Err(err) => {
                shared.record_request_error(err.to_string());
                self.record_ws_checks(&spec.checks, &spec.group, 0, b"", None);
                return Ok(());
            }
        };
        let status = ws.status.as_u16();
        self.record_ws_checks(&spec.checks, &spec.group, status, b"", Some(ws.connecting));
        let url = if spec.url_template.is_some() {
            spec.url.as_str()
        } else {
            request.url.as_str()
        };
        let status_str = status.to_string();
        let mut tags: Vec<(&str, &str)> = self
            .scenario
            .tags
            .iter()
            .chain(&spec.tags)
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        tags.extend([
            ("url", url),
            ("name", spec.name.as_deref().unwrap_or(url)),
            ("group", spec.group.as_str()),
            ("status", status_str.as_str()),
        ]);
        let tags = shared.intern(&tags);
        let mut session = WsSession::new(
            ws,
            shared.clone(),
            tags,
            self.scenario.iteration_tags,
            started,
        );
        // Messages that arrived while a ping waited for its pong.
        let mut early = std::collections::VecDeque::new();
        for step in &spec.steps {
            match step {
                CWsStep::Send(text) => {
                    let text = match text {
                        CText::Static(text) => text.clone(),
                        CText::Template(template) => self
                            .render_text(template)
                            .map_err(|e| Some(format!("{}: {e}", spec.location)))?,
                    };
                    if session.send(k9_http::WsMessage::Text(text)).await.is_err() {
                        return Ok(());
                    }
                }
                CWsStep::Receive {
                    location,
                    timeout,
                    checks,
                    extract,
                } => {
                    let waiting = Instant::now();
                    let received = match early.pop_front() {
                        Some(message) => Ok(message),
                        None => next_message(&mut session, *timeout).await,
                    };
                    let message = match received {
                        Ok(message) => message,
                        Err(missed) => {
                            let why = match missed {
                                Missed::Timeout(t) => Some(format!("no message within {t:?}")),
                                Missed::Closed => {
                                    Some("closed before a message arrived".to_owned())
                                }
                                Missed::Failed => None,
                            };
                            if let Some(why) = why {
                                shared.record_request_error(format!("WebSocket {url}: {why}"));
                            }
                            self.record_ws_checks(checks, &spec.group, 0, b"", None);
                            return if extract.is_empty() {
                                Ok(())
                            } else {
                                Err(None)
                            };
                        }
                    };
                    let body: &[u8] = match &message {
                        k9_http::WsMessage::Text(text) => text.as_bytes(),
                        k9_http::WsMessage::Binary(data) => data,
                    };
                    self.record_ws_checks(checks, &spec.group, 0, body, Some(waiting.elapsed()));
                    let mut json = None;
                    for e in extract {
                        match e
                            .extractor
                            .extract(&http::HeaderMap::new(), body, &mut json)
                        {
                            Some(value) => self.vars[e.var] = Some(value),
                            None => {
                                return Err(Some(format!(
                                    "{location}: extractor `{}` found nothing in the message",
                                    shared.flows.vars[e.var]
                                )));
                            }
                        }
                    }
                }
                CWsStep::Ping => {
                    if session.ping().await.is_err() {
                        return Ok(());
                    }
                    loop {
                        match session.recv().await {
                            Some(Ok(k9_http::WsEvent::Pong(_))) => break,
                            Some(Ok(k9_http::WsEvent::Message(message))) => {
                                early.push_back(message)
                            }
                            _ => return Ok(()),
                        }
                    }
                }
                CWsStep::Sleep(d) => tokio::time::sleep(*d).await,
                CWsStep::Close { code, reason } => {
                    close(&mut session, *code, reason).await;
                    return Ok(());
                }
            }
        }
        close(&mut session, 1000, "").await;
        Ok(())
    }

    /// `message` with its templates filled in.
    fn render_message(&mut self, message: &CMessage) -> Result<serde_json::Value, String> {
        let mut value = message.value.clone();
        for (pointer, template) in &message.templates {
            let text = self.render_text(template)?;
            *value.pointer_mut(pointer).expect("found at compile time") =
                serde_json::Value::String(text);
        }
        Ok(value)
    }

    /// The call with its templates filled in (a unary call's message
    /// included; a stream's messages are rendered as they're sent).
    fn render_grpc(&mut self, spec: &CGrpc) -> Result<k9_http::grpc::GrpcRequest, String> {
        let fail = |e: String| format!("{}: {e}", spec.location);
        let mut request = spec.request.clone();
        if !spec.streaming && !spec.messages[0].templates.is_empty() {
            let message = self.render_message(&spec.messages[0]).map_err(fail)?;
            request.body = request
                .method
                .encode(&message)
                .map_err(|e| fail(e.to_string()))?;
        }
        for (name, template) in &spec.metadata_templates {
            let text = self.render_text(template).map_err(fail)?;
            let value = http::HeaderValue::try_from(text)
                .map_err(|e| fail(format!("metadata `{name}`: {e}")))?;
            request.metadata.append(name.clone(), value);
        }
        Ok(request)
    }

    /// A gRPC step: a unary call, or a stream that sends its messages,
    /// half-closes and reads every answer. Then its checks and extractors.
    async fn grpc(&mut self, index: usize) -> Result<(), Option<String>> {
        let shared = self.shared.clone();
        let spec = &shared.flows.grpcs[index];
        let rendered;
        let request = if spec.is_templated() {
            rendered = self.render_grpc(spec).map_err(Some)?;
            &rendered
        } else {
            &spec.request
        };
        let messages = if spec.streaming {
            let mut messages = Vec::with_capacity(spec.messages.len());
            for message in &spec.messages {
                messages.push(
                    self.render_message(message)
                        .map_err(|e| Some(format!("{}: {e}", spec.location)))?,
                );
            }
            messages
        } else {
            Vec::new()
        };
        let client = self.grpc_client.get_or_insert_with(|| self.client.grpc());
        let tags = self
            .scenario
            .tags
            .iter()
            .chain(&spec.tags)
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let call = CallTags {
            tags,
            name: spec.name.as_deref(),
            group: &spec.group,
            data_tags: self.scenario.iteration_tags,
        };
        let (status, body, headers, trailers, duration) = if spec.streaming {
            let started = Instant::now();
            let mut stream = grpc::open_stream(
                &shared,
                client,
                &spec.target,
                &request.method,
                request.metadata.clone(),
                spec.timeout,
                &request.options,
                call,
            )
            .await;
            for message in &messages {
                if stream.send(message).await.is_err() {
                    break;
                }
            }
            stream.close_send();
            let mut answers = Vec::new();
            let mut end = k9_http::grpc::GrpcEnd::default();
            while let Some(event) = stream.recv().await {
                match event {
                    k9_http::grpc::GrpcEvent::Message(message) => answers.push(message),
                    k9_http::grpc::GrpcEvent::End(e) => end = e,
                }
            }
            let duration = end.answered.then(|| started.elapsed());
            let body = serde_json::Value::Array(answers).to_string();
            (end.status, body, end.headers, end.trailers, duration)
        } else {
            let (res, duration) = grpc::invoke(&shared, client, request, spec.timeout, call).await;
            let body = res
                .message
                .as_ref()
                .map(serde_json::Value::to_string)
                .unwrap_or_default();
            (res.status, body, res.headers, res.trailers, duration)
        };
        let code = u16::try_from(status).unwrap_or(u16::MAX);
        self.record_ws_checks(&spec.checks, &spec.group, code, body.as_bytes(), duration);
        if duration.is_none() && !spec.extract.is_empty() {
            // No answer (already recorded as an error): nothing to extract.
            return Err(None);
        }
        let mut json = None;
        for e in &spec.extract {
            let value = e
                .extractor
                .extract(&headers, body.as_bytes(), &mut json)
                .or_else(|| e.extractor.extract(&trailers, body.as_bytes(), &mut json));
            match value {
                Some(value) => self.vars[e.var] = Some(value),
                None => {
                    return Err(Some(format!(
                        "{}: extractor `{}` found nothing in the response (status {})",
                        spec.location,
                        shared.flows.vars[e.var],
                        k9_http::grpc::status_name(status)
                    )));
                }
            }
        }
        Ok(())
    }

    fn record_ws_checks(
        &mut self,
        checks: &[CCheck],
        group: &str,
        status: u16,
        body: &[u8],
        duration: Option<std::time::Duration>,
    ) {
        let shared = self.shared.clone();
        for c in checks {
            let passed = match &c.check {
                Check::Status(expected) => expected.matches(status),
                Check::BodyContains(needle) => {
                    duration.is_some() && contains(body, needle.as_bytes())
                }
                Check::DurationBelow(limit) => duration.is_some_and(|d| d < limit.as_std()),
            };
            let tags = match self.ws_check_tags.get(&c.id) {
                Some(id) => *id,
                None => {
                    let mut tags: Vec<(&str, &str)> = self
                        .scenario
                        .tags
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();
                    tags.push(("group", group));
                    tags.push(("check", c.name.as_str()));
                    let id = shared.intern(&tags);
                    self.ws_check_tags.insert(c.id, id);
                    id
                }
            };
            shared
                .metrics
                .record(shared.b.checks, tags, f64::from(passed));
        }
    }

    fn record_checks(
        &mut self,
        index: usize,
        status: u16,
        body: &[u8],
        duration: Option<std::time::Duration>,
    ) {
        let shared = self.shared.clone();
        let req = &shared.flows.requests[index];
        for (i, (name, check)) in req.checks.iter().enumerate() {
            let passed = match check {
                Check::Status(expected) => expected.matches(status),
                Check::BodyContains(needle) => contains(body, needle.as_bytes()),
                Check::DurationBelow(limit) => duration.is_some_and(|d| d < limit.as_std()),
            };
            let tags = match self.check_tags.get(&(index, i)) {
                Some(id) => *id,
                None => {
                    let mut tags: Vec<(&str, &str)> = self
                        .scenario
                        .tags
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();
                    tags.push(("group", req.group.as_str()));
                    tags.push(("check", name.as_str()));
                    let id = shared.intern(&tags);
                    self.check_tags.insert((index, i), id);
                    id
                }
            };
            shared
                .metrics
                .record(shared.b.checks, tags, f64::from(passed));
        }
    }

    /// Tags for a response from the plan's own URL; cached per (request, status).
    fn request_tags(
        &mut self,
        index: usize,
        status: u16,
        proto: &'static str,
        tls: &'static str,
    ) -> TagSetId {
        let key = (index, status, proto, tls);
        if let Some(id) = self.request_tags.get(&key) {
            return *id;
        }
        let shared = self.shared.clone();
        let req = &shared.flows.requests[index];
        let id = self.tags_for(index, status, proto, tls, &req.url, &req.name);
        self.request_tags.insert(key, id);
        id
    }

    /// Tags for a response from `url` (which differs from the plan's URL after
    /// a redirect). Not cached: redirect targets can vary.
    fn tags_for(
        &self,
        index: usize,
        status: u16,
        proto: &str,
        tls: &str,
        url: &str,
        name: &str,
    ) -> TagSetId {
        let req = &self.shared.flows.requests[index];
        let status_str = status.to_string();
        let mut tags: Vec<(&str, &str)> = self
            .scenario
            .tags
            .iter()
            .chain(&req.tags)
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        tags.extend([
            ("method", req.method),
            ("url", url),
            ("name", name),
            ("group", req.group.as_str()),
            ("status", status_str.as_str()),
            (
                "expected_response",
                if is_expected(status) { "true" } else { "false" },
            ),
        ]);
        // No `proto` without a response, and no `tls_version` without TLS.
        if !proto.is_empty() {
            tags.push(("proto", proto));
        }
        if !tls.is_empty() {
            tags.push(("tls_version", tls));
        }
        self.shared.intern(&tags)
    }

    fn group_tags(&mut self, group: usize) -> TagSetId {
        if let Some(id) = self.group_tags.get(&group) {
            return *id;
        }
        let path = &self.shared.flows.groups[group];
        let mut tags: Vec<(&str, &str)> = self
            .scenario
            .tags
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        tags.push(("group", path.as_str()));
        let id = self.shared.intern(&tags);
        self.group_tags.insert(group, id);
        id
    }
}

/// A shuffled round of transaction indexes, each as many times as its weight.
fn deal(weights: impl Iterator<Item = u32>) -> Vec<usize> {
    let mut deck = Vec::new();
    for (i, weight) in weights.enumerate() {
        deck.extend(std::iter::repeat_n(i, weight as usize));
    }
    fastrand::shuffle(&mut deck);
    deck
}

/// The next message within `timeout`, skipping pongs.
async fn next_message(
    session: &mut WsSession,
    timeout: std::time::Duration,
) -> Result<k9_http::WsMessage, Missed> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match tokio::time::timeout_at(deadline, session.recv()).await {
            Err(_) => return Err(Missed::Timeout(timeout)),
            Ok(Some(Ok(k9_http::WsEvent::Message(message)))) => return Ok(message),
            Ok(Some(Ok(k9_http::WsEvent::Pong(_)))) => {}
            Ok(Some(Ok(k9_http::WsEvent::Close(_))) | None) => return Err(Missed::Closed),
            Ok(Some(Err(_))) => return Err(Missed::Failed),
        }
    }
}

/// Closes and waits (briefly) for the peer's answer, so the session's
/// duration covers the closing handshake.
async fn close(session: &mut WsSession, code: u16, reason: &str) {
    if session.close(code, reason).await.is_ok() {
        let drain = async { while session.recv().await.is_some() {} };
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), drain).await;
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || haystack.windows(needle.len()).any(|w| w == needle)
}

/// k6's default: 2xx and 3xx responses are expected; everything else fails.
/// k6's `tls_version` tag value; empty without TLS.
pub(crate) fn tls_version(tls: Option<k9_http::TlsInfo>) -> &'static str {
    tls.map_or("", |t| t.version)
}

pub(crate) fn is_expected(status: u16) -> bool {
    (200..400).contains(&status)
}

/// How many iterations a VU may still start.
#[derive(Clone)]
pub(crate) enum Budget {
    Unlimited,
    Shared(Arc<AtomicU64>),
    PerVu(u64),
}

impl Budget {
    pub fn claim(&mut self) -> bool {
        match self {
            Budget::Unlimited => true,
            Budget::Shared(left) => left
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok(),
            Budget::PerVu(left) => {
                let ok = *left > 0;
                *left = left.saturating_sub(1);
                ok
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_round_holds_each_transaction_by_weight_in_a_shuffled_order() {
        let mut orders = std::collections::HashSet::new();
        for _ in 0..50 {
            let deck = deal([3, 1, 2].into_iter());
            let mut sorted = deck.clone();
            sorted.sort();
            assert_eq!(sorted, [0, 0, 0, 1, 2, 2]);
            orders.insert(deck);
        }
        // 60 orders are possible; 50 identical draws would be a fluke.
        assert!(orders.len() > 1);
    }
}
