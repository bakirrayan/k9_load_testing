//! Turning a plan's flows into ready-to-send requests, once, before the run.

use std::collections::HashMap;

use bytes::Bytes;
use http::header::{HeaderName, HeaderValue};

use crate::Segment;
use crate::engine::EngineError;
use crate::engine::extract::CExtractor;
use crate::engine::feeder::CFeeder;
use crate::engine::grpc::{LoadedProtos, proto_key};
use crate::engine::template::{Scope, Template, is_identifier};
use crate::plan::{
    Check, Extractor, FormPart, GrpcSpec, Method, Plan, RequestSpec, Step, WsSpec, WsStep,
};

/// How long a `receive` step waits by default.
const RECEIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

pub(crate) enum CStep {
    /// Index into [`Flows::requests`].
    Request(usize),
    /// Index into [`Flows::websockets`].
    WebSocket(usize),
    /// Index into [`Flows::grpcs`].
    Grpc(usize),
    /// Indexes into [`Flows::requests`], sent in parallel.
    Batch(Vec<usize>),
    Sleep(std::time::Duration),
    /// Index into [`Flows::groups`], plus the group's steps.
    Group(usize, Vec<CStep>),
}

pub(crate) struct CRequest {
    /// Where it is in the plan, e.g. `flows.default[0]`.
    pub location: String,
    /// Ready to send, unless it has templates: then it holds the static parts
    /// (its URL is a placeholder if `url_template` is set).
    pub request: k9_http::Request,
    pub url_template: Option<Template>,
    pub header_templates: Vec<(HeaderName, Template)>,
    pub body_template: Option<Template>,
    /// A multipart form with templated fields, built for each request (a
    /// static form is already in `request`).
    pub form: Option<Vec<CPart>>,
    /// Applied to the body (a static one is already compressed).
    pub compression: Vec<k9_http::compression::Encoding>,
    pub extract: Vec<Extract>,
    pub method: &'static str,
    pub url: String,
    /// `name` tag: the spec's name, or the URL.
    pub name: String,
    /// Whether `name` came from the plan rather than defaulting to the URL.
    pub explicit_name: bool,
    /// `group` tag of the enclosing group path, e.g. `::login::profile`.
    pub group: String,
    pub tags: Vec<(String, String)>,
    pub checks: Vec<(String, Check)>,
}

pub(crate) enum CPart {
    Text(String, CText),
    File {
        name: String,
        filename: String,
        content_type: Option<String>,
        data: Bytes,
    },
}

/// A check with a plan-wide id, so each VU caches its tag set.
pub(crate) struct CCheck {
    pub id: usize,
    pub name: String,
    pub check: Check,
}

pub(crate) struct CWebSocket {
    pub location: String,
    /// Ready to open, unless templated (then its URL is a placeholder).
    pub request: k9_http::WsRequest,
    pub url_template: Option<Template>,
    pub header_templates: Vec<(HeaderName, Template)>,
    /// The URL as written: the `url` tag when it's templated.
    pub url: String,
    pub name: Option<String>,
    pub group: String,
    pub tags: Vec<(String, String)>,
    /// On the handshake.
    pub checks: Vec<CCheck>,
    pub steps: Vec<CWsStep>,
}

impl CWebSocket {
    pub fn is_templated(&self) -> bool {
        self.url_template.is_some() || !self.header_templates.is_empty()
    }
}

pub(crate) struct CGrpc {
    pub location: String,
    /// A unary call ready to send, unless templated: then its message is
    /// rendered and its templated metadata added, for each call. For a
    /// stream, its URL, method and static metadata (no body).
    pub request: k9_http::grpc::GrpcRequest,
    /// The target's base URL.
    pub target: url::Url,
    pub streaming: bool,
    /// One for a unary call; a stream's, in order.
    pub messages: Vec<CMessage>,
    pub metadata_templates: Vec<(HeaderName, Template)>,
    pub name: Option<String>,
    pub group: String,
    pub tags: Vec<(String, String)>,
    pub timeout: Option<std::time::Duration>,
    pub checks: Vec<CCheck>,
    pub extract: Vec<Extract>,
}

pub(crate) struct CMessage {
    pub value: serde_json::Value,
    /// JSON pointers to the strings in `value` that are templates.
    pub templates: Vec<(String, Template)>,
}

impl CGrpc {
    pub fn is_templated(&self) -> bool {
        self.messages.iter().any(|m| !m.templates.is_empty()) || !self.metadata_templates.is_empty()
    }
}

pub(crate) enum CText {
    Static(String),
    Template(Template),
}

pub(crate) enum CWsStep {
    Send(CText),
    Receive {
        location: String,
        timeout: std::time::Duration,
        checks: Vec<CCheck>,
        extract: Vec<Extract>,
    },
    Ping,
    Sleep(std::time::Duration),
    Close {
        code: u16,
        reason: String,
    },
}

pub(crate) struct Extract {
    /// Index into [`Flows::vars`].
    pub var: usize,
    pub extractor: CExtractor,
}

impl CRequest {
    pub fn is_templated(&self) -> bool {
        self.url_template.is_some()
            || !self.header_templates.is_empty()
            || self.body_template.is_some()
            || self.form.is_some()
    }

    /// The `name` tag for a response from `final_url`: the plan's explicit
    /// name if it has one, otherwise the URL that answered.
    pub fn name_for<'a>(&'a self, final_url: &'a str) -> &'a str {
        if self.explicit_name {
            &self.name
        } else {
            final_url
        }
    }
}

pub(crate) struct Flows {
    pub flows: HashMap<String, Vec<CStep>>,
    pub requests: Vec<CRequest>,
    pub websockets: Vec<CWebSocket>,
    pub grpcs: Vec<CGrpc>,
    /// `batch` and `batchPerHost` (0: no limit per host).
    pub batch_limits: (u32, u32),
    /// `localIPs`: VU `n` sends from the `n - 1`-th, wrapping around.
    pub local_ips: k9_http::IpPool,
    /// `noVUConnectionReuse`: close a VU's connections between iterations.
    pub no_vu_connection_reuse: bool,
    /// `minIterationDuration`: shorter iterations are padded with a sleep.
    pub min_iteration_duration: Option<std::time::Duration>,
    /// `noCookiesReset`: VUs keep their cookies across iterations.
    pub no_cookies_reset: bool,
    /// `throw`: script requests without a response raise.
    pub throw: bool,
    pub system_tags: super::tags::SystemTags,
    /// How many [`CCheck`]s there are (their ids are `0..checks`).
    pub checks: usize,
    /// Group paths, e.g. `::login`.
    pub groups: Vec<String>,
    /// Names of the variables extractors set; a VU keeps their values
    /// across iterations.
    pub vars: Vec<String>,
    pub feeders: Vec<CFeeder>,
}

/// `segment`: which part of sequential feeders' rows this engine uses.
/// `protos`: every gRPC step's, loaded beforehand.
pub(crate) fn compile(
    plan: &Plan,
    segment: &Segment,
    protos: &LoadedProtos,
) -> Result<Flows, EngineError> {
    let network = super::network(&plan.options.http)?;
    let feeders = plan
        .feeders
        .iter()
        .map(|(name, spec)| {
            CFeeder::load(name, spec, segment).map_err(|message| EngineError::InvalidFeeder {
                name: name.clone(),
                message,
            })
        })
        .collect::<Result<_, _>>()?;
    let mut vars = Vec::new();
    for (name, steps) in &plan.flows {
        collect_vars(steps, &format!("flows.{name}"), &mut vars)?;
    }
    let mut out = Flows {
        flows: HashMap::new(),
        requests: Vec::new(),
        websockets: Vec::new(),
        grpcs: Vec::new(),
        batch_limits: (plan.options.http.batch, plan.options.http.batch_per_host),
        local_ips: network.local_ips,
        no_vu_connection_reuse: plan.options.http.no_vu_connection_reuse,
        min_iteration_duration: plan.options.min_iteration_duration.map(|d| d.0),
        no_cookies_reset: plan.options.no_cookies_reset,
        throw: plan.options.throw,
        system_tags: super::tags::SystemTags::new(&plan.options.system_tags),
        checks: 0,
        groups: Vec::new(),
        vars,
        feeders,
    };
    for (name, steps) in &plan.flows {
        let compiled = compile_steps(steps, &format!("flows.{name}"), "", protos, &mut out)?;
        out.flows.insert(name.clone(), compiled);
    }
    Ok(out)
}

/// Every extractor's variable, in order of first appearance.
fn collect_vars(steps: &[Step], path: &str, vars: &mut Vec<String>) -> Result<(), EngineError> {
    for (i, step) in steps.iter().enumerate() {
        let location = format!("{path}[{i}]");
        let names: Vec<&String> = match step {
            Step::Request(spec) => spec.extract.keys().collect(),
            Step::Websocket(ws) => ws
                .steps
                .iter()
                .flat_map(|step| match step {
                    WsStep::Receive(receive) => receive.extract.keys().collect(),
                    _ => Vec::new(),
                })
                .collect(),
            Step::Grpc(spec) => spec.extract.keys().collect(),
            Step::Batch(specs) => specs.iter().flat_map(|s| s.extract.keys()).collect(),
            Step::Sleep(_) | Step::Group { .. } => Vec::new(),
        };
        for name in names {
            if !is_identifier(name) || name.starts_with("__") {
                return Err(EngineError::InvalidRequest {
                    location,
                    message: format!(
                        "extract: `{name}` isn't a variable name (letters, digits and `_`, not starting with `__`)"
                    ),
                });
            }
            if !vars.contains(name) {
                vars.push(name.clone());
            }
        }
        match step {
            Step::Request(_)
            | Step::Websocket(_)
            | Step::Grpc(_)
            | Step::Batch(_)
            | Step::Sleep(_) => {}
            Step::Group { steps, .. } => collect_vars(steps, &format!("{location}.steps"), vars)?,
        }
    }
    Ok(())
}

fn compile_steps(
    steps: &[Step],
    path: &str,
    group: &str,
    protos: &LoadedProtos,
    out: &mut Flows,
) -> Result<Vec<CStep>, EngineError> {
    let mut compiled = Vec::with_capacity(steps.len());
    for (i, step) in steps.iter().enumerate() {
        let location = format!("{path}[{i}]");
        compiled.push(match step {
            Step::Request(spec) => {
                let request = compile_request(spec, group, &location, out)?;
                out.requests.push(request);
                CStep::Request(out.requests.len() - 1)
            }
            Step::Websocket(spec) => {
                let ws = compile_websocket(spec, group, &location, out)?;
                out.websockets.push(ws);
                CStep::WebSocket(out.websockets.len() - 1)
            }
            Step::Batch(specs) => {
                let mut indexes = Vec::with_capacity(specs.len());
                for (j, spec) in specs.iter().enumerate() {
                    let at = format!("{location}.batch[{j}]");
                    let request = compile_request(spec, group, &at, out)?;
                    out.requests.push(request);
                    indexes.push(out.requests.len() - 1);
                }
                CStep::Batch(indexes)
            }
            Step::Grpc(spec) => {
                let grpc = compile_grpc(spec, group, &location, protos, out)?;
                out.grpcs.push(grpc);
                CStep::Grpc(out.grpcs.len() - 1)
            }
            Step::Sleep(d) => CStep::Sleep(d.as_std()),
            Step::Group { name, steps } => {
                let group_path = format!("{group}::{name}");
                let inner = compile_steps(
                    steps,
                    &format!("{location}.steps"),
                    &group_path,
                    protos,
                    out,
                )?;
                out.groups.push(group_path);
                CStep::Group(out.groups.len() - 1, inner)
            }
        });
    }
    Ok(compiled)
}

fn compile_request(
    spec: &RequestSpec,
    group: &str,
    location: &str,
    flows: &Flows,
) -> Result<CRequest, EngineError> {
    let invalid = |message: String| EngineError::InvalidRequest {
        location: location.to_owned(),
        message,
    };
    let scope = Scope {
        vars: &flows.vars,
        feeders: &flows.feeders,
    };
    let url_template = Template::parse(&spec.url, &scope).map_err(invalid)?;
    let body_template = match &spec.body {
        Some(body) => Template::parse(body, &scope).map_err(|e| invalid(format!("body: {e}")))?,
        None => None,
    };
    let method = match spec.method {
        Method::Get => http::Method::GET,
        Method::Post => http::Method::POST,
        Method::Put => http::Method::PUT,
        Method::Patch => http::Method::PATCH,
        Method::Delete => http::Method::DELETE,
        Method::Head => http::Method::HEAD,
        Method::Options => http::Method::OPTIONS,
    };
    let url = match url_template {
        // Rendered for each request; the static parts must still be valid.
        Some(_) => "http://template.invalid/",
        None => &spec.url,
    };
    let mut request =
        k9_http::Request::new(method.clone(), url).map_err(|e| invalid(e.to_string()))?;
    let mut header_templates = Vec::new();
    for (key, value) in &spec.headers {
        let name = HeaderName::try_from(key.as_str())
            .map_err(|e| invalid(format!("header `{key}`: {e}")))?;
        match Template::parse(value, &scope).map_err(|e| invalid(format!("header `{key}`: {e}")))? {
            Some(template) => header_templates.push((name, template)),
            None => {
                let value = HeaderValue::try_from(value.as_str())
                    .map_err(|e| invalid(format!("header `{key}`: {e}")))?;
                request.headers.append(name, value);
            }
        }
    }
    let compression = match &spec.compression {
        Some(list) => k9_http::compression::Encoding::parse_list(list)
            .map_err(|e| invalid(format!("compression: {e}")))?,
        None => Vec::new(),
    };
    if !compression.is_empty() {
        let value = k9_http::compression::Encoding::header_value(&compression);
        request.headers.insert(
            http::header::CONTENT_ENCODING,
            HeaderValue::try_from(value).expect("encoding names"),
        );
    }
    if spec.body.is_some() && !spec.multipart.is_empty() {
        return Err(invalid("give body or multipart, not both".to_owned()));
    }
    let mut form = None;
    if !spec.multipart.is_empty() {
        let parts = compile_form(spec, &scope).map_err(invalid)?;
        let templated = parts
            .iter()
            .any(|p| matches!(p, CPart::Text(_, CText::Template(_))));
        if templated {
            form = Some(parts);
        } else {
            let (content_type, body) =
                build_form(&parts, |_| unreachable!("a static form")).map_err(invalid)?;
            request.headers.insert(
                http::header::CONTENT_TYPE,
                HeaderValue::try_from(content_type).expect("a boundary is a token"),
            );
            request.body = body;
        }
    }
    if let (Some(body), None) = (&spec.body, &body_template) {
        request.body = Bytes::from(body.clone());
    }
    if !compression.is_empty() && body_template.is_none() && form.is_none() {
        request.body = k9_http::compression::compress(&request.body, &compression)
            .map_err(|e| invalid(format!("compression: {e}")))?;
    }
    let extract = spec
        .extract
        .iter()
        .map(|(name, extractor)| {
            Ok(Extract {
                var: flows
                    .vars
                    .iter()
                    .position(|v| v == name)
                    .expect("collected"),
                extractor: CExtractor::compile(extractor)
                    .map_err(|e| invalid(format!("extract `{name}`: {e}")))?,
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    request.keep_body = extract.iter().any(|e| e.extractor.needs_body());
    Ok(CRequest {
        location: location.to_owned(),
        request,
        url_template,
        header_templates,
        body_template,
        form,
        compression,
        extract,
        method: method_name(&method),
        url: spec.url.clone(),
        name: spec.name.clone().unwrap_or_else(|| spec.url.clone()),
        explicit_name: spec.name.is_some(),
        checks: spec
            .checks
            .iter()
            .map(|(name, check)| (name.clone(), check.clone()))
            .collect(),
        group: group.to_owned(),
        tags: spec
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    })
}

fn compile_form(spec: &RequestSpec, scope: &Scope<'_>) -> Result<Vec<CPart>, String> {
    let mut parts = Vec::with_capacity(spec.multipart.len());
    for (name, part) in &spec.multipart {
        parts.push(match part {
            FormPart::Text(text) => {
                match Template::parse(text, scope)
                    .map_err(|e| format!("multipart `{name}`: {e}"))?
                {
                    Some(template) => CPart::Text(name.clone(), CText::Template(template)),
                    None => CPart::Text(name.clone(), CText::Static(text.clone())),
                }
            }
            FormPart::File(file) => {
                let data = match &file.data {
                    Some(data) => {
                        use base64::Engine as _;
                        base64::engine::general_purpose::STANDARD
                            .decode(data)
                            .map_err(|e| format!("multipart `{name}`: data: {e}"))?
                    }
                    None => std::fs::read(&file.file)
                        .map_err(|e| format!("multipart `{name}`: reading {}: {e}", file.file))?,
                };
                let filename = file.filename.clone().unwrap_or_else(|| {
                    std::path::Path::new(&file.file)
                        .file_name()
                        .map_or_else(|| file.file.clone(), |n| n.to_string_lossy().into_owned())
                });
                CPart::File {
                    name: name.clone(),
                    filename,
                    content_type: file.content_type.clone(),
                    data: Bytes::from(data),
                }
            }
        });
    }
    Ok(parts)
}

/// The form's `Content-Type` and body, with `render` filling in templates.
pub(crate) fn build_form(
    parts: &[CPart],
    mut render: impl FnMut(&Template) -> Result<String, String>,
) -> Result<(String, Bytes), String> {
    let mut form = k9_http::multipart::Form::new();
    for part in parts {
        form = match part {
            CPart::Text(name, CText::Static(text)) => form.text(name, text),
            CPart::Text(name, CText::Template(template)) => form.text(name, &render(template)?),
            CPart::File {
                name,
                filename,
                content_type,
                data,
            } => form.file(name, filename, content_type.as_deref(), data),
        };
    }
    Ok(form.finish())
}

fn compile_websocket(
    spec: &WsSpec,
    group: &str,
    location: &str,
    flows: &mut Flows,
) -> Result<CWebSocket, EngineError> {
    let invalid = |message: String| EngineError::InvalidRequest {
        location: location.to_owned(),
        message,
    };
    let scope = Scope {
        vars: &flows.vars,
        feeders: &flows.feeders,
    };
    let url_template = Template::parse(&spec.url, &scope).map_err(invalid)?;
    let url = match url_template {
        Some(_) => "ws://template.invalid/",
        None => &spec.url,
    };
    let mut request = k9_http::WsRequest::new(url).map_err(|e| invalid(e.to_string()))?;
    let mut header_templates = Vec::new();
    for (key, value) in &spec.headers {
        let name = HeaderName::try_from(key.as_str())
            .map_err(|e| invalid(format!("header `{key}`: {e}")))?;
        match Template::parse(value, &scope).map_err(|e| invalid(format!("header `{key}`: {e}")))? {
            Some(template) => header_templates.push((name, template)),
            None => {
                let value = HeaderValue::try_from(value.as_str())
                    .map_err(|e| invalid(format!("header `{key}`: {e}")))?;
                request.headers.append(name, value);
            }
        }
    }
    for (name, check) in &spec.checks {
        if matches!(check, Check::BodyContains(_)) {
            return Err(invalid(format!(
                "check `{name}`: a handshake has no body (check its status or durationBelow)"
            )));
        }
    }
    let checks = ccheck(&spec.checks, &mut flows.checks);
    let mut steps = Vec::with_capacity(spec.steps.len());
    for (i, step) in spec.steps.iter().enumerate() {
        let at = format!("{location}.steps[{i}]");
        let invalid = |message: String| EngineError::InvalidRequest {
            location: at.clone(),
            message,
        };
        let scope = Scope {
            vars: &flows.vars,
            feeders: &flows.feeders,
        };
        steps.push(match step {
            WsStep::Send(text) => match Template::parse(text, &scope).map_err(invalid)? {
                Some(template) => CWsStep::Send(CText::Template(template)),
                None => CWsStep::Send(CText::Static(text.clone())),
            },
            WsStep::Receive(receive) => {
                for (name, check) in &receive.checks {
                    if matches!(check, Check::Status(_)) {
                        return Err(invalid(format!(
                            "check `{name}`: a message has no status (check bodyContains or durationBelow)"
                        )));
                    }
                }
                let mut extract = Vec::new();
                for (name, extractor) in &receive.extract {
                    if matches!(extractor, Extractor::Header(_)) {
                        return Err(invalid(format!(
                            "extract `{name}`: a message has no headers (use json or regex)"
                        )));
                    }
                    extract.push(Extract {
                        var: flows.vars.iter().position(|v| v == name).expect("collected"),
                        extractor: CExtractor::compile(extractor)
                            .map_err(|e| invalid(format!("extract `{name}`: {e}")))?,
                    });
                }
                CWsStep::Receive {
                    location: at.clone(),
                    timeout: receive.timeout.map_or(RECEIVE_TIMEOUT, |t| t.as_std()),
                    checks: ccheck(&receive.checks, &mut flows.checks),
                    extract,
                }
            }
            WsStep::Ping => CWsStep::Ping,
            WsStep::Sleep(d) => CWsStep::Sleep(d.as_std()),
            WsStep::Close(close) => CWsStep::Close {
                code: close.code,
                reason: close.reason.clone(),
            },
        });
    }
    Ok(CWebSocket {
        location: location.to_owned(),
        request,
        url_template,
        header_templates,
        url: spec.url.clone(),
        name: spec.name.clone(),
        group: group.to_owned(),
        tags: spec
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        checks,
        steps,
    })
}

fn compile_grpc(
    spec: &GrpcSpec,
    group: &str,
    location: &str,
    protos: &LoadedProtos,
    flows: &mut Flows,
) -> Result<CGrpc, EngineError> {
    let invalid = |message: String| EngineError::InvalidRequest {
        location: location.to_owned(),
        message,
    };
    if spec.target.contains("{{") {
        return Err(invalid(
            "target: templates aren't allowed here (one target per step)".to_owned(),
        ));
    }
    let target =
        k9_http::grpc::target(&spec.target, spec.plaintext).map_err(|e| invalid(e.to_string()))?;
    let method = protos
        .get(&proto_key(spec))
        .expect("loaded before compiling")
        .method(&spec.method)
        .map_err(|e| invalid(e.to_string()))?;
    let scope = Scope {
        vars: &flows.vars,
        feeders: &flows.feeders,
    };
    let streaming = method.is_client_streaming() || method.is_server_streaming();
    let empty = serde_json::Value::Object(Default::default());
    let values = match (&spec.message, spec.messages.as_slice()) {
        (Some(_), [_, ..]) => {
            return Err(invalid("give message or messages, not both".to_owned()));
        }
        (_, [_, ..]) if !streaming => {
            return Err(invalid(format!(
                "messages: {} isn't a streaming method (give one message)",
                spec.method
            )));
        }
        (Some(message), []) => vec![message.clone()],
        (None, []) => vec![empty.clone()],
        (None, messages) => messages.to_vec(),
    };
    let mut messages = Vec::with_capacity(values.len());
    for (i, value) in values.into_iter().enumerate() {
        let what = if streaming {
            format!("messages[{i}]")
        } else {
            "message".to_owned()
        };
        let mut templates = Vec::new();
        find_templates(&value, &mut String::new(), &scope, &mut templates)
            .map_err(|e| invalid(format!("{what}: {e}")))?;
        if templates.is_empty() {
            // Mistakes in static messages show now rather than mid-run.
            method
                .encode(&value)
                .map_err(|e| invalid(format!("{what}: {e}")))?;
        }
        messages.push(CMessage { value, templates });
    }
    let mut request = if streaming {
        k9_http::grpc::GrpcRequest {
            url: target
                .join(method.path())
                .expect("a path joins any base URL"),
            method: method.clone(),
            body: Bytes::new(),
            metadata: http::HeaderMap::new(),
            options: Default::default(),
        }
    } else {
        let first = &messages[0];
        let message = if first.templates.is_empty() {
            &first.value
        } else {
            &empty
        };
        k9_http::grpc::GrpcRequest::new(&target, &method, message)
            .map_err(|e| invalid(e.to_string()))?
    };
    request.options = k9_http::grpc::GrpcOptions {
        gzip: match spec.compression.as_deref() {
            None | Some("identity") => false,
            Some("gzip") => true,
            Some(other) => {
                return Err(invalid(format!(
                    "unknown compression `{other}` (expected gzip)"
                )));
            }
        },
        max_send_size: spec.max_send_size.map(|n| n as usize),
        max_receive_size: spec.max_receive_size.map(|n| n as usize),
    };
    let mut metadata_templates = Vec::new();
    for (key, value) in &spec.metadata {
        let name = HeaderName::try_from(key.as_str())
            .map_err(|e| invalid(format!("metadata `{key}`: {e}")))?;
        match Template::parse(value, &scope)
            .map_err(|e| invalid(format!("metadata `{key}`: {e}")))?
        {
            Some(template) => metadata_templates.push((name, template)),
            None => {
                let value = HeaderValue::try_from(value.as_str())
                    .map_err(|e| invalid(format!("metadata `{key}`: {e}")))?;
                request.metadata.append(name, value);
            }
        }
    }
    let extract = spec
        .extract
        .iter()
        .map(|(name, extractor)| {
            Ok(Extract {
                var: flows
                    .vars
                    .iter()
                    .position(|v| v == name)
                    .expect("collected"),
                extractor: CExtractor::compile(extractor)
                    .map_err(|e| invalid(format!("extract `{name}`: {e}")))?,
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    Ok(CGrpc {
        location: location.to_owned(),
        request,
        target,
        streaming,
        messages,
        metadata_templates,
        name: spec.name.clone(),
        group: group.to_owned(),
        tags: spec
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        timeout: spec.timeout.map(|t| t.as_std()),
        checks: ccheck(&spec.checks, &mut flows.checks),
        extract,
    })
}

/// The strings in `value` that hold templates, by JSON pointer.
fn find_templates(
    value: &serde_json::Value,
    pointer: &mut String,
    scope: &Scope<'_>,
    out: &mut Vec<(String, Template)>,
) -> Result<(), String> {
    let len = pointer.len();
    match value {
        serde_json::Value::String(text) => {
            if let Some(template) = Template::parse(text, scope)? {
                out.push((pointer.clone(), template));
            }
        }
        serde_json::Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                pointer.push_str(&format!("/{i}"));
                find_templates(item, pointer, scope, out)?;
                pointer.truncate(len);
            }
        }
        serde_json::Value::Object(fields) => {
            for (key, item) in fields {
                pointer.push('/');
                pointer.push_str(&key.replace('~', "~0").replace('/', "~1"));
                find_templates(item, pointer, scope, out)?;
                pointer.truncate(len);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Numbers `checks` from `next` on.
fn ccheck(checks: &indexmap::IndexMap<String, Check>, next: &mut usize) -> Vec<CCheck> {
    checks
        .iter()
        .map(|(name, check)| {
            *next += 1;
            CCheck {
                id: *next - 1,
                name: name.clone(),
                check: check.clone(),
            }
        })
        .collect()
}

fn method_name(method: &http::Method) -> &'static str {
    match *method {
        http::Method::GET => "GET",
        http::Method::POST => "POST",
        http::Method::PUT => "PUT",
        http::Method::PATCH => "PATCH",
        http::Method::DELETE => "DELETE",
        http::Method::HEAD => "HEAD",
        _ => "OPTIONS",
    }
}
