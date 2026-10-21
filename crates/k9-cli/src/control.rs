//! The control API (`--address`): k6's REST API status and metrics endpoints,
//! in their JSON:API form. `GET /v1/status` reports the run; `PATCH /v1/status`
//! pauses, resumes, scales (`vus`) or stops it; `GET /v1/metrics[/{id}]` has
//! every metric's totals so far (updated each second). The `k9 status`,
//! `stats`, `pause`, `resume`, `scale` and `stop` commands are its client.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use k9_core::engine::{ControlError, LiveMetric, RunControl, Status};
use k9_metrics::{MetricKind, ValueType};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::exit_code;

/// Listens on `address` (e.g. `localhost:6565`, or port 0 for any).
pub(crate) async fn bind(address: &str) -> Result<TcpListener, String> {
    TcpListener::bind(address)
        .await
        .map_err(|e| format!("control API: can't listen on {address}: {e}"))
}

/// Serves the API until the process exits.
pub(crate) fn serve(listener: TcpListener, control: RunControl) {
    // Holding a receiver makes the engine publish metrics.
    let metrics = control.metrics();
    let app = Router::new()
        .route("/v1/status", get(get_status).patch(patch_status))
        .route("/v1/metrics", get(get_metrics))
        .route("/v1/metrics/{id}", get(get_metric))
        .fallback(|| async { error(StatusCode::NOT_FOUND, "no such endpoint".into()) })
        .with_state(Api { control, metrics });
    tokio::spawn(async move { axum::serve(listener, app).await });
}

#[derive(Deserialize)]
struct Document {
    data: Data,
}

#[derive(Deserialize)]
struct Data {
    #[serde(default)]
    attributes: Attributes,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
struct Attributes {
    paused: Option<bool>,
    vus: Option<u32>,
    vus_max: Option<u32>,
    stopped: Option<bool>,
}

#[derive(Clone)]
struct Api {
    control: RunControl,
    metrics: watch::Receiver<Arc<Vec<LiveMetric>>>,
}

async fn get_status(State(api): State<Api>) -> Response {
    status_document(api.control.status())
}

async fn get_metrics(State(api): State<Api>) -> Response {
    let metrics = api.metrics.borrow().clone();
    let list: Vec<Value> = metrics.iter().map(metric_json).collect();
    json_response(StatusCode::OK, json!({ "data": list }))
}

async fn get_metric(State(api): State<Api>, Path(id): Path<String>) -> Response {
    let metrics = api.metrics.borrow().clone();
    match metrics.iter().find(|m| m.name == id) {
        Some(metric) => json_response(StatusCode::OK, json!({ "data": metric_json(metric) })),
        None => error(StatusCode::NOT_FOUND, format!("no metric `{id}` (yet)")),
    }
}

fn metric_json(metric: &LiveMetric) -> Value {
    let sample: serde_json::Map<String, Value> = metric
        .sample
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect();
    json!({
        "type": "metrics",
        "id": metric.name,
        "attributes": {
            "type": kind_name(metric.kind),
            "contains": match metric.contains {
                ValueType::Default => "default",
                ValueType::Time => "time",
                ValueType::Data => "data",
            },
            "tainted": null,
            "sample": sample,
        }
    })
}

fn kind_name(kind: MetricKind) -> &'static str {
    match kind {
        MetricKind::Counter => "counter",
        MetricKind::Gauge => "gauge",
        MetricKind::Rate => "rate",
        MetricKind::Trend => "trend",
    }
}

async fn patch_status(State(Api { control, .. }): State<Api>, body: Bytes) -> Response {
    let attributes = match serde_json::from_slice::<Document>(&body) {
        Ok(document) => document.data.attributes,
        Err(e) => return error(StatusCode::BAD_REQUEST, format!("invalid status: {e}")),
    };
    if let Some(max) = attributes.vus_max {
        let current = control.status().vus_max;
        if max != current {
            return error(
                StatusCode::BAD_REQUEST,
                format!(
                    "vus-max can't change during a k9 run (it is {current}); set the scenario's maxVUs"
                ),
            );
        }
    }
    let applied = (|| {
        if let Some(vus) = attributes.vus {
            control.scale(vus)?;
        }
        match attributes.paused {
            Some(true) => control.pause()?,
            Some(false) => control.resume()?,
            None => {}
        }
        Ok::<_, ControlError>(())
    })();
    if let Err(e) = applied {
        return error(StatusCode::BAD_REQUEST, e.to_string());
    }
    if attributes.stopped == Some(true) {
        control.stop();
    }
    status_document(control.status())
}

fn status_document(status: Status) -> Response {
    let body = json!({
        "data": {
            "type": "status",
            "id": "default",
            "attributes": {
                "paused": status.paused,
                "running": status.running,
                "stopped": status.stopped,
                "vus": status.vus,
                "vus-max": status.vus_max,
            }
        }
    });
    json_response(StatusCode::OK, body)
}

/// A JSON:API error document.
fn error(status: StatusCode, detail: String) -> Response {
    let body = json!({
        "errors": [{
            "status": status.as_u16().to_string(),
            "title": status.canonical_reason().unwrap_or_default(),
            "detail": detail,
        }]
    });
    json_response(status, body)
}

fn json_response(status: StatusCode, body: Value) -> Response {
    (
        status,
        [("content-type", "application/vnd.api+json")],
        body.to_string(),
    )
        .into_response()
}

/// What a control command does.
pub(crate) enum Action {
    Status,
    Stats,
    Pause,
    Resume,
    Scale(u32),
    Stop,
}

const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs a control command against the API at `address`; prints the run's
/// status (or metrics) and returns the exit code.
pub(crate) fn command(address: &str, action: Action) -> i32 {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("error: {e}");
            return exit_code::COMMAND_FAILED;
        }
    };
    let result = runtime.block_on(async {
        let (method, path, attributes) = match action {
            Action::Status => ("GET", "/v1/status", None),
            Action::Stats => ("GET", "/v1/metrics", None),
            Action::Pause => ("PATCH", "/v1/status", Some(json!({ "paused": true }))),
            Action::Resume => ("PATCH", "/v1/status", Some(json!({ "paused": false }))),
            Action::Scale(vus) => ("PATCH", "/v1/status", Some(json!({ "vus": vus }))),
            Action::Stop => ("PATCH", "/v1/status", Some(json!({ "stopped": true }))),
        };
        let body = attributes.map(|attributes| {
            json!({ "data": { "type": "status", "id": "default", "attributes": attributes } })
        });
        call(address, method, path, body).await
    });
    match result {
        Ok(document) if document["data"].is_array() => {
            print!("{}", render_stats(&document["data"]));
            exit_code::OK
        }
        Ok(document) => {
            print!("{}", render_status(&document["data"]["attributes"]));
            exit_code::OK
        }
        Err(message) => {
            eprintln!("error: {message}");
            exit_code::COMMAND_FAILED
        }
    }
}

async fn call(
    address: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> Result<Value, String> {
    let url = if address.contains("://") {
        format!("{}{path}", address.trim_end_matches('/'))
    } else {
        format!("http://{address}{path}")
    };
    let client = k9_http::Client::new(k9_http::ClientConfig {
        max_redirects: 0,
        ..k9_http::ClientConfig::default()
    })
    .map_err(|e| e.to_string())?;
    let method = axum::http::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?;
    let mut request = k9_http::Request::new(method, &url).map_err(|e| e.to_string())?;
    request.keep_body = true;
    if let Some(body) = body {
        request.headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        );
        request.body = body.to_string().into_bytes().into();
    }
    let response = client
        .send_with_timeout(&request, COMMAND_TIMEOUT)
        .await
        .map_err(|e| format!("control API at {address}: {e}"))?;
    let document: Value = serde_json::from_slice(&response.body).map_err(|e| {
        format!(
            "control API at {address}: unexpected response (status {}): {e}",
            response.status.as_u16()
        )
    })?;
    if response.status.is_success() {
        Ok(document)
    } else {
        let detail = document["errors"][0]["detail"]
            .as_str()
            .unwrap_or("request refused")
            .to_owned();
        Err(detail)
    }
}

/// `key: value` lines (YAML), in the API's order.
fn render_status(attributes: &Value) -> String {
    let mut out = String::new();
    for key in ["paused", "running", "stopped", "vus", "vus-max"] {
        out.push_str(&format!("{key}: {}\n", attributes[key]));
    }
    out
}

/// One line per metric: `name (type): key=value …`.
fn render_stats(metrics: &Value) -> String {
    let mut out = String::new();
    for metric in metrics.as_array().into_iter().flatten() {
        let attributes = &metric["attributes"];
        let sample: Vec<String> = attributes["sample"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| {
                let v = v.as_f64().unwrap_or_default();
                format!("{k}={}", k9_output::format::number(v))
            })
            .collect();
        out.push_str(&format!(
            "{} ({}): {}\n",
            metric["id"].as_str().unwrap_or_default(),
            attributes["type"].as_str().unwrap_or_default(),
            sample.join(" ")
        ));
    }
    out
}
