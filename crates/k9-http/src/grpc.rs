//! Unary gRPC over the client's HTTP/2: `.proto` files compiled at run time
//! (no `protoc` needed), messages as JSON (protobuf's JSON mapping), the
//! status read from the trailers, timings and bytes like any request's.
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use k9_http::grpc::{GrpcRequest, Protos, target};
//! let protos = Protos::load(&["echo.proto"], &["protos"])?;
//! let method = protos.method("k9.test.Echo/Say")?;
//! let url = target("localhost:50051", true)?;
//! let request = GrpcRequest::new(&url, &method, &serde_json::json!({"text": "hi"}))?;
//! let client = k9_http::Client::new(Default::default())?.grpc();
//! let response = client.grpc_invoke(&request, None).await?;
//! println!("{} {:?}", response.status, response.message);
//! # Ok(()) }
//! ```

use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use http::header::{CONTENT_TYPE, TE};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor, SerializeOptions};
use serde_json::Value;
use url::Url;

use crate::{Client, HttpError, Request, Response, Timings};

mod reflect;
mod stream;

pub use stream::{GrpcEnd, GrpcEvent, GrpcReceiver, GrpcSender, GrpcStream};

pub const OK: u32 = 0;
pub const DEADLINE_EXCEEDED: u32 = 4;
pub const RESOURCE_EXHAUSTED: u32 = 8;
pub const INTERNAL: u32 = 13;
pub const UNAVAILABLE: u32 = 14;

/// Mistakes found before anything is sent.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GrpcError {
    #[error("loading protos: {0}")]
    Load(String),
    #[error("unknown gRPC method {name:?} (known: {known})")]
    UnknownMethod { name: String, known: String },
    #[error("{method} is a streaming method: open a stream to call it")]
    Streaming { method: String },
    #[error("{method}: invalid request message: {reason}")]
    Message { method: String, reason: String },
    #[error("invalid gRPC target {target:?}: {reason}")]
    Target { target: String, reason: String },
    #[error("server reflection: {0}")]
    Reflection(String),
    #[error("the stream is closed for sending")]
    Closed,
    #[error("trying to send message larger than max ({size} vs. {max})")]
    TooLarge { size: usize, max: usize },
}

/// How calls send and receive messages: gzip, and k6's `maxSendSize` /
/// `maxReceiveSize`. Answers compressed with gzip are read either way.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrpcOptions {
    /// Compress sent messages with gzip (`grpc-encoding: gzip`).
    pub gzip: bool,
    /// The largest message to send, in bytes before compression; `None`: any.
    pub max_send_size: Option<usize>,
    /// The largest message to accept, in bytes (compressed and not);
    /// `None`: any. A larger one fails the call with RESOURCE_EXHAUSTED.
    pub max_receive_size: Option<usize>,
}

impl GrpcOptions {
    /// `payload` length-prefixed for sending: checked against
    /// `max_send_size`, and gzipped if asked.
    fn frame(&self, payload: &[u8]) -> Result<Bytes, GrpcError> {
        if let Some(max) = self.max_send_size
            && payload.len() > max
        {
            return Err(GrpcError::TooLarge {
                size: payload.len(),
                max,
            });
        }
        let (flag, data) = if self.gzip {
            let gzipped =
                crate::compression::compress(payload, &[crate::compression::Encoding::Gzip])
                    .expect("gzip into memory can't fail");
            (1, gzipped)
        } else {
            (0, Bytes::copy_from_slice(payload))
        };
        let mut buf = BytesMut::with_capacity(5 + data.len());
        buf.put_u8(flag);
        buf.put_u32(data.len() as u32);
        buf.put_slice(&data);
        Ok(buf.freeze())
    }

    /// A framed, uncompressed message (as [`GrpcMethod::encode`] makes it)
    /// framed for sending with these options.
    fn reframe(&self, framed: &Bytes) -> Result<Bytes, GrpcError> {
        if !self.gzip && self.max_send_size.is_none() {
            return Ok(framed.clone());
        }
        let payload = framed.get(5..).unwrap_or_default();
        self.frame(payload)
    }
}

/// Services and messages from `.proto` files. Cheap to clone.
#[derive(Debug, Clone)]
pub struct Protos {
    pool: DescriptorPool,
}

impl Protos {
    /// Compiles `files` (and what they import) found in `import_paths`; with
    /// no import paths, each file's own directory.
    pub fn load(
        files: &[impl AsRef<Path>],
        import_paths: &[impl AsRef<Path>],
    ) -> Result<Protos, GrpcError> {
        let mut includes: Vec<PathBuf> =
            import_paths.iter().map(|p| p.as_ref().to_owned()).collect();
        if includes.is_empty() {
            for file in files {
                let dir = match file.as_ref().parent() {
                    Some(dir) if !dir.as_os_str().is_empty() => dir.to_owned(),
                    _ => PathBuf::from("."),
                };
                if !includes.contains(&dir) {
                    includes.push(dir);
                }
            }
        }
        let load = |e: protox::Error| GrpcError::Load(e.to_string());
        let mut compiler = protox::Compiler::new(includes).map_err(load)?;
        compiler.include_imports(true);
        compiler.open_files(files).map_err(load)?;
        Ok(Protos {
            pool: compiler.descriptor_pool(),
        })
    }

    /// The compiled files, as a `FileDescriptorSet`: what [`Protos::decode`]
    /// takes (e.g. on a machine without the `.proto` files).
    pub fn encode(&self) -> Vec<u8> {
        self.pool.encode_to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Result<Protos, GrpcError> {
        DescriptorPool::decode(bytes)
            .map(|pool| Protos { pool })
            .map_err(|e| GrpcError::Load(e.to_string()))
    }

    /// A method by name: `package.Service/Method` (or with a leading `/`).
    pub fn method(&self, name: &str) -> Result<GrpcMethod, GrpcError> {
        let found = name
            .trim_start_matches('/')
            .split_once('/')
            .and_then(|(service, method)| {
                self.pool
                    .get_service_by_name(service)?
                    .methods()
                    .find(|m| m.name() == method)
            });
        match found {
            Some(desc) => Ok(GrpcMethod::new(desc)),
            None => Err(GrpcError::UnknownMethod {
                name: name.to_owned(),
                known: self.methods().join(", "),
            }),
        }
    }

    /// Every method, as `package.Service/Method`.
    pub fn methods(&self) -> Vec<String> {
        let mut names = Vec::new();
        for service in self.pool.services() {
            for method in service.methods() {
                names.push(format!("{}/{}", service.full_name(), method.name()));
            }
        }
        names
    }
}

/// One method of a service.
#[derive(Debug, Clone)]
pub struct GrpcMethod {
    desc: MethodDescriptor,
    path: String,
}

impl GrpcMethod {
    fn new(desc: MethodDescriptor) -> GrpcMethod {
        let path = format!("/{}/{}", desc.parent_service().full_name(), desc.name());
        GrpcMethod { desc, path }
    }

    /// `/package.Service/Method`: the request path (and k6's `name` tag).
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn is_client_streaming(&self) -> bool {
        self.desc.is_client_streaming()
    }

    pub fn is_server_streaming(&self) -> bool {
        self.desc.is_server_streaming()
    }

    /// `message` (JSON, with proto or JSON field names) encoded and framed.
    pub fn encode(&self, message: &Value) -> Result<Bytes, GrpcError> {
        let msg = DynamicMessage::deserialize(self.desc.input(), message).map_err(|e| {
            GrpcError::Message {
                method: self.path.clone(),
                reason: e.to_string(),
            }
        })?;
        let mut buf = BytesMut::new();
        frame(&msg, &mut buf);
        Ok(buf.freeze())
    }

    /// `message` (JSON) encoded, without the length prefix.
    fn payload(&self, message: &Value) -> Result<Bytes, GrpcError> {
        let msg = DynamicMessage::deserialize(self.desc.input(), message).map_err(|e| {
            GrpcError::Message {
                method: self.path.clone(),
                reason: e.to_string(),
            }
        })?;
        Ok(Bytes::from(msg.encode_to_vec()))
    }

    /// A response message (unframed) as JSON, default values included.
    pub fn decode(&self, payload: &[u8]) -> Result<Value, String> {
        let msg = DynamicMessage::decode(self.desc.output(), payload).map_err(|e| e.to_string())?;
        let options = SerializeOptions::new().skip_default_fields(false);
        msg.serialize_with_options(serde_json::value::Serializer, &options)
            .map_err(|e| e.to_string())
    }
}

/// A target's base URL: `host:port` (`https`, or `http` if `plaintext`), or
/// an `http://` / `https://` URL.
pub fn target(address: &str, plaintext: bool) -> Result<Url, GrpcError> {
    let invalid = |reason: &dyn std::fmt::Display| GrpcError::Target {
        target: address.to_owned(),
        reason: reason.to_string(),
    };
    let url = if address.contains("://") {
        Url::parse(address)
    } else {
        let scheme = if plaintext { "http" } else { "https" };
        Url::parse(&format!("{scheme}://{address}"))
    }
    .map_err(|e| invalid(&e))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(invalid(&"use host:port, or an http or https URL"));
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(invalid(&"missing host"));
    }
    if url.path() != "/" || url.query().is_some() {
        return Err(invalid(&"a target has no path"));
    }
    Ok(url)
}

/// A unary call, ready to send.
#[derive(Debug, Clone)]
pub struct GrpcRequest {
    /// The target plus the method's path.
    pub url: Url,
    pub method: GrpcMethod,
    /// The framed message.
    pub body: Bytes,
    /// Custom metadata, sent as headers (`-bin` keys take base64 values).
    pub metadata: HeaderMap,
    pub options: GrpcOptions,
}

impl GrpcRequest {
    pub fn new(
        target: &Url,
        method: &GrpcMethod,
        message: &Value,
    ) -> Result<GrpcRequest, GrpcError> {
        if method.is_client_streaming() || method.is_server_streaming() {
            return Err(GrpcError::Streaming {
                method: method.path.clone(),
            });
        }
        Ok(GrpcRequest {
            url: target
                .join(&method.path)
                .expect("a path joins any base URL"),
            method: method.clone(),
            body: method.encode(message)?,
            metadata: HeaderMap::new(),
            options: GrpcOptions::default(),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct GrpcResponse {
    /// The gRPC status code (0 is OK; see [`status_name`]).
    pub status: u32,
    /// `grpc-message`, or why the call failed on k9's side.
    pub status_message: String,
    /// The response message as JSON; `None` unless the status is OK.
    pub message: Option<Value>,
    pub headers: HeaderMap,
    pub trailers: HeaderMap,
    pub timings: Timings,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

impl GrpcResponse {
    /// The status a call that got no answer ends with, as gRPC clients report
    /// it: DEADLINE_EXCEEDED after a timeout, UNAVAILABLE otherwise.
    pub fn from_error(err: &HttpError) -> GrpcResponse {
        let status = match err {
            HttpError::Timeout(_) => DEADLINE_EXCEEDED,
            _ => UNAVAILABLE,
        };
        GrpcResponse {
            status,
            status_message: err.to_string(),
            ..GrpcResponse::default()
        }
    }

    fn from_http(method: &GrpcMethod, res: Response, options: &GrpcOptions) -> GrpcResponse {
        let (mut status, mut status_message) = status_of(&res);
        let mut message = None;
        if status == OK {
            let encoding = res
                .headers
                .get("grpc-encoding")
                .and_then(|v| v.to_str().ok());
            let decoded = unframe(&res.body, encoding, options.max_receive_size).and_then(|p| {
                method
                    .decode(&p)
                    .map_err(|reason| (INTERNAL, format!("invalid response message: {reason}")))
            });
            match decoded {
                Ok(value) => message = Some(value),
                Err((code, reason)) => (status, status_message) = (code, reason),
            }
        }
        GrpcResponse {
            status,
            status_message,
            message,
            headers: res.headers,
            trailers: res.trailers,
            timings: res.timings,
            bytes_sent: res.bytes_sent,
            bytes_received: res.bytes_received,
        }
    }
}

impl Client {
    /// Makes a unary call. `timeout` (default: the client's) is sent as
    /// `grpc-timeout` and enforced here too. Errs only if no response came
    /// ([`GrpcResponse::from_error`] gives the status gRPC reports for it);
    /// a call that failed on the server has its status in the response.
    pub async fn grpc_invoke(
        &self,
        request: &GrpcRequest,
        timeout: Option<Duration>,
    ) -> Result<GrpcResponse, HttpError> {
        let options = &request.options;
        let body = match options.reframe(&request.body) {
            Ok(body) => body,
            Err(e) => {
                return Ok(GrpcResponse {
                    status: RESOURCE_EXHAUSTED,
                    status_message: e.to_string(),
                    ..GrpcResponse::default()
                });
            }
        };
        let res = self
            .grpc_send(&request.url, &request.metadata, body, timeout, options.gzip)
            .await?;
        Ok(GrpcResponse::from_http(&request.method, res, options))
    }

    /// The request headers of a call to `url`.
    fn grpc_request(
        &self,
        url: &Url,
        metadata: &HeaderMap,
        body: Bytes,
        timeout: Duration,
        gzip: bool,
    ) -> Request {
        let mut headers = metadata.clone();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));
        headers.insert(TE, HeaderValue::from_static("trailers"));
        headers.insert("grpc-timeout", grpc_timeout(timeout));
        headers.insert("grpc-accept-encoding", HeaderValue::from_static("gzip"));
        if gzip {
            headers.insert("grpc-encoding", HeaderValue::from_static("gzip"));
        }
        Request {
            method: Method::POST,
            url: url.clone(),
            headers,
            body,
            keep_body: true,
        }
    }

    /// POSTs framed messages to a method's URL, half-closes, and reads the
    /// whole response (every message, then the trailers).
    async fn grpc_send(
        &self,
        url: &Url,
        metadata: &HeaderMap,
        body: Bytes,
        timeout: Option<Duration>,
        gzip: bool,
    ) -> Result<Response, HttpError> {
        let timeout = timeout.unwrap_or(self.shared.config.timeout);
        let http = self.grpc_request(url, metadata, body, timeout, gzip);
        self.send_with_timeout(&http, timeout).await
    }
}

/// Appends `msg` as a length-prefixed, uncompressed message.
fn frame(msg: &impl Message, buf: &mut BytesMut) {
    let len = msg.encoded_len();
    buf.reserve(5 + len);
    buf.put_u8(0);
    buf.put_u32(len as u32);
    msg.encode(buf).expect("the buffer has room");
}

/// The status and its message: from the trailers, or the headers of a
/// trailers-only answer, or gRPC's mapping of the HTTP status.
fn status_of(res: &Response) -> (u32, String) {
    status_from(res.status, &res.headers, &res.trailers)
}

fn status_from(http: StatusCode, headers: &HeaderMap, trailers: &HeaderMap) -> (u32, String) {
    let field = |name: &str| trailers.get(name).or_else(|| headers.get(name));
    let code = field("grpc-status").and_then(|v| v.to_str().ok()?.parse::<u32>().ok());
    match code {
        Some(code) => (
            code,
            field("grpc-message")
                .map(|v| percent_decode(v.as_bytes()))
                .unwrap_or_default(),
        ),
        None if http != StatusCode::OK => (from_http_status(http), format!("HTTP status {http}")),
        None => (INTERNAL, "the response has no grpc-status".to_owned()),
    }
}

/// The name of a status code, e.g. `NOT_FOUND` for 5.
pub fn status_name(code: u32) -> &'static str {
    const NAMES: [&str; 17] = [
        "OK",
        "CANCELLED",
        "UNKNOWN",
        "INVALID_ARGUMENT",
        "DEADLINE_EXCEEDED",
        "NOT_FOUND",
        "ALREADY_EXISTS",
        "PERMISSION_DENIED",
        "RESOURCE_EXHAUSTED",
        "FAILED_PRECONDITION",
        "ABORTED",
        "OUT_OF_RANGE",
        "UNIMPLEMENTED",
        "INTERNAL",
        "UNAVAILABLE",
        "DATA_LOSS",
        "UNAUTHENTICATED",
    ];
    NAMES.get(code as usize).copied().unwrap_or("UNKNOWN")
}

/// gRPC's mapping for a response that isn't gRPC at all.
fn from_http_status(status: StatusCode) -> u32 {
    match status.as_u16() {
        400 => INTERNAL,
        401 => 16,
        403 => 7,
        404 => 12,
        429 | 502 | 503 | 504 => UNAVAILABLE,
        _ => 2,
    }
}

/// Why a received message can't be used: a status and its message.
type Unreadable = (u32, String);

/// The single message of a unary response (see [`frames`]).
fn unframe(body: &Bytes, encoding: Option<&str>, max: Option<usize>) -> Result<Bytes, Unreadable> {
    frames(body, encoding, max)?
        .into_iter()
        .next()
        .ok_or_else(|| {
            (
                INTERNAL,
                "invalid response message: the response has no message".to_owned(),
            )
        })
}

/// Every length-prefixed message in `body`, decompressed (`encoding` is the
/// answer's `grpc-encoding`) and checked against `max` bytes.
fn frames(
    body: &Bytes,
    encoding: Option<&str>,
    max: Option<usize>,
) -> Result<Vec<Bytes>, Unreadable> {
    let invalid = |reason: String| (INTERNAL, format!("invalid response message: {reason}"));
    let mut messages = Vec::new();
    let mut at = 0;
    while at < body.len() {
        let Some(&[compressed, a, b, c, d]) = body[at..].first_chunk::<5>() else {
            return Err(invalid(format!(
                "truncated message prefix ({} bytes)",
                body.len() - at
            )));
        };
        let len = u32::from_be_bytes([a, b, c, d]) as usize;
        let rest = body.len() - at - 5;
        if rest < len {
            return Err(invalid(format!(
                "truncated message ({rest} of {len} bytes)"
            )));
        }
        let data = body.slice(at + 5..at + 5 + len);
        messages.push(payload(compressed, data, encoding, max)?);
        at += 5 + len;
    }
    Ok(messages)
}

/// A received message's bytes: decompressed if its flag says so, and no
/// larger than `max`.
fn payload(
    compressed: u8,
    data: Bytes,
    encoding: Option<&str>,
    max: Option<usize>,
) -> Result<Bytes, Unreadable> {
    let too_large = |size: usize, max: usize| {
        (
            RESOURCE_EXHAUSTED,
            format!("received message larger than max ({size} vs. {max})"),
        )
    };
    if let Some(max) = max
        && data.len() > max
    {
        return Err(too_large(data.len(), max));
    }
    let data = match (compressed, encoding) {
        (0, _) => data,
        (_, Some("gzip")) => crate::compression::decompress(data, "gzip")
            .map_err(|e| (INTERNAL, format!("invalid response message: {e}")))?,
        (_, other) => {
            return Err((
                INTERNAL,
                format!(
                    "invalid response message: compressed with {} (k9 reads gzip)",
                    other.unwrap_or("no grpc-encoding")
                ),
            ));
        }
    };
    match max {
        Some(max) if data.len() > max => Err(too_large(data.len(), max)),
        _ => Ok(data),
    }
}

/// `grpc-timeout`: at most 8 digits, in milliseconds while they fit.
fn grpc_timeout(timeout: Duration) -> HeaderValue {
    let ms = timeout.as_millis();
    let value = if ms <= 99_999_999 {
        format!("{ms}m")
    } else {
        format!("{}S", timeout.as_secs().min(99_999_999))
    };
    HeaderValue::from_str(&value).expect("digits and a unit")
}

/// `grpc-message` is percent-encoded UTF-8.
fn percent_decode(raw: &[u8]) -> String {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        let hex = raw
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (raw[i], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_message_is_percent_decoded() {
        assert_eq!(percent_decode(b"zo%C3%AB 100%"), "zoë 100%");
        assert_eq!(percent_decode(b"%zz"), "%zz");
    }

    #[test]
    fn timeouts_fit_in_eight_digits() {
        assert_eq!(grpc_timeout(Duration::from_millis(1500)), "1500m");
        assert_eq!(grpc_timeout(Duration::from_secs(200_000)), "200000S");
    }

    #[test]
    fn frames_are_checked() {
        let b = |bytes: &[u8]| Bytes::copy_from_slice(bytes);
        let err = |r: Result<Bytes, Unreadable>| r.unwrap_err().1;
        assert_eq!(
            unframe(&b(&[0, 0, 0, 0, 2, 7, 8]), None, None),
            Ok(b(&[7, 8]))
        );
        assert!(err(unframe(&b(&[0, 0, 0, 0, 3, 7]), None, None)).contains("truncated"));
        assert!(err(unframe(&b(&[1, 0, 0, 0, 0]), None, None)).contains("compressed"));
        assert!(err(unframe(&b(&[]), None, None)).contains("no message"));
        assert_eq!(
            frames(&b(&[0, 0, 0, 0, 1, 7, 0, 0, 0, 0, 0]), None, None),
            Ok(vec![b(&[7]), b(&[])])
        );
        assert!(
            frames(&b(&[0, 0]), None, None)
                .unwrap_err()
                .1
                .contains("prefix")
        );

        let options = GrpcOptions {
            gzip: true,
            ..GrpcOptions::default()
        };
        let framed = options.frame(&[7; 100]).unwrap();
        assert_eq!(framed[0], 1, "flagged as compressed");
        assert!(framed.len() < 50);
        assert_eq!(unframe(&framed, Some("gzip"), Some(100)), Ok(b(&[7; 100])));
        let (status, message) = unframe(&framed, Some("gzip"), Some(99)).unwrap_err();
        assert_eq!(
            (status, message.as_str()),
            (
                RESOURCE_EXHAUSTED,
                "received message larger than max (100 vs. 99)"
            )
        );
        assert!(err(unframe(&framed, Some("snappy"), None)).contains("snappy"));
        let limited = GrpcOptions {
            max_send_size: Some(3),
            ..GrpcOptions::default()
        };
        assert!(matches!(
            limited.frame(&[1, 2, 3, 4]),
            Err(GrpcError::TooLarge { size: 4, max: 3 })
        ));
        assert_eq!(
            limited.frame(&[1, 2, 3]).unwrap(),
            b(&[0, 0, 0, 0, 3, 1, 2, 3])
        );
    }
}
