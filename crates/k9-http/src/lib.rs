//! K9's HTTP client, built on hyper with a timed connector so every request
//! reports k6's per-phase timings: blocked (DNS + waiting for a connection),
//! connecting, TLS handshaking, sending, waiting and receiving.
//!
//! Connections are pooled per [`Client`]. Give each VU its own client for k6's
//! default per-VU connections (realistic), or clone one client to share a pool
//! (faster, but results look better than reality).
//!
//! HTTPS negotiates HTTP/2 or HTTP/1.1 with ALPN; plain HTTP is HTTP/1.1
//! unless HTTP/2 is forced, and HTTP/3 (QUIC) is used only when asked for
//! ([`HttpVersion`]). An HTTP/2 or HTTP/3 connection carries every concurrent
//! request of the clients sharing its pool, one stream each.

mod body;
pub mod compression;
mod connect;
mod debug;
pub mod grpc;
mod io;
pub mod multipart;
pub mod net;
mod quic;
mod tls;
mod ws;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::tls::Connectors;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::header::{
    ACCEPT_ENCODING, AUTHORIZATION, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
    COOKIE, HOST, LOCATION, SET_COOKIE, TRANSFER_ENCODING, USER_AGENT,
};
use http::{HeaderMap, HeaderValue, Method, StatusCode, Version};
use http_body_util::BodyExt;
use url::{Host, Position, Url};

use crate::body::TimedBody;
use crate::connect::{Connection, PoolKey, Sender, connect};
use crate::net::Resolver;
use crate::quic::{H3, Quic};

pub use debug::HttpDebug;
pub use net::{DnsConfig, DnsPolicy, DnsSelect, HostOverride, HostPattern, IpPool, IpRange};
pub use rustls::pki_types::CertificateDer;
pub use tls::{ClientCert, TlsInfo, TlsVersion};
pub use ws::{WebSocket, WsEvent, WsMessage, WsRequest};

/// k6's default `maxRedirects`.
pub const DEFAULT_MAX_REDIRECTS: u32 = 10;

/// k6's default request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// The HTTP versions a client may speak.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HttpVersion {
    /// HTTP/2 when the server accepts it during the TLS handshake (ALPN),
    /// otherwise HTTP/1.1; plain HTTP stays HTTP/1.1. What browsers and k6 do.
    #[default]
    Auto,
    /// HTTP/1.1 only.
    Http1,
    /// HTTP/2 only: over TLS the server must accept it; plain HTTP uses prior
    /// knowledge (h2c) and the server must expect it.
    Http2,
    /// HTTP/3 over QUIC (UDP, the URL's port), HTTPS only. Experimental: the
    /// `h3` crate is pre-1.0.
    Http3,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Limit for the whole request, from start to last body byte.
    pub timeout: Duration,
    /// Certificates trusted in addition to the bundled Mozilla roots.
    pub extra_root_certs: Vec<CertificateDer<'static>>,
    /// Read response bodies (so timing and byte counts stay correct) but don't keep them.
    pub discard_body: bool,
    /// Sent unless the request sets its own `User-Agent`.
    pub user_agent: Option<String>,
    /// Send k6's `Accept-Encoding: gzip, deflate` unless the request sets
    /// its own. Compressed responses are decoded either way.
    pub accept_encoding: bool,
    /// Redirects to follow before returning the redirect response itself
    /// (k6's `maxRedirects`, default 10; 0 disables following).
    pub max_redirects: u32,
    /// Accept any server certificate (k6's `insecureSkipTLSVerify`).
    pub insecure_skip_tls_verify: bool,
    pub http_version: HttpVersion,
    /// Client certificates by domain (k6's `tlsAuth`).
    pub client_certs: Vec<ClientCert>,
    /// The lowest and highest TLS versions to speak (k6's `tlsVersion`);
    /// default: TLS 1.2 to 1.3.
    pub tls_versions: Option<(TlsVersion, TlsVersion)>,
    /// IANA names of the cipher suites to offer (k6's `tlsCipherSuites`);
    /// empty: rustls's defaults.
    pub cipher_suites: Vec<String>,
    /// Names sent to other addresses (k6's `hosts`).
    pub hosts: Vec<HostOverride>,
    /// Caching, IP versions and address choice for DNS lookups (k6's `dns`).
    pub dns: DnsConfig,
    /// Hostnames requests may not go to (k6's `blockHostnames`).
    pub block_hostnames: Vec<HostPattern>,
    /// Addresses requests may not go to (k6's `blacklistIPs`).
    pub blocked_ips: Vec<IpRange>,
    /// A new connection for every request (k6's `noConnectionReuse`).
    pub no_connection_reuse: bool,
    /// Dump requests and responses to stderr (k6's `httpDebug`).
    pub debug: Option<HttpDebug>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            timeout: DEFAULT_TIMEOUT,
            extra_root_certs: Vec::new(),
            discard_body: false,
            user_agent: Some(format!("k9/{}", env!("CARGO_PKG_VERSION"))),
            accept_encoding: true,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            client_certs: Vec::new(),
            tls_versions: None,
            cipher_suites: Vec::new(),
            hosts: Vec::new(),
            dns: DnsConfig::default(),
            block_hostnames: Vec::new(),
            blocked_ips: Vec::new(),
            no_connection_reuse: false,
            debug: None,
            insecure_skip_tls_verify: false,
            http_version: HttpVersion::Auto,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Request {
    pub method: Method,
    pub url: Url,
    pub headers: HeaderMap,
    pub body: Bytes,
    /// Keeps the response body even if the client discards bodies.
    pub keep_body: bool,
}

impl Request {
    pub fn new(method: Method, url: &str) -> Result<Request, HttpError> {
        let invalid = |reason: String| HttpError::InvalidUrl {
            url: url.to_owned(),
            reason,
        };
        let parsed = Url::parse(url).map_err(|e| invalid(e.to_string()))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(invalid(format!(
                "unsupported scheme `{}` (use http or https)",
                parsed.scheme()
            )));
        }
        if parsed.host_str().is_none_or(str::is_empty) {
            return Err(invalid("missing host".to_owned()));
        }
        Ok(Request {
            method,
            url: parsed,
            headers: HeaderMap::new(),
            body: Bytes::new(),
            keep_body: false,
        })
    }

    pub fn get(url: &str) -> Result<Request, HttpError> {
        Request::new(Method::GET, url)
    }
}

/// k6's HTTP timing phases for one request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Timings {
    /// Waiting for a connection: DNS lookup plus pool wait (k6 `http_req_blocked`).
    pub blocked: Duration,
    /// TCP connect (`http_req_connecting`). Zero on a reused connection.
    pub connecting: Duration,
    /// TLS handshake (`http_req_tls_handshaking`). Zero on a reused connection.
    pub tls_handshaking: Duration,
    /// Writing the request (`http_req_sending`). On HTTP/2, until hyper took
    /// the body's last chunk (the stream's share of the socket isn't visible).
    pub sending: Duration,
    /// From request sent to first response byte (`http_req_waiting`, TTFB).
    /// On HTTP/2, to the stream's response headers.
    pub waiting: Duration,
    /// Reading the response (`http_req_receiving`).
    pub receiving: Duration,
}

impl Timings {
    /// k6's `http_req_duration`: sending + waiting + receiving.
    pub fn duration(&self) -> Duration {
        self.sending + self.waiting + self.receiving
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    pub status: StatusCode,
    /// The protocol the response came in (k6's `proto`).
    pub version: Version,
    pub headers: HeaderMap,
    /// Raw body; empty when the client discards bodies.
    pub body: Bytes,
    /// HTTP/2 trailers (gRPC's status), empty without any.
    pub trailers: HeaderMap,
    /// The connection's TLS details; `None` over plain HTTP (and HTTP/3).
    pub tls: Option<TlsInfo>,
    pub timings: Timings,
    /// Bytes written to the socket for this request, including headers. On
    /// HTTP/2, the connection's bytes since the previous stream finished: exact
    /// in total, approximate per request when streams overlap.
    pub bytes_sent: u64,
    /// Bytes read from the socket for this response, including headers (same
    /// HTTP/2 caveat).
    pub bytes_received: u64,
    pub remote_addr: SocketAddr,
    /// The URL that produced this response (after any redirects).
    pub url: Url,
    /// Redirect responses that were followed to get here, in order.
    pub redirects: Vec<Hop>,
}

/// One followed redirect: the request that got a 3xx, with its own timings.
#[derive(Debug, Clone)]
pub struct Hop {
    pub url: Url,
    pub method: Method,
    pub status: StatusCode,
    pub version: Version,
    pub timings: Timings,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub tls: Option<TlsInfo>,
}

/// k6's name for an HTTP version, as in its `proto` tag.
pub fn proto(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "HTTP/0.9",
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_2 => "HTTP/2.0",
        Version::HTTP_3 => "HTTP/3.0",
        _ => "HTTP/1.1",
    }
}

impl Response {
    /// The body as text, decoded lossily as UTF-8.
    pub fn text(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    pub fn json(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_slice(&self.body)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("invalid URL {url:?}: {reason}")]
    InvalidUrl { url: String, reason: String },
    #[error("DNS lookup for {host} failed: {reason}")]
    Dns { host: String, reason: String },
    #[error("could not connect to {addr}: {source}")]
    Connect {
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[error("TLS handshake with {host} failed: {source}")]
    Tls {
        host: String,
        source: std::io::Error,
    },
    #[error("{host} doesn't speak HTTP/2 (it didn't accept h2 in the TLS handshake)")]
    NoHttp2 { host: String },
    #[error("WebSocket {0}")]
    WebSocket(String),
    #[error("HTTP/3: {0}")]
    Http3(String),
    #[error("request timed out after {0:?}")]
    Timeout(Duration),
    #[error("HTTP error: {0}")]
    Protocol(#[from] hyper::Error),
    #[error("invalid client configuration: {0}")]
    Config(String),
    #[error("{0}")]
    Decompress(String),
    #[error("request to {host} blocked: {reason}")]
    Blocked { host: String, reason: String },
}

impl HttpError {
    /// True when a reused keep-alive connection turned out to be dead before
    /// the server answered, so the request can be retried on a new connection.
    /// Depending on timing, that shows up as a clean close or as a reset.
    fn is_stale_connection(&self) -> bool {
        let HttpError::Protocol(e) = self else {
            return false;
        };
        if e.is_canceled() || e.is_incomplete_message() {
            return true;
        }
        let io_kind = std::error::Error::source(e)
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .map(std::io::Error::kind);
        matches!(
            io_kind,
            Some(
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
            )
        )
    }
}

/// An HTTP client with its own connection pool. Cloning shares the pool;
/// [`Client::with_separate_pool`] shares everything except the pool.
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
    pool: Arc<Mutex<Pool>>,
    /// Cookie jar; like the pool, one per [`Client::with_separate_pool`].
    cookies: Arc<Mutex<cookie_store::CookieStore>>,
    /// The address connections are made from (k6's `localIPs`).
    local_ip: Option<IpAddr>,
}

struct Shared {
    config: ClientConfig,
    user_agent: Option<HeaderValue>,
    resolver: Arc<Resolver>,
    tls: Connectors,
    /// For WebSockets: HTTP/1.1 only in ALPN (the upgrade is HTTP/1.1's).
    ws_tls: Connectors,
    /// For gRPC: h2 only in ALPN.
    h2_tls: Connectors,
    /// Set up only for [`HttpVersion::Http3`].
    quic: Option<Quic>,
}

type Pool = HashMap<PoolKey, Pooled>;

/// A host's connections: idle HTTP/1.1 ones, and the HTTP/2 one every request
/// shares.
#[derive(Default)]
struct Pooled {
    idle: Vec<Connection>,
    shared: Option<Connection>,
}

impl Client {
    pub fn new(config: ClientConfig) -> Result<Client, HttpError> {
        let user_agent = config
            .user_agent
            .as_deref()
            .map(HeaderValue::from_str)
            .transpose()
            .map_err(|e| HttpError::Config(format!("user agent: {e}")))?;
        let resolver = Arc::new(Resolver::new(&config)?);
        let tls = Connectors::new(&config)?;
        let ws_tls = Connectors::new(&ClientConfig {
            http_version: HttpVersion::Http1,
            ..config.clone()
        })?;
        let h2_tls = Connectors::new(&ClientConfig {
            http_version: HttpVersion::Http2,
            ..config.clone()
        })?;
        let quic = match config.http_version {
            HttpVersion::Http3 => {
                let everywhere = config.client_certs.iter().find(|c| c.domains.is_empty());
                Some(Quic::new(tls::tls_config(&config, everywhere)?)?)
            }
            _ => None,
        };
        Ok(Client {
            shared: Arc::new(Shared {
                config,
                user_agent,
                resolver,
                tls,
                ws_tls,
                h2_tls,
                quic,
            }),
            pool: Arc::default(),
            cookies: Arc::default(),
            local_ip: None,
        })
    }

    /// A client with the same configuration, resolver and TLS setup but its own
    /// connection pool: what each VU gets by default.
    pub fn with_separate_pool(&self) -> Client {
        Client {
            shared: self.shared.clone(),
            pool: Arc::default(),
            cookies: Arc::default(),
            local_ip: self.local_ip,
        }
    }

    /// This client, connecting from `ip` (one of k6's `localIPs`). Not for
    /// HTTP/3.
    pub fn with_local_ip(self, ip: IpAddr) -> Client {
        Client {
            local_ip: Some(ip),
            ..self
        }
    }

    /// Closes this client's pooled connections: requests already using one
    /// finish on it (k6's `noVUConnectionReuse`, between iterations).
    pub fn close_connections(&self) {
        self.lock_pool().clear();
    }

    /// A client for gRPC: the same configuration, resolver and TLS roots, but
    /// HTTP/2 only (h2c on plain HTTP), with its own connection pool.
    pub fn grpc(&self) -> Client {
        let s = &self.shared;
        Client {
            shared: Arc::new(Shared {
                config: ClientConfig {
                    http_version: HttpVersion::Http2,
                    max_redirects: 0,
                    accept_encoding: false,
                    ..s.config.clone()
                },
                user_agent: s.user_agent.clone(),
                resolver: s.resolver.clone(),
                tls: s.h2_tls.clone(),
                ws_tls: s.ws_tls.clone(),
                h2_tls: s.h2_tls.clone(),
                quic: None,
            }),
            pool: Arc::default(),
            cookies: Arc::default(),
            local_ip: self.local_ip,
        }
    }

    pub async fn send(&self, request: &Request) -> Result<Response, HttpError> {
        self.send_with_timeout(request, self.shared.config.timeout)
            .await
    }

    /// Like [`Client::send`], with this request's own time limit instead of
    /// the configured one.
    pub async fn send_with_timeout(
        &self,
        request: &Request,
        timeout: Duration,
    ) -> Result<Response, HttpError> {
        tokio::time::timeout(timeout, self.send_following(request))
            .await
            .map_err(|_| HttpError::Timeout(timeout))?
    }

    /// Forgets all cookies (k6 resets each VU's jar at the start of every iteration).
    pub fn clear_cookies(&self) {
        self.lock_cookies().clear();
    }

    /// Sends `request`, following up to `max_redirects` redirects.
    async fn send_following(&self, request: &Request) -> Result<Response, HttpError> {
        let mut hops = Vec::new();
        let mut next: Option<Request> = None;
        loop {
            let current = next.as_ref().unwrap_or(request);
            let mut res = self.send_inner(current).await?;
            let follow = if hops.len() < self.shared.config.max_redirects as usize {
                redirect_target(current, &res)
            } else {
                None
            };
            let Some(target) = follow else {
                res.redirects = hops;
                return Ok(res);
            };
            hops.push(Hop {
                url: current.url.clone(),
                method: current.method.clone(),
                status: res.status,
                version: res.version,
                timings: res.timings,
                bytes_sent: res.bytes_sent,
                bytes_received: res.bytes_received,
                tls: res.tls,
            });
            next = Some(target);
        }
    }

    /// Idle pooled connections, across all hosts.
    pub fn idle_connections(&self) -> usize {
        self.lock_pool()
            .values()
            .map(|p| p.idle.len() + usize::from(p.shared.is_some()))
            .sum()
    }

    async fn send_inner(&self, request: &Request) -> Result<Response, HttpError> {
        let key = pool_key(&request.url);
        let started = Instant::now();
        let reuse = !self.shared.config.no_connection_reuse;

        if reuse && let Some(conn) = self.checkout(&key).await {
            let timings = Timings {
                blocked: started.elapsed(),
                ..Timings::default()
            };
            match self.exchange(conn, request, &key, timings).await {
                Err(e) if e.is_stale_connection() && is_idempotent(&request.method) => {}
                result => return result,
            }
        }

        let (conn, ct) = match &self.shared.quic {
            Some(quic) => quic::connect(&key, &self.shared.resolver, quic).await?,
            None => {
                connect(
                    &key,
                    &self.shared.resolver,
                    self.local_ip,
                    &self.shared.tls,
                    self.shared.config.http_version,
                )
                .await?
            }
        };
        if reuse && let Some(shared) = conn.share() {
            // Concurrent requests use it from now on; the newest one wins.
            self.lock_pool().entry(key.clone()).or_default().shared = Some(shared);
        }
        let timings = Timings {
            // Everything spent acquiring the connection that wasn't connect or TLS:
            // DNS, pool checkout, and any failed attempt on a stale connection.
            blocked: started
                .elapsed()
                .saturating_sub(ct.connecting + ct.tls_handshaking),
            connecting: ct.connecting,
            tls_handshaking: ct.tls_handshaking,
            ..Timings::default()
        };
        self.exchange(conn, request, &key, timings).await
    }

    /// An HTTP/2 connection to `url`'s host: the pooled one, or a new one
    /// (then pooled for later requests).
    pub(crate) async fn h2_connection(&self, url: &Url) -> Result<Connection, HttpError> {
        let key = pool_key(url);
        if let Some(conn) = self.checkout(&key).await
            && matches!(conn.sender, Sender::H2(_))
        {
            return Ok(conn);
        }
        let (conn, _) = connect(
            &key,
            &self.shared.resolver,
            self.local_ip,
            &self.shared.tls,
            HttpVersion::Http2,
        )
        .await?;
        if let Some(shared) = conn.share() {
            self.lock_pool().entry(key).or_default().shared = Some(shared);
        }
        Ok(conn)
    }

    /// Shares the HTTP/2 connection for `key`, or takes an idle HTTP/1.1 one
    /// that is still usable, if any.
    async fn checkout(&self, key: &PoolKey) -> Option<Connection> {
        loop {
            let mut conn = {
                let mut pool = self.lock_pool();
                let pooled = pool.get_mut(key)?;
                if let Some(shared) = &pooled.shared {
                    if !shared.is_closed() {
                        return shared.share();
                    }
                    pooled.shared = None;
                }
                pooled.idle.pop()?
            };
            if let Sender::H1(sender) = &mut conn.sender
                && !sender.is_closed()
                && sender.ready().await.is_ok()
            {
                return Some(conn);
            }
        }
    }

    /// Sends one request on `conn`, reads the whole response, fills in the
    /// sending/waiting/receiving phases, and returns the connection to the pool
    /// if it can be reused.
    async fn exchange(
        &self,
        mut conn: Connection,
        request: &Request,
        key: &PoolKey,
        mut timings: Timings,
    ) -> Result<Response, HttpError> {
        if let Sender::H3(h3) = conn.sender {
            return self
                .exchange_h3(h3, request, conn.remote_addr, timings)
                .await;
        }
        let h2 = matches!(conn.sender, Sender::H2(_));
        let (body, body_sent) = TimedBody::new(request.body.clone(), h2);
        let version = if h2 {
            Version::HTTP_2
        } else {
            Version::HTTP_11
        };
        let req = self.build(request, body, version)?;
        self.debug_request(&req, &request.body);
        if !h2 {
            conn.marks.reset();
        }
        let send_start = Instant::now();
        let res = match &mut conn.sender {
            Sender::H1(sender) => sender.send_request(req).await?,
            Sender::H2(sender) => sender.send_request(req).await?,
            Sender::H3(_) => unreachable!("answered above"),
        };
        let headers_at = Instant::now();
        let (parts, mut body) = res.into_parts();

        let discard = self.shared.config.discard_body && !request.keep_body;
        let mut buf = BytesMut::new();
        let mut trailers = HeaderMap::new();
        while let Some(frame) = body.frame().await {
            match frame?.into_data() {
                Ok(data) if !discard => buf.extend_from_slice(&data),
                Ok(_) => {}
                Err(frame) => {
                    if let Ok(t) = frame.into_trailers() {
                        trailers.extend(t);
                    }
                }
            }
        }
        let done = Instant::now();

        let (bytes_sent, bytes_received) = if h2 {
            let sent = body_sent.get().copied().unwrap_or(send_start);
            timings.sending = sent.saturating_duration_since(send_start);
            timings.waiting = headers_at.saturating_duration_since(sent);
            timings.receiving = done.saturating_duration_since(headers_at);
            conn.marks.take_bytes()
        } else {
            let marks = conn.marks.get();
            let last_write = marks.last_write.unwrap_or(send_start);
            let first_read = marks.first_read.unwrap_or(done);
            timings.sending = last_write.saturating_duration_since(send_start);
            timings.waiting = first_read.saturating_duration_since(last_write);
            timings.receiving = done.saturating_duration_since(first_read);
            (marks.bytes_written, marks.bytes_read)
        };

        let remote_addr = conn.remote_addr;
        let tls = conn.tls;
        self.keep_cookies(&parts.headers, &request.url);
        // An HTTP/2 connection stays in the pool while it's in use.
        let reusable = !h2
            && !self.shared.config.no_connection_reuse
            && !conn.is_closed()
            && !wants_close(&parts.headers)
            && !wants_close(&request.headers);
        if reusable {
            self.lock_pool()
                .entry(key.clone())
                .or_default()
                .idle
                .push(conn);
        }

        let body = decoded(&parts.headers, buf.freeze(), discard)?;
        self.debug_response(parts.version, parts.status, &parts.headers, &body);
        Ok(Response {
            status: parts.status,
            version: parts.version,
            headers: parts.headers,
            body,
            trailers,
            tls,
            timings,
            bytes_sent,
            bytes_received,
            remote_addr,
            url: request.url.clone(),
            redirects: Vec::new(),
        })
    }

    /// One request on an HTTP/3 connection: the stream's phases are explicit
    /// (request sent and finished, response headers, body).
    async fn exchange_h3(
        &self,
        mut h3: H3,
        request: &Request,
        remote_addr: SocketAddr,
        mut timings: Timings,
    ) -> Result<Response, HttpError> {
        let h3_err = |e: h3::error::StreamError| HttpError::Http3(e.to_string());
        let req = self.build(request, (), Version::HTTP_3)?;
        self.debug_request(&req, &request.body);
        let send_start = Instant::now();
        let mut stream = h3.send.send_request(req).await.map_err(h3_err)?;
        if !request.body.is_empty() {
            stream
                .send_data(request.body.clone())
                .await
                .map_err(h3_err)?;
        }
        stream.finish().await.map_err(h3_err)?;
        let sent = Instant::now();
        let res = stream.recv_response().await.map_err(h3_err)?;
        let headers_at = Instant::now();

        let discard = self.shared.config.discard_body && !request.keep_body;
        let mut buf = BytesMut::new();
        while let Some(mut chunk) = stream.recv_data().await.map_err(h3_err)? {
            if discard {
                chunk.advance(chunk.remaining());
            } else {
                buf.put(chunk);
            }
        }
        let trailers = stream
            .recv_trailers()
            .await
            .map_err(h3_err)?
            .unwrap_or_default();
        let done = Instant::now();
        timings.sending = sent.saturating_duration_since(send_start);
        timings.waiting = headers_at.saturating_duration_since(sent);
        timings.receiving = done.saturating_duration_since(headers_at);
        let (bytes_sent, bytes_received) = h3.take_bytes();

        let (parts, ()) = res.into_parts();
        self.keep_cookies(&parts.headers, &request.url);
        let body = decoded(&parts.headers, buf.freeze(), discard)?;
        self.debug_response(Version::HTTP_3, parts.status, &parts.headers, &body);
        Ok(Response {
            status: parts.status,
            version: Version::HTTP_3,
            headers: parts.headers,
            body,
            trailers,
            tls: None,
            timings,
            bytes_sent,
            bytes_received,
            remote_addr,
            url: request.url.clone(),
            redirects: Vec::new(),
        })
    }

    fn debug_request<B>(&self, req: &http::Request<B>, body: &[u8]) {
        if let Some(debug) = self.shared.config.debug {
            eprint!("{}", debug::request_text(req, body, debug));
        }
    }

    fn debug_response(
        &self,
        version: Version,
        status: StatusCode,
        headers: &HeaderMap,
        body: &[u8],
    ) {
        if let Some(debug) = self.shared.config.debug {
            eprint!(
                "{}",
                debug::response_text(version, status, headers, body, debug)
            );
        }
    }

    fn keep_cookies(&self, headers: &HeaderMap, url: &Url) {
        let mut jar = self.lock_cookies();
        for value in headers.get_all(SET_COOKIE) {
            if let Ok(value) = value.to_str() {
                // Invalid or rejected cookies are ignored, as browsers do.
                let _ = jar.parse(value, url);
            }
        }
    }

    fn build<B>(
        &self,
        request: &Request,
        body: B,
        version: Version,
    ) -> Result<http::Request<B>, HttpError> {
        let url = &request.url;
        // HTTP/2 and HTTP/3 take the scheme and authority from the URI
        // (`:authority` replaces `Host`); HTTP/1.1 sends the path and a `Host`.
        let h1 = version == Version::HTTP_11;
        let uri = if h1 {
            &url[Position::BeforePath..Position::AfterQuery]
        } else {
            &url[..Position::AfterQuery]
        };
        let mut req = http::Request::builder()
            .method(request.method.clone())
            .uri(uri)
            .version(version)
            .body(body)
            .map_err(|e| HttpError::InvalidUrl {
                url: url.to_string(),
                reason: e.to_string(),
            })?;
        let headers = req.headers_mut();
        headers.clone_from(&request.headers);
        if h1 && !headers.contains_key(HOST) {
            let host = match url.port() {
                Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
                None => url.host_str().unwrap_or_default().to_owned(),
            };
            let value = HeaderValue::from_str(&host).map_err(|e| HttpError::InvalidUrl {
                url: url.to_string(),
                reason: e.to_string(),
            })?;
            headers.insert(HOST, value);
        }
        if let Some(ua) = &self.shared.user_agent
            && !headers.contains_key(USER_AGENT)
        {
            headers.insert(USER_AGENT, ua.clone());
        }
        if self.shared.config.accept_encoding && !headers.contains_key(ACCEPT_ENCODING) {
            headers.insert(
                ACCEPT_ENCODING,
                HeaderValue::from_static(compression::DEFAULT_ACCEPT_ENCODING),
            );
        }
        let jar_cookies = self
            .lock_cookies()
            .get_request_values(url)
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ");
        if !jar_cookies.is_empty() {
            // Cookies set on the request come first, then the jar's.
            let merged = match headers.get(COOKIE).and_then(|v| v.to_str().ok()) {
                Some(explicit) => format!("{explicit}; {jar_cookies}"),
                None => jar_cookies,
            };
            if let Ok(value) = HeaderValue::from_str(&merged) {
                headers.insert(COOKIE, value);
            }
        }
        Ok(req)
    }

    fn lock_cookies(&self) -> MutexGuard<'_, cookie_store::CookieStore> {
        self.cookies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_pool(&self) -> MutexGuard<'_, Pool> {
        // Never held across an await; a poisoned pool is still structurally valid.
        self.pool
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The request to send for a redirect response, or `None` if `res` isn't one
/// we follow. Follows browser rules: 303 (and 301/302 after a POST) switch to
/// GET without a body; credentials aren't sent to a different host.
fn redirect_target(current: &Request, res: &Response) -> Option<Request> {
    let status = res.status.as_u16();
    if !matches!(status, 301 | 302 | 303 | 307 | 308) {
        return None;
    }
    let location = res.headers.get(LOCATION)?.to_str().ok()?;
    let url = current.url.join(location).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    let mut next = Request {
        method: current.method.clone(),
        url,
        headers: current.headers.clone(),
        body: current.body.clone(),
        keep_body: current.keep_body,
    };
    next.headers.remove(HOST);
    let to_get = status == 303 && current.method != Method::HEAD
        || matches!(status, 301 | 302) && current.method == Method::POST;
    if to_get {
        next.method = Method::GET;
        next.body = Bytes::new();
        for header in [CONTENT_LENGTH, CONTENT_TYPE, TRANSFER_ENCODING] {
            next.headers.remove(header);
        }
    }
    if next.url.host_str() != current.url.host_str() {
        next.headers.remove(AUTHORIZATION);
        next.headers.remove(COOKIE);
    }
    Some(next)
}

fn pool_key(url: &Url) -> PoolKey {
    let host = match url.host() {
        Some(Host::Domain(domain)) => domain.to_owned(),
        Some(Host::Ipv4(ip)) => ip.to_string(),
        Some(Host::Ipv6(ip)) => ip.to_string(),
        None => String::new(), // rejected by Request::new
    };
    PoolKey {
        https: matches!(url.scheme(), "https" | "wss"),
        host,
        port: url.port_or_known_default().unwrap_or(80),
    }
}

/// `body` decoded per the response's `Content-Encoding` (unless discarded).
fn decoded(headers: &HeaderMap, body: Bytes, discarded: bool) -> Result<Bytes, HttpError> {
    match headers.get(CONTENT_ENCODING) {
        Some(encoding) if !discarded && !body.is_empty() => {
            compression::decompress(body, &String::from_utf8_lossy(encoding.as_bytes()))
                .map_err(HttpError::Decompress)
        }
        _ => Ok(body),
    }
}

fn wants_close(headers: &HeaderMap) -> bool {
    headers
        .get_all(CONNECTION)
        .iter()
        .any(|v| v.to_str().is_ok_and(|v| v.eq_ignore_ascii_case("close")))
}

fn is_idempotent(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::PUT | Method::DELETE | Method::OPTIONS | Method::TRACE
    )
}
