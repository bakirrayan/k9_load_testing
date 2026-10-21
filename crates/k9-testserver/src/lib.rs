//! A target server for testing K9 itself: endpoints with fixed delays, error
//! rates, stalls on command, large bodies, redirects and connection closes.
//!
//! It serves HTTP, HTTPS (self-signed certificate for `localhost` and
//! `127.0.0.1`) and HTTPS requiring a client certificate (mutual TLS) and counts accepted TCP connections, so tests can check
//! connection reuse from the server's side.
//!
//! | Endpoint | Behavior |
//! |----------|----------|
//! | `GET /ok` | 200 `ok`; waits while the server is stalled |
//! | `GET /delay/{ms}` | 200 after `ms` milliseconds |
//! | `ANY /status/{code}` | responds with `code` |
//! | `GET /error-rate/{pct}` | 500 for exactly `pct` of every 100 requests, spread evenly |
//! | `GET /bytes/{n}` | `n`-byte body |
//! | `ANY /echo` | JSON with the request's method, path, query, headers and body (decoded per `Content-Encoding`) |
//! | `GET /compressed/{encodings}` | JSON (with the request's `acceptEncoding`) compressed with `gzip`, `deflate`, `br`, `zstd` (comma-separated, in order); `broken` claims gzip but isn't |
//! | `POST /multipart` | `{"fields": {…}, "files": {name: {filename, contentType, size, content}}}` from a multipart form |
//! | `GET /close` | 200 with `Connection: close` |
//! | `GET /ws/echo` | WebSocket: echoes each text or binary message; the text `close` makes the server close (code 4000, reason `bye`); answers pings |
//! | `GET /ws/push/{n}` | WebSocket: sends `1` … `n` as text, then closes normally (1000) |
//! | gRPC `k9.test.Echo` | `proto/k9/test/echo.proto`, over HTTP/2 (h2c on the plain port): `Say` echoes after `delay_ms` or fails with `status`; `Chat` streams `Say`'s answers; with server reflection (v1 and v1alpha) |
//! | `GET /redirect/{n}` | 302 chain ending at `/redirect/0` (200) |
//! | `ANY /redirect-to?url=…&status=…` | redirect (default 302) to `url` |
//! | `GET /cookies/set/{name}/{value}` | sets a cookie; `?redirect=…` answers with a 302 there |
//! | `GET /ip` | the client's IP address (HTTP/1.1 and HTTP/2) |
//! | `GET /cookies/has/{name}` | 200 if the request carries that cookie, else 404 |
//! | `GET /expect-header/{name}/{value}` | 200 if the request has that header value, else 412 |
//! | `POST /login` | JSON `{"username", "password"}`: with password `secret`, 200 `{"token": "tok-<username>", "user": {"name": …}}` and `X-Request-Id: req-<username>`; else 401 |
//! | `GET /me` | 200 `{"name": …}` with `Authorization: Bearer tok-<username>` (counted per user), else 401 |
//! | `POST /api/v1/write` | a Prometheus remote-write receiver: stores each push (see [`TestServer::remote_writes`]), 204 |
//! | `POST /v1/metrics` | an OTLP/HTTP metrics receiver: stores each push (see [`TestServer::otlp_pushes`]), 204 |
//! | `POST /write`, `POST /api/v2/write` | InfluxDB v1 and v2 write receivers: store each push (see [`TestServer::pushes`]), 204 |
//! | `POST /query` | InfluxDB v1 queries (`CREATE DATABASE`): stored like pushes, 200 |
//! | `POST /control/stall`, `POST /control/resume` | stall or release `/ok` |
//! | `POST /control/close-idle` | close idle keep-alive connections |
//! | `GET /control/stats` | `{"requests": …, "connections": …, "users": {…}}` (`/me` per user) |

pub mod grpc;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{CloseFrame, Message, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, Query, Request, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Extension, Router};
use bytes::Buf;
use http_body_util::{BodyExt, Full};
use hyper::service::Service;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

/// Largest body `/bytes/{n}` will produce (64 MiB).
pub const MAX_BYTES: usize = 64 << 20;

#[derive(Default)]
struct Counters {
    requests: AtomicU64,
    connections: AtomicU64,
    error_rate: AtomicU64,
}

#[derive(Clone)]
struct AppState {
    counters: Arc<Counters>,
    stalled: watch::Sender<bool>,
    /// Bumped to make every connection shut down gracefully (idle ones close now).
    close_idle: watch::Sender<u64>,
    /// Successful `/me` requests by user.
    users: Arc<Mutex<BTreeMap<String, u64>>>,
    remote_writes: Arc<Mutex<Vec<RemoteWrite>>>,
}

/// One push to `POST /api/v1/write`, as received.
#[derive(Debug, Clone)]
pub struct RemoteWrite {
    pub path: String,
    /// The query string (empty without one).
    pub query: String,
    /// Request headers (lower-case names).
    pub headers: BTreeMap<String, String>,
    /// The raw (snappy-compressed) body.
    pub body: Vec<u8>,
}

/// A running test server. Its tasks live on the Tokio runtime that started it.
pub struct TestServer {
    http_addr: SocketAddr,
    https_addr: SocketAddr,
    /// HTTPS that requires a client certificate.
    mtls_addr: SocketAddr,
    cert_der: CertificateDer<'static>,
    client_identity: ClientIdentity,
    state: AppState,
}

impl TestServer {
    /// Starts HTTP and HTTPS listeners on ephemeral ports of 127.0.0.1. Both
    /// speak HTTP/1.1 and HTTP/2 (negotiated with ALPN over TLS, prior
    /// knowledge in plain text), and the HTTPS port also answers HTTP/3 over
    /// QUIC (UDP, same port number).
    pub async fn start() -> std::io::Result<TestServer> {
        Self::bind(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            SocketAddr::from(([127, 0, 0, 1], 0)),
        )
        .await
    }

    /// Like [`TestServer::start`], but the server only speaks HTTP/1.1 (no
    /// HTTP/2, no QUIC listener).
    pub async fn start_http1() -> std::io::Result<TestServer> {
        Self::bind_with(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            SocketAddr::from(([127, 0, 0, 1], 0)),
            false,
        )
        .await
    }

    pub async fn bind(http: SocketAddr, https: SocketAddr) -> std::io::Result<TestServer> {
        Self::bind_with(http, https, true).await
    }

    async fn bind_with(
        http: SocketAddr,
        https: SocketAddr,
        http2: bool,
    ) -> std::io::Result<TestServer> {
        let state = AppState {
            counters: Arc::default(),
            stalled: watch::Sender::new(false),
            close_idle: watch::Sender::new(0),
            users: Arc::default(),
            remote_writes: Arc::default(),
        };
        let router = router(state.clone());

        let (cert_der, key, tls_config, quic_config) = self_signed_tls(http2)?;
        let (mtls_config, client_identity) = mutual_tls(cert_der.clone(), key)?;
        let mtls_listener = TcpListener::bind(SocketAddr::new(https.ip(), 0)).await?;
        let http_listener = TcpListener::bind(http).await?;
        let (https_listener, quic) = if http2 {
            let (tcp, quic) = bind_tcp_and_udp(https, quic_config).await?;
            (tcp, Some(quic))
        } else {
            (TcpListener::bind(https).await?, None)
        };
        let server = TestServer {
            http_addr: http_listener.local_addr()?,
            https_addr: https_listener.local_addr()?,
            mtls_addr: mtls_listener.local_addr()?,
            cert_der,
            client_identity,
            state: state.clone(),
        };
        tokio::spawn(accept_loop(
            mtls_listener,
            Some(TlsAcceptor::from(Arc::new(mtls_config))),
            router.clone(),
            state.clone(),
            true,
        ));

        tokio::spawn(accept_loop(
            http_listener,
            None,
            router.clone(),
            state.clone(),
            http2,
        ));
        if let Some(endpoint) = quic {
            tokio::spawn(quic_loop(endpoint, router.clone(), state.clone()));
        }
        tokio::spawn(accept_loop(
            https_listener,
            Some(TlsAcceptor::from(Arc::new(tls_config))),
            router,
            state.clone(),
            http2,
        ));
        Ok(server)
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    pub fn https_addr(&self) -> SocketAddr {
        self.https_addr
    }

    /// `http://127.0.0.1:{port}{path}`
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.http_addr)
    }

    /// `https://127.0.0.1:{port}{path}` (use [`TestServer::cert_der`] as a trusted root).
    pub fn https_url(&self, path: &str) -> String {
        format!("https://{}{path}", self.https_addr)
    }

    /// The self-signed certificate, DER-encoded, for adding to a client's roots.
    /// The mutual-TLS listener: HTTPS (HTTP/1.1 and HTTP/2) that completes
    /// the handshake only with [`TestServer::client_identity`]'s certificate.
    pub fn mtls_addr(&self) -> SocketAddr {
        self.mtls_addr
    }

    pub fn mtls_url(&self, path: &str) -> String {
        format!("https://localhost:{}{path}", self.mtls_addr.port())
    }

    /// A client certificate (and key) the mutual-TLS listener accepts.
    pub fn client_identity(&self) -> &ClientIdentity {
        &self.client_identity
    }

    pub fn cert_der(&self) -> &CertificateDer<'static> {
        &self.cert_der
    }

    /// Makes `/ok` hang until [`TestServer::resume`] is called.
    pub fn stall(&self) {
        self.state.stalled.send_replace(true);
    }

    pub fn resume(&self) {
        self.state.stalled.send_replace(false);
    }

    /// Closes idle keep-alive connections, as a server with a short keep-alive
    /// timeout would. Requests in flight finish first.
    pub fn close_idle_connections(&self) {
        self.state
            .close_idle
            .send_modify(|generation| *generation += 1);
    }

    /// Requests handled so far.
    pub fn requests(&self) -> u64 {
        self.state.counters.requests.load(Ordering::Relaxed)
    }

    /// Successful `GET /me` requests so far, by user.
    pub fn users(&self) -> BTreeMap<String, u64> {
        self.state.users.lock().unwrap().clone()
    }

    /// Pushes received on `POST /api/v1/write`, in order.
    pub fn remote_writes(&self) -> Vec<RemoteWrite> {
        self.pushes("/api/v1/write")
    }

    /// Pushes received on `POST /v1/metrics` (OTLP/HTTP), in order.
    pub fn otlp_pushes(&self) -> Vec<RemoteWrite> {
        self.pushes("/v1/metrics")
    }

    /// Pushes received on `POST {path}`, in order.
    pub fn pushes(&self, path: &str) -> Vec<RemoteWrite> {
        let all = self.state.remote_writes.lock().unwrap();
        all.iter().filter(|p| p.path == path).cloned().collect()
    }

    /// TCP connections accepted so far (HTTP and HTTPS).
    pub fn connections(&self) -> u64 {
        self.state.counters.connections.load(Ordering::Relaxed)
    }
}

type TlsConfigs = (
    CertificateDer<'static>,
    PrivateKeyDer<'static>,
    ServerConfig,
    quinn::ServerConfig,
);

/// A client certificate and its key, PEM-encoded.
#[derive(Debug, Clone)]
pub struct ClientIdentity {
    pub cert_pem: String,
    pub key_pem: String,
}

/// A server config that requires a client certificate signed by a CA made
/// here, and a client identity it accepts.
fn mutual_tls(
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> std::io::Result<(ServerConfig, ClientIdentity)> {
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose,
        IsCa, KeyPair,
    };
    let mut ca = CertificateParams::new(Vec::<String>::new()).map_err(std::io::Error::other)?;
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.distinguished_name.push(DnType::CommonName, "k9 test CA");
    let ca = CertifiedIssuer::self_signed(ca, KeyPair::generate().map_err(std::io::Error::other)?)
        .map_err(std::io::Error::other)?;
    let client_key = KeyPair::generate().map_err(std::io::Error::other)?;
    let mut client = CertificateParams::new(Vec::<String>::new()).map_err(std::io::Error::other)?;
    client
        .distinguished_name
        .push(DnType::CommonName, "k9 client");
    client.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client = client
        .signed_by(&client_key, &ca)
        .map_err(std::io::Error::other)?;

    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.add(ca.der().clone()).map_err(std::io::Error::other)?;
    let verifier = tokio_rustls::rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        provider.clone(),
    )
    .build()
    .map_err(std::io::Error::other)?;
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(std::io::Error::other)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![cert], key)
        .map_err(std::io::Error::other)?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok((
        config,
        ClientIdentity {
            cert_pem: client.pem(),
            key_pem: client_key.serialize_pem(),
        },
    ))
}

/// A self-signed certificate, and the TLS (TCP) and QUIC configurations using it.
fn self_signed_tls(http2: bool) -> std::io::Result<TlsConfigs> {
    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .map_err(std::io::Error::other)?;
    let cert_der = certified.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certified.signing_key.serialize_der(),
    ));
    let server_key = key.clone_key();
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let mut quic = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&tokio_rustls::rustls::version::TLS13])
        .map_err(std::io::Error::other)?
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key.clone_key())
        .map_err(std::io::Error::other)?;
    quic.alpn_protocols = vec![b"h3".to_vec()];
    let quic =
        quinn::crypto::rustls::QuicServerConfig::try_from(quic).map_err(std::io::Error::other)?;
    let quic = quinn::ServerConfig::with_crypto(Arc::new(quic));
    let config = ServerConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(std::io::Error::other)?
    .with_no_client_auth()
    .with_single_cert(vec![cert_der.clone()], key)
    .map_err(std::io::Error::other)?;
    let mut config = config;
    config.alpn_protocols = if http2 {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    } else {
        vec![b"http/1.1".to_vec()]
    };
    Ok((cert_der, server_key, config, quic))
}

/// Binds TCP and UDP on the same port number, as HTTPS servers offering
/// HTTP/3 do. For an ephemeral port, retries until the UDP port is free too.
async fn bind_tcp_and_udp(
    addr: SocketAddr,
    config: quinn::ServerConfig,
) -> std::io::Result<(TcpListener, quinn::Endpoint)> {
    let mut attempts = 0;
    loop {
        let tcp = TcpListener::bind(addr).await?;
        let bound = tcp.local_addr()?;
        match quinn::Endpoint::server(config.clone(), bound) {
            Ok(endpoint) => return Ok((tcp, endpoint)),
            Err(e) if addr.port() != 0 || attempts == 20 => return Err(e),
            Err(_) => attempts += 1,
        }
    }
}

async fn quic_loop(endpoint: quinn::Endpoint, router: Router, state: AppState) {
    while let Some(incoming) = endpoint.accept().await {
        state.counters.connections.fetch_add(1, Ordering::Relaxed);
        let router = router.clone();
        tokio::spawn(async move {
            let Ok(conn) = incoming.await else {
                return;
            };
            let Ok(mut conn) =
                h3::server::Connection::<_, Bytes>::new(h3_quinn::Connection::new(conn)).await
            else {
                return;
            };
            while let Ok(Some(resolver)) = conn.accept().await {
                let router = router.clone();
                tokio::spawn(async move {
                    if let Ok((request, stream)) = resolver.resolve_request().await {
                        let _ = serve_h3(request, stream, router).await;
                    }
                });
            }
        });
    }
}

/// Answers one HTTP/3 request with the same router as HTTP/1.1 and HTTP/2.
async fn serve_h3<S>(
    request: axum::http::Request<()>,
    mut stream: h3::server::RequestStream<S, Bytes>,
    router: Router,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: h3::quic::BidiStream<Bytes>,
{
    let mut body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let part = chunk.chunk();
            body.extend_from_slice(part);
            let n = part.len();
            chunk.advance(n);
        }
    }
    let (parts, ()) = request.into_parts();
    let request = axum::http::Request::from_parts(parts, Full::new(Bytes::from(body)));
    let response = TowerToHyperService::new(router).call(request).await?;
    let (parts, body) = response.into_parts();
    let body = body.collect().await?.to_bytes();
    stream
        .send_response(axum::http::Response::from_parts(parts, ()))
        .await?;
    if !body.is_empty() {
        stream.send_data(body).await?;
    }
    stream.finish().await?;
    Ok(())
}

async fn accept_loop(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    router: Router,
    state: AppState,
    http2: bool,
) {
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            continue;
        };
        let _ = stream.set_nodelay(true);
        state.counters.connections.fetch_add(1, Ordering::Relaxed);
        let router = router.clone().layer(Extension(ConnectInfo(peer)));
        let close_idle = state.close_idle.subscribe();
        let tls = tls.clone();
        tokio::spawn(async move {
            match tls {
                None => serve(stream, router, close_idle, http2).await,
                Some(acceptor) => {
                    if let Ok(stream) = acceptor.accept(stream).await {
                        serve(stream, router, close_idle, http2).await;
                    }
                }
            }
        });
    }
}

async fn serve<S>(stream: S, router: Router, mut close_idle: watch::Receiver<u64>, http2: bool)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // `auto` tells HTTP/2 from HTTP/1.1 by the connection preface.
    let mut builder = auto::Builder::new(TokioExecutor::new());
    if !http2 {
        builder = builder.http1_only();
    }
    let conn = builder
        .serve_connection_with_upgrades(TokioIo::new(stream), TowerToHyperService::new(router));
    tokio::pin!(conn);
    tokio::select! {
        _ = conn.as_mut() => {}
        _ = close_idle.changed() => {
            conn.as_mut().graceful_shutdown();
            let _ = conn.await;
        }
    }
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/ok", get(ok))
        .route("/ip", get(client_ip))
        .route("/delay/{ms}", get(delay))
        .route("/status/{code}", any(status))
        .route("/error-rate/{pct}", get(error_rate))
        .route("/bytes/{n}", get(bytes))
        .route("/echo", any(echo))
        .route("/compressed/{encodings}", get(compressed))
        .route("/multipart", post(multipart))
        .route("/ws/echo", get(ws_echo))
        .route("/ws/push/{n}", get(ws_push))
        .route_service(
            "/k9.test.Echo/{*method}",
            grpc::EchoServer::new(grpc::Echo)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .send_compressed(tonic::codec::CompressionEncoding::Gzip),
        )
        .route_service(
            "/grpc.reflection.v1.ServerReflection/{*method}",
            grpc::reflection().0,
        )
        .route_service(
            "/grpc.reflection.v1alpha.ServerReflection/{*method}",
            grpc::reflection().1,
        )
        .route("/close", get(close))
        .route("/redirect/{n}", get(redirect))
        .route("/redirect-to", any(redirect_to))
        .route("/cookies/set/{name}/{value}", get(set_cookie))
        .route("/cookies/has/{name}", get(has_cookie))
        .route("/expect-header/{name}/{value}", get(expect_header))
        .route("/login", post(login))
        .route("/api/v1/write", post(remote_write))
        .route("/v1/metrics", post(remote_write))
        .route("/write", post(remote_write))
        .route("/api/v2/write", post(remote_write))
        .route("/query", post(influx_query))
        .route("/me", get(me))
        .route("/control/stall", post(control_stall))
        .route("/control/resume", post(control_resume))
        .route("/control/close-idle", post(control_close_idle))
        .route("/control/stats", get(control_stats))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            count_requests,
        ))
        .with_state(state)
}

async fn count_requests(
    State(state): State<AppState>,
    req: Request,
    next: axum::middleware::Next,
) -> Response {
    state.counters.requests.fetch_add(1, Ordering::Relaxed);
    next.run(req).await
}

async fn client_ip(ConnectInfo(peer): ConnectInfo<SocketAddr>) -> String {
    peer.ip().to_string()
}

async fn ok(State(state): State<AppState>) -> &'static str {
    let mut stalled = state.stalled.subscribe();
    let _ = stalled.wait_for(|s| !*s).await;
    "ok"
}

async fn delay(Path(ms): Path<u64>) -> String {
    tokio::time::sleep(Duration::from_millis(ms)).await;
    format!("delayed {ms}ms")
}

async fn status(Path(code): Path<u16>) -> Response {
    match StatusCode::from_u16(code) {
        Ok(status) => (status, format!("status {code}")).into_response(),
        Err(_) => (StatusCode::BAD_REQUEST, "invalid status code").into_response(),
    }
}

/// Fails exactly `pct` of every 100 requests, spread evenly rather than in a burst.
async fn error_rate(State(state): State<AppState>, Path(pct): Path<u64>) -> Response {
    let pct = pct.min(100);
    let n = state.counters.error_rate.fetch_add(1, Ordering::Relaxed);
    let fails = (n + 1) * pct / 100 > n * pct / 100;
    if fails {
        (StatusCode::INTERNAL_SERVER_ERROR, "injected error").into_response()
    } else {
        "ok".into_response()
    }
}

async fn bytes(Path(n): Path<usize>) -> Response {
    if n > MAX_BYTES {
        return (StatusCode::BAD_REQUEST, "too large").into_response();
    }
    Bytes::from(vec![b'x'; n]).into_response()
}

async fn echo(method: Method, uri: Uri, headers: HeaderMap, body: Bytes) -> Response {
    let body = match headers.get(header::CONTENT_ENCODING) {
        Some(encoding) => {
            let encodings = String::from_utf8_lossy(encoding.as_bytes()).into_owned();
            match decode(&encodings, &body) {
                Ok(decoded) => decoded,
                Err(err) => {
                    return (StatusCode::BAD_REQUEST, format!("{encodings}: {err}"))
                        .into_response();
                }
            }
        }
        None => body.to_vec(),
    };
    let headers: BTreeMap<_, _> = headers
        .iter()
        .map(|(k, v)| (k.as_str().to_owned(), String::from_utf8_lossy(v.as_bytes())))
        .collect();
    axum::Json(serde_json::json!({
        "method": method.as_str(),
        "path": uri.path(),
        "query": uri.query(),
        "headers": headers,
        "body": String::from_utf8_lossy(&body),
    }))
    .into_response()
}

/// Undoes `Content-Encoding: encodings` (applied in order, so undone in reverse).
fn decode(encodings: &str, body: &[u8]) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut data = body.to_vec();
    for encoding in encodings.split(',').map(str::trim).rev() {
        let mut out = Vec::new();
        match encoding {
            "gzip" => flate2::read::GzDecoder::new(&data[..]).read_to_end(&mut out)?,
            "deflate" => flate2::read::ZlibDecoder::new(&data[..]).read_to_end(&mut out)?,
            "br" => brotli::Decompressor::new(&data[..], 4096).read_to_end(&mut out)?,
            "zstd" => zstd::stream::read::Decoder::new(&data[..])?.read_to_end(&mut out)?,
            other => {
                return Err(std::io::Error::other(format!("unknown encoding {other}")));
            }
        };
        data = out;
    }
    Ok(data)
}

/// `/compressed/{encodings}`: a JSON body (with the request's
/// `Accept-Encoding`) compressed with `encodings` in order, whatever was asked.
async fn compressed(Path(encodings): Path<String>, headers: HeaderMap) -> Response {
    use std::io::Write;
    let accept = headers
        .get(header::ACCEPT_ENCODING)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned());
    let mut data = serde_json::to_vec(&serde_json::json!({
        "compressed": encodings,
        "acceptEncoding": accept,
        "padding": "k9 ".repeat(200),
    }))
    .expect("JSON");
    for encoding in encodings.split(',').map(str::trim) {
        data = match encoding {
            "gzip" => {
                let mut e =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(&data).and_then(|()| e.finish())
            }
            "deflate" => {
                let mut e =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(&data).and_then(|()| e.finish())
            }
            "br" => {
                let mut out = Vec::new();
                let mut e = brotli::CompressorWriter::new(&mut out, 4096, 5, 22);
                let written = e.write_all(&data);
                drop(e);
                written.map(|()| out)
            }
            "zstd" => zstd::stream::encode_all(&data[..], 3),
            "broken" => Ok(b"not gzip at all".to_vec()),
            other => {
                return (StatusCode::BAD_REQUEST, format!("unknown encoding {other}"))
                    .into_response();
            }
        }
        .expect("in-memory compression");
    }
    let content_encoding = encodings.replace("broken", "gzip");
    (
        [
            (header::CONTENT_ENCODING, content_encoding),
            (header::CONTENT_TYPE, "application/json".to_owned()),
        ],
        data,
    )
        .into_response()
}

/// `/multipart`: the form's fields and files, as parsed by `multer`.
async fn multipart(mut form: axum::extract::Multipart) -> Response {
    let mut fields = serde_json::Map::new();
    let mut files = serde_json::Map::new();
    loop {
        let field = match form.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(err) => return (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
        };
        let name = field.name().unwrap_or_default().to_owned();
        let filename = field.file_name().map(str::to_owned);
        let content_type = field.content_type().map(str::to_owned);
        let data = match field.bytes().await {
            Ok(data) => data,
            Err(err) => return (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
        };
        let text = String::from_utf8_lossy(&data).into_owned();
        match filename {
            Some(filename) => {
                files.insert(
                    name,
                    serde_json::json!({
                        "filename": filename,
                        "contentType": content_type,
                        "size": data.len(),
                        "content": text,
                    }),
                );
            }
            None => {
                fields.insert(name, text.into());
            }
        }
    }
    axum::Json(serde_json::json!({"fields": fields, "files": files})).into_response()
}

async fn ws_echo(upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(|mut socket| async move {
        while let Some(Ok(message)) = socket.recv().await {
            let reply = match message {
                Message::Text(text) if text.as_str() == "close" => {
                    let frame = CloseFrame {
                        code: 4000,
                        reason: "bye".into(),
                    };
                    let _ = socket.send(Message::Close(Some(frame))).await;
                    return;
                }
                Message::Text(_) | Message::Binary(_) => message,
                // Pongs to pings, and the reply to a close, are sent by axum
                // itself; reading on until the end lets the close reply out.
                Message::Ping(_) | Message::Pong(_) | Message::Close(_) => continue,
            };
            if socket.send(reply).await.is_err() {
                return;
            }
        }
    })
}

async fn ws_push(Path(n): Path<u32>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |mut socket| async move {
        for i in 1..=n {
            if socket
                .send(Message::Text(i.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
        let frame = CloseFrame {
            code: 1000,
            reason: "".into(),
        };
        let _ = socket.send(Message::Close(Some(frame))).await;
        // Waits for the client's close reply.
        while let Some(Ok(_)) = socket.recv().await {}
    })
}

async fn close() -> Response {
    ([(header::CONNECTION, "close")], "closing").into_response()
}

async fn redirect(Path(n): Path<u32>) -> Response {
    if n == 0 {
        return "redirects done".into_response();
    }
    (
        StatusCode::FOUND,
        [(header::LOCATION, format!("/redirect/{}", n - 1))],
    )
        .into_response()
}

async fn redirect_to(Query(params): Query<HashMap<String, String>>) -> Response {
    let Some(url) = params.get("url") else {
        return (StatusCode::BAD_REQUEST, "missing ?url=").into_response();
    };
    let status = params
        .get("status")
        .and_then(|s| s.parse().ok())
        .and_then(|s| StatusCode::from_u16(s).ok())
        .unwrap_or(StatusCode::FOUND);
    (status, [(header::LOCATION, url.clone())]).into_response()
}

async fn set_cookie(
    Path((name, value)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let cookie = format!("{name}={value}; Path=/");
    match params.get("redirect") {
        Some(to) => (
            StatusCode::FOUND,
            [(header::SET_COOKIE, cookie), (header::LOCATION, to.clone())],
        )
            .into_response(),
        None => ([(header::SET_COOKIE, cookie)], "cookie set").into_response(),
    }
}

async fn has_cookie(Path(name): Path<String>, headers: HeaderMap) -> StatusCode {
    let present = headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .any(|pair| pair.trim().split('=').next() == Some(name.as_str()));
    if present {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn expect_header(
    Path((name, value)): Path<(String, String)>,
    headers: HeaderMap,
) -> StatusCode {
    let matches = headers
        .get(name.as_str())
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == value);
    if matches {
        StatusCode::OK
    } else {
        StatusCode::PRECONDITION_FAILED
    }
}

async fn remote_write(
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let headers = headers
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_owned(),
                String::from_utf8_lossy(v.as_bytes()).into_owned(),
            )
        })
        .collect();
    state.remote_writes.lock().unwrap().push(RemoteWrite {
        path: uri.path().to_owned(),
        query: uri.query().unwrap_or_default().to_owned(),
        headers,
        body: body.to_vec(),
    });
    StatusCode::NO_CONTENT
}

async fn influx_query(
    state: State<AppState>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    remote_write(state, uri, headers, body).await;
    axum::Json(serde_json::json!({"results": [{"statement_id": 0}]})).into_response()
}

async fn login(body: Bytes) -> Response {
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let (Some(user), Some("secret")) = (json["username"].as_str(), json["password"].as_str())
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    (
        [("x-request-id", format!("req-{user}"))],
        axum::Json(serde_json::json!({"token": format!("tok-{user}"), "user": {"name": user}})),
    )
        .into_response()
}

async fn me(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let user = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer tok-"));
    let Some(user) = user.filter(|u| !u.is_empty()) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    *state
        .users
        .lock()
        .unwrap()
        .entry(user.to_owned())
        .or_default() += 1;
    axum::Json(serde_json::json!({"name": user})).into_response()
}

async fn control_stall(State(state): State<AppState>) -> &'static str {
    state.stalled.send_replace(true);
    "stalled"
}

async fn control_resume(State(state): State<AppState>) -> &'static str {
    state.stalled.send_replace(false);
    "resumed"
}

async fn control_close_idle(State(state): State<AppState>) -> &'static str {
    state.close_idle.send_modify(|generation| *generation += 1);
    "closing idle connections"
}

async fn control_stats(State(state): State<AppState>) -> Response {
    axum::Json(serde_json::json!({
        "requests": state.counters.requests.load(Ordering::Relaxed),
        "connections": state.counters.connections.load(Ordering::Relaxed),
        "users": *state.users.lock().unwrap(),
    }))
    .into_response()
}
