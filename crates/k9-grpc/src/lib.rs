//! The manager ↔ worker protocol over gRPC (`proto/k9/worker/v1/worker.proto`).
//!
//! The messages are k9-core's [`ToWorker`] and [`FromWorker`]; this crate
//! carries them over one bidirectional `Manager.Work` stream per worker, typed
//! in protobuf so tools in other languages can speak it too. The worker
//! connects ([`connect`]) and the manager serves ([`Listener`]); each side sees
//! the other's messages on a channel, and ending its sender ends its stream.
//!
//! Workers authenticate with the run's token (`authorization: Bearer …`
//! metadata); a wrong one is refused before any message. The stream can run
//! over TLS: the manager has a certificate ([`Tls`]), and workers trust it
//! through a CA certificate or the public web roots.

use std::net::SocketAddr;
use std::pin::Pin;

use k9_core::protocol::{FromWorker, ToWorker};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

mod convert;

/// The generated protobuf types and gRPC service.
pub mod pb {
    #![allow(clippy::all, missing_docs)]
    tonic::include_proto!("k9.worker.v1");
}

pub use convert::ConvertError;

/// Messages larger than this are refused (big `setup()` data, sample batches).
pub const MAX_MESSAGE: usize = 64 << 20;
/// Messages buffered per direction before the sender waits.
const BUFFER: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("could not connect to the manager at {address}: {}", chain(.source))]
    Connect {
        address: String,
        #[source]
        source: tonic::transport::Error,
    },
    #[error("the manager at {address} refused this worker's token")]
    Unauthenticated { address: String },
    #[error("the manager at {address} refused the stream: {}", .status.message())]
    Refused { address: String, status: Status },
    #[error("TLS: {0}")]
    Tls(String),
    #[error("invalid manager address `{0}`")]
    Address(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// An error and its causes, e.g. "transport error: invalid peer certificate".
fn chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(e) = source {
        text.push_str(": ");
        text.push_str(&e.to_string());
        source = e.source();
    }
    text
}

/// Whether a connection failed in the TLS handshake (a certificate that
/// isn't trusted, a protocol mismatch), rather than finding nobody there.
fn is_tls(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut next = Some(error);
    while let Some(e) = next {
        if e.is::<rustls::Error>() {
            return true;
        }
        if let Some(io) = e.downcast_ref::<std::io::Error>()
            && io
                .get_ref()
                .is_some_and(|inner| inner.is::<rustls::Error>())
        {
            return true;
        }
        next = e.source();
    }
    false
}

/// A certificate and its private key, in PEM.
#[derive(Clone)]
pub struct Tls {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

/// How the manager serves workers.
#[derive(Clone)]
pub struct ServerOptions {
    /// Workers must present it.
    pub token: String,
    pub tls: Option<Tls>,
}

impl ServerOptions {
    pub fn new(token: impl Into<String>) -> ServerOptions {
        ServerOptions {
            token: token.into(),
            tls: None,
        }
    }
}

/// How a worker reaches the manager.
#[derive(Clone)]
pub struct ClientOptions {
    pub token: String,
    /// The CA certificate (PEM) that signed the manager's, for `https://`
    /// addresses; without one, the public web roots are trusted.
    pub ca_pem: Option<Vec<u8>>,
}

impl ClientOptions {
    pub fn new(token: impl Into<String>) -> ClientOptions {
        ClientOptions {
            token: token.into(),
            ca_pem: None,
        }
    }
}

/// Why the other side's stream broke.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum StreamError {
    #[error("{0}")]
    Status(String),
    #[error("malformed message: {0}")]
    Malformed(#[from] ConvertError),
}

/// Messages from the other side; `None` once its stream has ended cleanly.
pub type Inbound<T> = mpsc::Receiver<Result<T, StreamError>>;

/// One worker's stream, on the manager's side.
pub struct Connection {
    /// Where the worker connected from.
    pub peer: Option<SocketAddr>,
    pub rx: Inbound<FromWorker>,
    /// Dropping it ends the stream to the worker.
    pub tx: mpsc::Sender<ToWorker>,
}

/// The manager's side: serves `Manager.Work` and hands over each worker's
/// stream as a [`Connection`].
pub struct Listener {
    address: SocketAddr,
    connections: mpsc::Receiver<Connection>,
    server: tokio::task::JoinHandle<()>,
}

impl Listener {
    /// Listens on `address` (e.g. `127.0.0.1:0` for any free port).
    pub async fn bind(address: &str, options: &ServerOptions) -> Result<Listener, Error> {
        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = &options.tls {
            let identity = tonic::transport::Identity::from_pem(&tls.cert_pem, &tls.key_pem);
            builder = builder
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
                .map_err(|e| Error::Tls(chain(&e)))?;
        }
        let tcp = tokio::net::TcpListener::bind(address).await?;
        let address = tcp.local_addr()?;
        let (connections_tx, connections) = mpsc::channel(BUFFER);
        let service = pb::manager_server::ManagerServer::new(Service {
            connections: connections_tx,
            bearer: format!("Bearer {}", options.token),
        })
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE);
        let incoming = TcpListenerStream::new(tcp).map(|stream| {
            if let Ok(stream) = &stream {
                let _ = stream.set_nodelay(true);
            }
            stream
        });
        let server = tokio::spawn(async move {
            let _ = builder
                .add_service(service)
                .serve_with_incoming(incoming)
                .await;
        });
        Ok(Listener {
            address,
            connections,
            server,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// The next worker's stream.
    pub async fn accept(&mut self) -> Option<Connection> {
        self.connections.recv().await
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct Service {
    connections: mpsc::Sender<Connection>,
    /// The `authorization` value workers must send.
    bearer: String,
}

/// Compares in constant time, so response times don't leak the token.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

type Outbound<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl pb::manager_server::Manager for Service {
    type WorkStream = Outbound<pb::ManagerMessage>;

    async fn work(
        &self,
        request: Request<Streaming<pb::WorkerMessage>>,
    ) -> Result<Response<Self::WorkStream>, Status> {
        let presented = request
            .metadata()
            .get("authorization")
            .map(|v| v.as_bytes())
            .unwrap_or_default();
        if !same(presented, self.bearer.as_bytes()) {
            return Err(Status::unauthenticated("wrong or missing token"));
        }
        let peer = request.remote_addr();
        let rx = inbound(request.into_inner(), FromWorker::try_from);
        let (tx, outgoing) = mpsc::channel::<ToWorker>(BUFFER);
        self.connections
            .send(Connection { peer, rx, tx })
            .await
            .map_err(|_| Status::unavailable("the manager isn't taking workers"))?;
        let stream = ReceiverStream::new(outgoing).map(|m| Ok(pb::ManagerMessage::from(m)));
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Converts a gRPC stream into a channel of k9 messages.
fn inbound<P, T>(mut stream: Streaming<P>, convert: fn(P) -> Result<T, ConvertError>) -> Inbound<T>
where
    P: Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel(BUFFER);
    tokio::spawn(async move {
        loop {
            let item = match stream.message().await {
                Ok(Some(message)) => convert(message).map_err(StreamError::from),
                Ok(None) => return,
                Err(status) => Err(StreamError::Status(status.message().to_owned())),
            };
            let broken = item.is_err();
            if tx.send(item).await.is_err() || broken {
                return;
            }
        }
    });
    rx
}

/// The worker's side: opens the stream to the manager at `address`
/// (`host:port`, or `https://host:port` for TLS). Send `Hello` first.
pub async fn connect(
    address: &str,
    options: &ClientOptions,
) -> Result<(mpsc::Sender<FromWorker>, Inbound<ToWorker>), Error> {
    let url = if address.contains("://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    };
    let mut endpoint = tonic::transport::Endpoint::from_shared(url)
        .map_err(|_| Error::Address(address.to_owned()))?
        .tcp_nodelay(true);
    if address.starts_with("https://") {
        let config = match &options.ca_pem {
            Some(ca) => tonic::transport::ClientTlsConfig::new()
                .ca_certificate(tonic::transport::Certificate::from_pem(ca)),
            None => tonic::transport::ClientTlsConfig::new().with_webpki_roots(),
        };
        endpoint = endpoint
            .tls_config(config)
            .map_err(|e| Error::Tls(chain(&e)))?;
    }
    let bearer: tonic::metadata::MetadataValue<_> = format!("Bearer {}", options.token)
        .parse()
        .map_err(|_| Error::Address("the token isn't valid in a header".to_owned()))?;
    let channel = endpoint.connect().await.map_err(|source| {
        if is_tls(&source) {
            // Retrying won't help: the certificate or the handshake is wrong.
            Error::Tls(format!("the manager at {address}: {}", chain(&source)))
        } else {
            Error::Connect {
                address: address.to_owned(),
                source,
            }
        }
    })?;
    let mut client = pb::manager_client::ManagerClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE);
    let (tx, outgoing) = mpsc::channel::<FromWorker>(BUFFER);
    let requests = ReceiverStream::new(outgoing).map(pb::WorkerMessage::from);
    let mut request = Request::new(requests);
    request.metadata_mut().insert("authorization", bearer);
    let response = client.work(request).await.map_err(|status| {
        if status.code() == tonic::Code::Unauthenticated {
            Error::Unauthenticated {
                address: address.to_owned(),
            }
        } else {
            Error::Refused {
                address: address.to_owned(),
                status,
            }
        }
    })?;
    Ok((tx, inbound(response.into_inner(), ToWorker::try_from)))
}
