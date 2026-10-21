//! HTTP/3 over QUIC (quinn + h3). Like HTTP/2, one connection per host
//! carries every concurrent request as a stream. The QUIC handshake includes
//! TLS, so all of it counts as connecting.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::HttpError;
use crate::connect::{ConnectTimings, Connection, PoolKey, Sender};
use crate::io::IoMarks;
use crate::net::Resolver;

pub(crate) type SendRequest = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// A client's QUIC side: its TLS setup, and one UDP endpoint per address
/// family, bound on first use (binding needs the Tokio runtime).
pub(crate) struct Quic {
    config: quinn::ClientConfig,
    v4: Mutex<Option<quinn::Endpoint>>,
    v6: Mutex<Option<quinn::Endpoint>>,
}

impl Quic {
    pub fn new(tls: rustls::ClientConfig) -> Result<Quic, HttpError> {
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
            .map_err(|e| HttpError::Config(format!("QUIC: {e}")))?;
        Ok(Quic {
            config: quinn::ClientConfig::new(Arc::new(crypto)),
            v4: Mutex::default(),
            v6: Mutex::default(),
        })
    }

    fn endpoint(&self, to: SocketAddr) -> Result<quinn::Endpoint, HttpError> {
        let (slot, local) = if to.is_ipv4() {
            (&self.v4, SocketAddr::from(([0, 0, 0, 0], 0)))
        } else {
            (&self.v6, SocketAddr::from(([0u16; 8], 0)))
        };
        let mut slot = lock(slot);
        if let Some(endpoint) = &*slot {
            return Ok(endpoint.clone());
        }
        let endpoint = quinn::Endpoint::client(local)
            .map_err(|e| HttpError::Http3(format!("could not open a UDP socket: {e}")))?;
        *slot = Some(endpoint.clone());
        Ok(endpoint)
    }
}

/// A shared handle on an HTTP/3 connection.
#[derive(Clone)]
pub(crate) struct H3 {
    pub send: SendRequest,
    quic: quinn::Connection,
    /// UDP bytes (sent, received) already handed to finished streams.
    taken: Arc<Mutex<(u64, u64)>>,
}

impl H3 {
    pub fn is_closed(&self) -> bool {
        self.quic.close_reason().is_some()
    }

    /// UDP bytes (sent, received) since the last call: as on HTTP/2, exact in
    /// total and approximate per request when streams overlap.
    pub fn take_bytes(&self) -> (u64, u64) {
        let stats = self.quic.stats();
        let now = (stats.udp_tx.bytes, stats.udp_rx.bytes);
        let mut taken = lock(&self.taken);
        let delta = (now.0 - taken.0, now.1 - taken.1);
        *taken = now;
        delta
    }
}

pub(crate) async fn connect(
    key: &PoolKey,
    resolver: &Resolver,
    quic: &Quic,
) -> Result<(Connection, ConnectTimings), HttpError> {
    if !key.https {
        return Err(HttpError::Http3(format!(
            "{} is plain HTTP; HTTP/3 needs https://",
            key.host
        )));
    }
    let addrs = resolver.resolve(&key.host, key.port, None).await?;
    let started = Instant::now();
    let mut last_err = None;
    let mut connected = None;
    for addr in addrs {
        let endpoint = quic.endpoint(addr)?;
        let attempt = endpoint
            .connect_with(quic.config.clone(), addr, &key.host)
            .map_err(|e| HttpError::Http3(format!("could not connect to {addr}: {e}")))?;
        match attempt.await {
            Ok(conn) => {
                connected = Some((conn, addr));
                break;
            }
            Err(e) => {
                last_err = Some(HttpError::Http3(format!(
                    "QUIC handshake with {addr} failed: {e}"
                )));
            }
        }
    }
    let Some((conn, addr)) = connected else {
        return Err(last_err.expect("resolve returns at least one address"));
    };
    let connecting = started.elapsed();

    let (mut driver, send) = h3::client::new(h3_quinn::Connection::new(conn.clone()))
        .await
        .map_err(|e| HttpError::Http3(e.to_string()))?;
    // Drives the connection until every handle is dropped or the server closes.
    tokio::spawn(async move {
        let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });
    let h3 = H3 {
        send,
        quic: conn,
        taken: Arc::default(),
    };
    Ok((
        Connection {
            sender: Sender::H3(h3),
            marks: Arc::new(IoMarks::default()),
            remote_addr: addr,
            // quinn doesn't say which suite QUIC's TLS 1.3 negotiated.
            tls: None,
        },
        ConnectTimings {
            connecting,
            tls_handshaking: Duration::ZERO,
        },
    ))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
