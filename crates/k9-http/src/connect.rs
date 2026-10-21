//! Opening connections with each phase timed: DNS lookup, TCP connect, TLS
//! handshake. The result is an HTTP/1.1 or HTTP/2 connection (ALPN decides
//! over TLS; plain text is HTTP/2 only when forced) whose I/O is timestamped.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hyper::client::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use crate::body::TimedBody;
use crate::io::{IoMarks, TimedStream};
use crate::net::{Resolver, tcp_connect};
use crate::quic::H3;
use crate::tls::{Connectors, TlsInfo};
use crate::{HttpError, HttpVersion};

/// Where a connection goes; connections are pooled per key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PoolKey {
    pub https: bool,
    pub host: String,
    pub port: u16,
}

pub(crate) struct Connection {
    pub sender: Sender,
    pub marks: Arc<IoMarks>,
    pub remote_addr: SocketAddr,
    pub tls: Option<TlsInfo>,
}

pub(crate) enum Sender {
    /// One request at a time: checked out of the pool for each request.
    H1(http1::SendRequest<TimedBody>),
    /// Any number of concurrent streams: shared, never checked out.
    H2(http2::SendRequest<TimedBody>),
    /// Like HTTP/2, over QUIC.
    H3(H3),
}

impl Connection {
    pub fn is_closed(&self) -> bool {
        match &self.sender {
            Sender::H1(sender) => sender.is_closed(),
            Sender::H2(sender) => sender.is_closed(),
            Sender::H3(h3) => h3.is_closed(),
        }
    }

    /// Another handle on an HTTP/2 or HTTP/3 connection; `None` for HTTP/1.1.
    pub fn share(&self) -> Option<Connection> {
        let sender = match &self.sender {
            Sender::H1(_) => return None,
            Sender::H2(sender) => Sender::H2(sender.clone()),
            Sender::H3(h3) => Sender::H3(h3.clone()),
        };
        Some(Connection {
            sender,
            marks: self.marks.clone(),
            remote_addr: self.remote_addr,
            tls: self.tls,
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ConnectTimings {
    pub connecting: Duration,
    pub tls_handshaking: Duration,
}

pub(crate) async fn connect(
    key: &PoolKey,
    resolver: &Resolver,
    local: Option<IpAddr>,
    tls: &Connectors,
    version: HttpVersion,
) -> Result<(Connection, ConnectTimings), HttpError> {
    let Dialed {
        stream,
        addr,
        timings,
        h2,
        tls,
    } = dial(key, resolver, local, tls, version).await?;
    let marks = Arc::new(IoMarks::default());
    let io = TokioIo::new(TimedStream::new(stream, marks.clone()));
    // Each connection task drives I/O until every sender is dropped or the server closes.
    let sender = if h2 {
        let (sender, conn) = http2::handshake(TokioExecutor::new(), io).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Sender::H2(sender)
    } else {
        let (sender, conn) = http1::handshake(io).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Sender::H1(sender)
    };

    Ok((
        Connection {
            sender,
            marks,
            remote_addr: addr,
            tls,
        },
        timings,
    ))
}

/// A TCP (and maybe TLS) stream to a host, before any HTTP.
pub(crate) struct Dialed {
    pub stream: MaybeTls,
    pub addr: SocketAddr,
    pub timings: ConnectTimings,
    /// TLS negotiated HTTP/2 (or it's forced over plain text).
    pub h2: bool,
    pub tls: Option<TlsInfo>,
}

/// Resolves, connects and does the TLS handshake for `key`, timing each.
pub(crate) async fn dial(
    key: &PoolKey,
    resolver: &Resolver,
    local: Option<IpAddr>,
    tls: &Connectors,
    version: HttpVersion,
) -> Result<Dialed, HttpError> {
    let addrs = resolver.resolve(&key.host, key.port, local).await?;

    // Try each resolved address in order (e.g. `localhost` → ::1, then
    // 127.0.0.1). `connecting` covers the whole dial, as in k6.
    let started = Instant::now();
    let mut last_err = None;
    let mut connected = None;
    for addr in addrs {
        match tcp_connect(addr, local).await {
            Ok(tcp) => {
                connected = Some((tcp, addr));
                break;
            }
            Err(source) => last_err = Some(HttpError::Connect { addr, source }),
        }
    }
    let Some((tcp, addr)) = connected else {
        return Err(last_err.expect("resolve returns at least one address"));
    };
    let _ = tcp.set_nodelay(true);
    let connecting = started.elapsed();

    let (stream, tls_handshaking, h2, info) = if key.https {
        let started = Instant::now();
        let name = ServerName::try_from(key.host.clone()).map_err(|e| HttpError::InvalidUrl {
            url: key.host.clone(),
            reason: e.to_string(),
        })?;
        let stream = tls
            .get(&key.host)
            .connect(name, tcp)
            .await
            .map_err(|source| {
                if version == HttpVersion::Http2 && refused_alpn(&source) {
                    HttpError::NoHttp2 {
                        host: key.host.clone(),
                    }
                } else {
                    HttpError::Tls {
                        host: key.host.clone(),
                        source,
                    }
                }
            })?;
        let h2 = stream.get_ref().1.alpn_protocol() == Some(b"h2");
        if version == HttpVersion::Http2 && !h2 {
            return Err(HttpError::NoHttp2 {
                host: key.host.clone(),
            });
        }
        let info = TlsInfo::of(stream.get_ref().1);
        (MaybeTls::Tls(Box::new(stream)), started.elapsed(), h2, info)
    } else {
        // Plain-text HTTP/2 (h2c) has no negotiation: prior knowledge only.
        let h2 = version == HttpVersion::Http2;
        (MaybeTls::Plain(tcp), Duration::ZERO, h2, None)
    };
    Ok(Dialed {
        stream,
        addr,
        timings: ConnectTimings {
            connecting,
            tls_handshaking,
        },
        h2,
        tls: info,
    })
}

/// True when the server ended the handshake because it speaks none of the
/// protocols offered with ALPN.
fn refused_alpn(error: &io::Error) -> bool {
    matches!(
        error
            .get_ref()
            .and_then(|e| e.downcast_ref::<rustls::Error>()),
        Some(rustls::Error::AlertReceived(
            rustls::AlertDescription::NoApplicationProtocol
        ))
    )
}

pub(crate) enum MaybeTls {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTls {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTls {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            MaybeTls::Tls(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            MaybeTls::Plain(s) => s.is_write_vectored(),
            MaybeTls::Tls(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}
