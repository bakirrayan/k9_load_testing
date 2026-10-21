//! TLS settings: k6's `tlsVersion`, `tlsCipherSuites` and `tlsAuth` (client
//! certificates per domain), and `insecureSkipTLSVerify`, which accepts any
//! server certificate.
//!
//! With `insecureSkipTLSVerify`, the handshake signatures are still checked
//! (so the TLS session itself is sound); only the certificate chain and
//! hostname aren't.

use std::sync::Arc;

use rustls::DigitallySignedStruct;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

#[derive(Debug)]
pub(crate) struct AcceptAnyCertificate(pub Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

use std::str::FromStr;

use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::pem::PemObject;
use tokio_rustls::TlsConnector;

use crate::{ClientConfig, HttpError, HttpVersion};

/// A TLS version k9 can be limited to (k6's `tlsVersion`). rustls speaks
/// TLS 1.2 and 1.3 only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TlsVersion {
    Tls12,
    Tls13,
}

impl TlsVersion {
    pub fn name(self) -> &'static str {
        match self {
            TlsVersion::Tls12 => "tls1.2",
            TlsVersion::Tls13 => "tls1.3",
        }
    }
}

impl FromStr for TlsVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<TlsVersion, String> {
        match s {
            "tls1.2" => Ok(TlsVersion::Tls12),
            "tls1.3" => Ok(TlsVersion::Tls13),
            "ssl3.0" | "tls1.0" | "tls1.1" => {
                Err(format!("{s} isn't supported: k9 speaks tls1.2 and tls1.3"))
            }
            other => Err(format!(
                "unknown TLS version {other:?} (use tls1.2 or tls1.3)"
            )),
        }
    }
}

/// A client certificate (k6's `tlsAuth` entry), presented to `domains`
/// (`example.com`, or `*.example.com` for its subdomains); to every host
/// when `domains` is empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCert {
    pub domains: Vec<String>,
    /// The certificate chain, PEM-encoded.
    pub cert_pem: String,
    /// Its private key (PKCS#8, PKCS#1 or SEC1), PEM-encoded and unencrypted.
    pub key_pem: String,
}

/// What a TLS connection negotiated (k6's `tls_version` and `tls_cipher_suite`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TlsInfo {
    /// `tls1.2` or `tls1.3`.
    pub version: &'static str,
    /// The IANA name, e.g. `TLS_AES_128_GCM_SHA256`.
    pub cipher_suite: &'static str,
}

impl TlsInfo {
    pub(crate) fn of(conn: &rustls::ClientConnection) -> Option<TlsInfo> {
        let version = match conn.protocol_version()? {
            rustls::ProtocolVersion::TLSv1_2 => "tls1.2",
            rustls::ProtocolVersion::TLSv1_3 => "tls1.3",
            _ => "unknown",
        };
        Some(TlsInfo {
            version,
            cipher_suite: suite_name(conn.negotiated_cipher_suite()?.suite()),
        })
    }
}

/// IANA's name for a suite (rustls prefixes TLS 1.3's with `TLS13_`).
fn suite_name(suite: rustls::CipherSuite) -> &'static str {
    use rustls::CipherSuite::*;
    match suite {
        TLS13_AES_128_GCM_SHA256 => "TLS_AES_128_GCM_SHA256",
        TLS13_AES_256_GCM_SHA384 => "TLS_AES_256_GCM_SHA384",
        TLS13_CHACHA20_POLY1305_SHA256 => "TLS_CHACHA20_POLY1305_SHA256",
        other => other.as_str().unwrap_or("unknown"),
    }
}

/// A TLS connector per client certificate: the one for a host is the first
/// whose domains match it, else the default (with the certificate that has
/// no domains, if any).
#[derive(Clone)]
pub(crate) struct Connectors {
    default: TlsConnector,
    by_domain: Arc<[(Vec<String>, TlsConnector)]>,
}

impl Connectors {
    pub fn new(config: &ClientConfig) -> Result<Connectors, HttpError> {
        let connector = |cert| tls_config(config, cert).map(|c| TlsConnector::from(Arc::new(c)));
        let everywhere = config.client_certs.iter().find(|c| c.domains.is_empty());
        let by_domain = config
            .client_certs
            .iter()
            .filter(|c| !c.domains.is_empty())
            .map(|c| Ok((c.domains.clone(), connector(Some(c))?)))
            .collect::<Result<Vec<_>, HttpError>>()?;
        Ok(Connectors {
            default: connector(everywhere)?,
            by_domain: by_domain.into(),
        })
    }

    pub fn get(&self, host: &str) -> &TlsConnector {
        self.by_domain
            .iter()
            .find(|(domains, _)| domains.iter().any(|d| domain_matches(d, host)))
            .map_or(&self.default, |(_, connector)| connector)
    }
}

fn domain_matches(pattern: &str, host: &str) -> bool {
    match pattern.strip_prefix("*.") {
        Some(parent) => host
            .len()
            .checked_sub(parent.len() + 1)
            .is_some_and(|cut| host[cut..].eq_ignore_ascii_case(&format!(".{parent}")) && cut > 0),
        None => pattern.eq_ignore_ascii_case(host),
    }
}

/// The rustls configuration for `config`, presenting `cert` if given.
pub(crate) fn tls_config(
    config: &ClientConfig,
    cert: Option<&ClientCert>,
) -> Result<rustls::ClientConfig, HttpError> {
    let invalid = |e: &dyn std::fmt::Display| HttpError::Config(e.to_string());
    let mut roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    for cert in &config.extra_root_certs {
        roots
            .add(cert.clone())
            .map_err(|e| HttpError::Config(format!("extra root certificate: {e}")))?;
    }
    let mut provider = rustls::crypto::ring::default_provider();
    if !config.cipher_suites.is_empty() {
        let known = provider.cipher_suites.clone();
        provider.cipher_suites = config
            .cipher_suites
            .iter()
            .map(|name| {
                known
                    .iter()
                    .find(|s| suite_name(s.suite()) == name.as_str())
                    .copied()
                    .ok_or_else(|| {
                        let names: Vec<_> = known.iter().map(|s| suite_name(s.suite())).collect();
                        HttpError::Config(format!(
                            "unknown cipher suite {name} (known: {})",
                            names.join(", ")
                        ))
                    })
            })
            .collect::<Result<_, _>>()?;
    }
    let provider = Arc::new(provider);
    let (min, max) = config
        .tls_versions
        .unwrap_or((TlsVersion::Tls12, TlsVersion::Tls13));
    if min > max {
        return Err(HttpError::Config(format!(
            "tlsVersion: min {} is above max {}",
            min.name(),
            max.name()
        )));
    }
    // QUIC is TLS 1.3 only.
    let min = match config.http_version {
        HttpVersion::Http3 => TlsVersion::Tls13,
        _ => min,
    };
    let versions: Vec<&'static rustls::SupportedProtocolVersion> = [
        (TlsVersion::Tls12, &rustls::version::TLS12),
        (TlsVersion::Tls13, &rustls::version::TLS13),
    ]
    .into_iter()
    .filter(|(v, _)| (min..=max).contains(v))
    .map(|(_, version)| version)
    .collect();
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&versions)
        .map_err(|e| invalid(&format!("TLS versions and cipher suites: {e}")))?
        .with_root_certificates(roots);
    let mut tls = match cert {
        Some(cert) => {
            let fail = |e: &dyn std::fmt::Display| {
                HttpError::Config(format!("client certificate for {:?}: {e}", cert.domains))
            };
            let chain = CertificateDer::pem_slice_iter(cert.cert_pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| fail(&e))?;
            if chain.is_empty() {
                return Err(fail(&"no certificate in the PEM"));
            }
            let key =
                PrivateKeyDer::from_pem_slice(cert.key_pem.as_bytes()).map_err(|e| fail(&e))?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| fail(&e))?
        }
        None => builder.with_no_client_auth(),
    };
    tls.alpn_protocols = match config.http_version {
        HttpVersion::Auto => vec![b"h2".to_vec(), b"http/1.1".to_vec()],
        HttpVersion::Http1 => vec![b"http/1.1".to_vec()],
        HttpVersion::Http2 => vec![b"h2".to_vec()],
        HttpVersion::Http3 => vec![b"h3".to_vec()],
    };
    if config.insecure_skip_tls_verify {
        tls.dangerous()
            .set_certificate_verifier(Arc::new(AcceptAnyCertificate(provider)));
    }
    Ok(tls)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domains_match_exactly_or_by_wildcard() {
        assert!(domain_matches("Example.com", "example.com"));
        assert!(domain_matches("*.example.com", "api.example.com"));
        assert!(domain_matches("*.example.com", "a.b.example.com"));
        assert!(!domain_matches("*.example.com", "example.com"));
        assert!(!domain_matches("*.example.com", "badexample.com"));
        assert!(!domain_matches("example.com", "api.example.com"));
    }
}
