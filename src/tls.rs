//! TLS metadata for connections over established Karmaio TLS streams.
//!
//! Applications configure Rustls providers, certificates, trust, peer names,
//! and ALPN, then perform the TLS handshake through Karmaio. Vakya's HTTP/1
//! TLS constructors accept absent ALPN or `http/1.1` and use portable decrypted
//! reads. This feature does not select a crypto provider or trust store.

pub use karmaio::tls::rustls;

/// The ALPN identifier used for HTTP/1.1 connections.
pub const HTTP_11_ALPN: &[u8] = b"http/1.1";

/// Negotiated TLS metadata for an HTTP connection.
///
/// The TLS-specific constructors insert this value into the extensions of every request received by a
/// TLS server and every response received by a TLS client. The metadata is a
/// compact snapshot and does not retain the TLS session or certificate chain.
#[derive(Clone, Debug)]
pub struct TlsInfo {
    alpn_http_11: bool,
    protocol_version: Option<rustls::ProtocolVersion>,
    cipher_suite: Option<rustls::CipherSuite>,
    handshake_kind: Option<rustls::HandshakeKind>,
    server_name: Option<Box<str>>,
}

impl TlsInfo {
    #[cfg(any(feature = "client", feature = "server"))]
    pub(crate) fn new(
        alpn_protocol: Option<&[u8]>,
        protocol_version: Option<rustls::ProtocolVersion>,
        cipher_suite: Option<rustls::SupportedCipherSuite>,
        handshake_kind: Option<rustls::HandshakeKind>,
        server_name: Option<&str>,
    ) -> Self {
        Self {
            alpn_http_11: alpn_protocol == Some(HTTP_11_ALPN),
            protocol_version,
            cipher_suite: cipher_suite.map(|suite| suite.suite()),
            handshake_kind,
            server_name: server_name.map(Into::into),
        }
    }

    /// Returns the negotiated ALPN protocol, if the peer selected one.
    ///
    /// Vakya accepts only `http/1.1`, so this is either that identifier or
    /// `None` when the handshake completed without ALPN negotiation.
    #[inline]
    pub fn alpn_protocol(&self) -> Option<&'static [u8]> {
        self.alpn_http_11.then_some(HTTP_11_ALPN)
    }

    /// Returns the negotiated TLS protocol version.
    #[inline]
    pub const fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        self.protocol_version
    }

    /// Returns the negotiated TLS cipher suite.
    #[inline]
    pub const fn cipher_suite(&self) -> Option<rustls::CipherSuite> {
        self.cipher_suite
    }

    /// Returns whether the connection used a full or resumed handshake.
    #[inline]
    pub const fn handshake_kind(&self) -> Option<rustls::HandshakeKind> {
        self.handshake_kind
    }

    /// Returns the server name supplied through SNI on an accepted connection.
    ///
    /// Client-side metadata returns `None`; the application supplies the peer
    /// name when establishing TLS. SNI alone does not authenticate a client.
    #[inline]
    pub fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }
}

#[cfg(any(feature = "client", feature = "server"))]
pub(crate) fn validate_alpn(protocol: Option<&[u8]>) -> Result<(), crate::Error> {
    match protocol {
        None | Some(HTTP_11_ALPN) => Ok(()),
        Some(_) => Err(crate::Error::new(
            crate::ErrorKind::Unsupported,
            "negotiated TLS protocol is not HTTP/1.1",
        )),
    }
}
