use super::Connection;
use crate::connection::{ConnectionControl, ConnectionOutcome};
use crate::{
    Body, Error, ErrorKind,
    client::{SendRequest, dispatch},
    io::transport::{Portable, Tcp},
    proto::h1::{client, config::Config},
};
use karmaio::{io::IntoOwnedSplit, net::tcp::TcpStream};
use std::future::Future;

/// Configuration for low-level HTTP/1 client connections.
///
/// Defaults bound heads to 64 KiB/100 fields, retained input to 128 KiB with
/// 16 KiB reads, chunk lines to 8 KiB, and trailers to 16 KiB/32 fields.
#[derive(Clone, Debug)]
pub struct Builder {
    pub(crate) protocol: Config,
    #[cfg(feature = "tls")]
    pub(crate) tls_info: Option<crate::tls::TlsInfo>,
    pub(crate) head_timeout: Option<std::time::Duration>,
    pub(crate) body_progress_timeout: Option<std::time::Duration>,
    pub(crate) write_progress_timeout: Option<std::time::Duration>,
    pub(crate) preferred_read: usize,
    pub(crate) max_retained: usize,
    pub(crate) continue_wait: Option<std::time::Duration>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            protocol: Config::default(),
            #[cfg(feature = "tls")]
            tls_info: None,
            head_timeout: None,
            body_progress_timeout: None,
            write_progress_timeout: None,
            preferred_read: 16 * 1024,
            max_retained: 128 * 1024,
            continue_wait: None,
        }
    }
}

impl Builder {
    /// Create a builder with bounded defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set response-head byte and field limits for this connection.
    ///
    /// # Errors
    /// Returns an error for zero limits. Encoded request heads share the byte limit.
    pub fn head_limits(&mut self, bytes: usize, fields: usize) -> Result<&mut Self, Error> {
        self.protocol.head_limits(bytes, fields)?;
        Ok(self)
    }

    /// Set the maximum informational heads per exchange, including automatic
    /// 100 Continue. Defaults to 16; zero disables informational responses.
    pub fn max_informational(&mut self, count: usize) -> &mut Self {
        self.protocol.max_informational = count;
        self
    }

    /// Set the total deadline for a final response head, including informational responses.
    /// Defaults to disabled; `None` disables it. Incremental bytes do not
    /// reset this budget. The clock starts when the driver awaits the head.
    ///
    /// # Errors
    /// Returns `LocalMessage` if the duration cannot be represented as a deadline.
    /// Rejection preserves the previous setting. Zero is an immediately expired budget.
    pub fn head_timeout(&mut self, timeout: Option<std::time::Duration>) -> Result<&mut Self, Error> {
        crate::io::deadline::configured_after(timeout)?;
        self.head_timeout = timeout;
        Ok(self)
    }

    /// Set the maximum wait for each demanded body read to make peer progress.
    /// Disabled by default. Application pauses without demand are excluded.
    ///
    /// # Errors
    /// Returns `LocalMessage` if the duration cannot be represented as a deadline.
    /// Rejection preserves the previous setting. Zero is an immediately expired budget.
    pub fn body_progress_timeout(&mut self, timeout: Option<std::time::Duration>) -> Result<&mut Self, Error> {
        crate::io::deadline::configured_after(timeout)?;
        self.body_progress_timeout = timeout;
        Ok(self)
    }

    /// Set the maximum wait for each write, flush, or shutdown completion.
    /// Disabled by default. Waiting for the application to produce a body frame
    /// is excluded. Submitted operations are canceled and settled on timeout.
    ///
    /// # Errors
    /// Returns `LocalMessage` if the duration cannot be represented as a deadline.
    /// Rejection preserves the previous setting. Zero is an immediately expired budget.
    pub fn write_progress_timeout(&mut self, timeout: Option<std::time::Duration>) -> Result<&mut Self, Error> {
        crate::io::deadline::configured_after(timeout)?;
        self.write_progress_timeout = timeout;
        Ok(self)
    }

    /// Set incoming chunk-line, trailer-byte, and trailer-field limits.
    ///
    /// # Errors
    /// Returns an error for zero limits. Trailer output shares the byte limit.
    pub fn body_limits(
        &mut self,
        chunk_line: usize,
        trailer_bytes: usize,
        trailer_fields: usize,
    ) -> Result<&mut Self, Error> {
        self.protocol.body_limits(chunk_line, trailer_bytes, trailer_fields)?;
        Ok(self)
    }

    /// Set preferred read size and the maximum initialized input retained by
    /// the parser. Body data is delivered on demand; application-retained data
    /// is outside this limit.
    ///
    /// # Errors
    /// Returns an error for zero or reversed limits.
    pub fn read_buffer_limits(&mut self, preferred: usize, retained: usize) -> Result<&mut Self, Error> {
        if preferred == 0 || preferred > retained {
            return Err(Error::new(
                ErrorKind::LocalMessage,
                "receive limits must be nonzero and ordered",
            ));
        }
        self.preferred_read = preferred;
        self.max_retained = retained;
        Ok(self)
    }

    /// Configure bounded waiting after flushing an Expect: 100-continue request
    /// head. A peer 100, any final response, or timeout permits the upload.
    /// `None` (the default) sends immediately. This does not add an Expect header.
    ///
    /// # Errors
    /// Returns `LocalMessage` if the duration cannot be represented as a deadline.
    /// Rejection preserves the previous setting. Zero is an immediately expired budget.
    pub fn continue_wait(&mut self, timeout: Option<std::time::Duration>) -> Result<&mut Self, Error> {
        crate::io::deadline::configured_after(timeout)?;
        self.continue_wait = timeout;
        Ok(self)
    }

    /// Create a sender and unboxed driver over established Karmaio I/O.
    /// The caller selects HTTP/1 on negotiated transports and drives `run()`
    /// concurrently with sender operations. No dialing or spawning occurs.
    #[allow(clippy::type_complexity)] // Preserve concrete halves and an unboxed future without transport erasure.
    pub fn handshake<I, B>(
        &self,
        io: I,
    ) -> (
        SendRequest<B>,
        Connection<impl Future<Output = Result<ConnectionOutcome<I::ReadHalf, I::WriteHalf>, Error>> + use<I, B>>,
    )
    where
        I: IntoOwnedSplit,
        B: Body,
        B::Error: std::error::Error + 'static,
    {
        let control = ConnectionControl::new();
        let (sender, receiver) = dispatch::channel(control.clone());
        (
            sender,
            Connection {
                future: client::run(io, receiver, Portable, self.clone(), control.clone()),
                control,
            },
        )
    }

    /// Create an HTTP/1 driver over an already-handshaken Karmaio TLS stream.
    /// This performs no TLS handshake. Decrypted input uses portable reads and
    /// responses carry [`crate::tls::TlsInfo`] in their extensions. Handoff
    /// returns the concrete encrypted TLS halves. The underlying transport's
    /// `'static` bound comes from Karmaio splitting; outgoing bodies may borrow.
    ///
    /// # Errors
    /// Rejects negotiated ALPN other than absent or `http/1.1`, before HTTP I/O.
    /// Rejection drops the supplied stream without an HTTP exchange.
    #[cfg(feature = "tls")]
    #[allow(clippy::type_complexity)] // Preserve concrete TLS halves and the unboxed driver.
    pub fn handshake_tls<I, B>(
        &self,
        io: karmaio::tls::ClientTlsStream<I>,
    ) -> Result<
        (
            SendRequest<B>,
            Connection<
                impl Future<
                    Output = Result<
                        ConnectionOutcome<karmaio::tls::ClientTlsReadHalf<I>, karmaio::tls::ClientTlsWriteHalf<I>>,
                        Error,
                    >,
                > + use<I, B>,
            >,
        ),
        Error,
    >
    where
        I: IntoOwnedSplit + 'static,
        B: Body,
        B::Error: std::error::Error + 'static,
    {
        crate::tls::validate_alpn(io.alpn_protocol())?;

        let mut config = self.clone();
        config.tls_info = Some(crate::tls::TlsInfo::new(
            io.alpn_protocol(),
            io.protocol_version(),
            io.negotiated_cipher_suite(),
            io.handshake_kind(),
            None,
        ));

        Ok(config.handshake(io))
    }

    /// Create a sender and driver using the explicit TCP receive strategy.
    /// Linux uses demand-driven managed receives and requires kernel 6.12+ with io_uring enabled, as required by Karmaio.
    /// Other platforms use portable reads. Retained payload leases and partial parser prefixes use bounded
    /// portable fallback. Use `handshake` to select portable I/O explicitly, including on Linux.
    #[allow(clippy::type_complexity)] // Preserve concrete halves and an unboxed future without transport erasure.
    pub fn handshake_tcp<B>(
        &self,
        io: TcpStream,
    ) -> (
        SendRequest<B>,
        Connection<
            impl Future<
                Output = Result<
                    ConnectionOutcome<
                        <TcpStream as IntoOwnedSplit>::ReadHalf,
                        <TcpStream as IntoOwnedSplit>::WriteHalf,
                    >,
                    Error,
                >,
            > + use<B>,
        >,
    )
    where
        B: Body,
        B::Error: std::error::Error + 'static,
    {
        let control = ConnectionControl::new();
        let (sender, receiver) = dispatch::channel(control.clone());
        (
            sender,
            Connection {
                future: client::run(io, receiver, Tcp, self.clone(), control.clone()),
                control,
            },
        )
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn timeout_validation_preserves_configuration_on_rejection() {
        let mut builder = Builder::new();
        macro_rules! check {
            ($method:ident) => {{
                let previous = builder.$method;
                assert_eq!(
                    builder.$method(Some(Duration::MAX)).unwrap_err().kind(),
                    ErrorKind::LocalMessage
                );
                assert_eq!(builder.$method, previous);
                for timeout in [None, Some(Duration::ZERO), Some(Duration::from_secs(1))] {
                    builder.$method(timeout).unwrap();
                    assert_eq!(builder.$method, timeout);
                }
            }};
        }
        check!(head_timeout);
        check!(body_progress_timeout);
        check!(write_progress_timeout);
        check!(continue_wait);
    }
}
