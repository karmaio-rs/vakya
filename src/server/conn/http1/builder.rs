use super::Connection;
use crate::connection::{ConnectionControl, ConnectionOutcome};
use crate::{
    Body, Error, ErrorKind, Incoming, Service,
    io::transport::{Portable, Tcp},
    proto::h1::{config::Config, server},
    server::RequestContext,
};
use http::{Request, Response};
use karmaio::{io::IntoOwnedSplit, net::tcp::TcpStream};
use std::future::Future;

/// Configuration for low-level HTTP/1 server connections.
///
/// Defaults bound heads to 64 KiB/100 fields, retained input to 128 KiB with
/// 16 KiB reads, chunk lines to 8 KiB, and trailers to 16 KiB/32 fields.
/// Missing Date headers are inserted where HTTP requires them.
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
    pub(crate) auto_date: bool,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            protocol: Config::default(),
            #[cfg(feature = "tls")]
            tls_info: None,
            head_timeout: Some(std::time::Duration::from_secs(30)),
            body_progress_timeout: None,
            write_progress_timeout: None,
            preferred_read: 16 * 1024,
            max_retained: 128 * 1024,
            auto_date: true,
        }
    }
}

impl Builder {
    /// Create a builder with bounded defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set request-head byte and field limits for this connection.
    ///
    /// # Errors
    /// Returns an error for zero limits. Encoded response heads share the byte limit.
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

    /// Set the total deadline for a request head, including idle keep-alive.
    /// Defaults to 30 seconds; `None` disables it. Incremental bytes do not
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

    /// Enable or disable automatic Date insertion. Supplied values are preserved.
    pub fn auto_date(&mut self, enabled: bool) -> &mut Self {
        self.auto_date = enabled;
        self
    }

    /// Create a connection from established, independently splittable Karmaio I/O.
    /// The caller must have selected HTTP/1 on any negotiated transport.
    /// Generic transports use portable reads; no dialing, accepting, or spawning
    /// occurs here. Services receive `(Request<Incoming>, RequestContext)`.
    /// The connection owns a configuration copy and does not borrow the builder.
    ///
    /// ```
    /// use karmaio::io::IntoOwnedSplit;
    /// use vakya::{Empty, Error, Incoming, Request, Response, service_fn};
    /// use vakya::server::{RequestContext, conn::http1::Builder};
    ///
    /// async fn serve<I: IntoOwnedSplit>(io: I) -> Result<(), Error> {
    ///     let calls = std::cell::Cell::new(0);
    ///     let service = service_fn(async |(_, context): (Request<Incoming>, RequestContext)| {
    ///         calls.set(calls.get() + 1);
    ///         context.close_connection();
    ///         Ok::<_, std::convert::Infallible>(Response::new(Empty::new()))
    ///     });
    ///     Builder::new().serve_connection(io, service).run().await?;
    ///     Ok(())
    /// }
    /// ```
    #[allow(clippy::type_complexity)] // Preserve concrete halves and an unboxed future without transport erasure.
    pub fn serve_connection<I, S, B>(
        &self,
        io: I,
        service: S,
    ) -> Connection<impl Future<Output = Result<ConnectionOutcome<I::ReadHalf, I::WriteHalf>, Error>> + use<I, S, B>>
    where
        I: IntoOwnedSplit,
        S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
        S::Error: std::error::Error + 'static,
        B: Body,
        B::Error: std::error::Error + 'static,
    {
        let control = ConnectionControl::new();
        Connection {
            future: server::run(io, service, Portable, self.clone(), control.clone()),
            control,
        }
    }

    /// Create an HTTP/1 driver over an already-handshaken Karmaio TLS stream.
    /// This performs no TLS handshake. Decrypted input uses portable reads and
    /// requests carry [`crate::tls::TlsInfo`] in their extensions. Handoff
    /// returns the concrete encrypted TLS halves. The underlying transport's
    /// `'static` bound comes from Karmaio splitting; services and bodies may borrow.
    ///
    /// # Errors
    /// Rejects negotiated ALPN other than absent or `http/1.1`, before HTTP I/O.
    /// Rejection drops the supplied stream without an HTTP exchange.
    #[cfg(feature = "tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
    #[allow(clippy::type_complexity)] // Preserve concrete TLS halves and the unboxed driver.
    pub fn serve_tls<I, S, B>(
        &self,
        io: karmaio::tls::ServerTlsStream<I>,
        service: S,
    ) -> Result<
        Connection<
            impl Future<
                Output = Result<
                    ConnectionOutcome<karmaio::tls::ServerTlsReadHalf<I>, karmaio::tls::ServerTlsWriteHalf<I>>,
                    Error,
                >,
            > + use<I, S, B>,
        >,
        Error,
    >
    where
        I: IntoOwnedSplit + 'static,
        S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
        S::Error: std::error::Error + 'static,
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
            io.server_name(),
        ));

        Ok(config.serve_connection(io, service))
    }

    /// Create a connection from established TCP using the explicit TCP receive strategy.
    /// Linux uses demand-driven managed receives and requires kernel 6.12+ with io_uring enabled, as required by Karmaio.
    /// Other platforms use portable reads. Retained payload leases and partial parser prefixes use bounded portable fallback.
    /// Use `serve_connection` to select portable I/O explicitly, including on Linux.
    #[allow(clippy::type_complexity)] // Preserve concrete halves and an unboxed future without transport erasure.
    pub fn serve_tcp<S, B>(
        &self,
        io: TcpStream,
        service: S,
    ) -> Connection<
        impl Future<
            Output = Result<
                ConnectionOutcome<<TcpStream as IntoOwnedSplit>::ReadHalf, <TcpStream as IntoOwnedSplit>::WriteHalf>,
                Error,
            >,
        > + use<S, B>,
    >
    where
        S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
        S::Error: std::error::Error + 'static,
        B: Body,
        B::Error: std::error::Error + 'static,
    {
        let control = ConnectionControl::new();
        Connection {
            future: server::run(io, service, Tcp, self.clone(), control.clone()),
            control,
        }
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
    }
}
