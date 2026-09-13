use super::{Connection, config::Config};
use crate::connection::{ConnectionControl, ConnectionOutcome};
use crate::{
    Body, Error, ErrorKind, Incoming, Service,
    io::transport::{Portable, Tcp},
    proto::h1::server,
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
#[derive(Clone, Debug, Default)]
pub struct Builder {
    config: Config,
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
        self.config.protocol.head_limits(bytes, fields)?;
        Ok(self)
    }

    /// Set incoming head bytes and field count without changing outgoing limits.
    /// Later calls to `head_limits` set both directions again.
    ///
    /// # Errors
    /// Returns `LocalMessage` for zero limits without changing configuration.
    pub fn incoming_head_limits(&mut self, bytes: usize, fields: usize) -> Result<&mut Self, Error> {
        self.config.protocol.incoming_head_limits(bytes, fields)?;
        Ok(self)
    }

    /// Set encoded head bytes without changing incoming limits.
    ///
    /// # Errors
    /// Returns `LocalMessage` for zero without changing configuration.
    pub fn outgoing_head_limit(&mut self, bytes: usize) -> Result<&mut Self, Error> {
        self.config.protocol.outgoing_head_limit(bytes)?;
        Ok(self)
    }

    /// Set incoming chunk-line, trailer-byte, and trailer-field limits independently.
    /// Later calls to `body_limits` set the trailer byte limit in both directions again.
    ///
    /// # Errors
    /// Returns `LocalMessage` for zero limits without changing configuration.
    pub fn incoming_body_limits(
        &mut self,
        chunk_line: usize,
        trailer_bytes: usize,
        trailer_fields: usize,
    ) -> Result<&mut Self, Error> {
        self.config
            .protocol
            .incoming_body_limits(chunk_line, trailer_bytes, trailer_fields)?;
        Ok(self)
    }

    /// Set encoded trailer bytes without changing incoming limits.
    ///
    /// # Errors
    /// Returns `LocalMessage` for zero without changing configuration.
    pub fn outgoing_trailer_limit(&mut self, bytes: usize) -> Result<&mut Self, Error> {
        self.config.protocol.outgoing_trailer_limit(bytes)?;
        Ok(self)
    }

    /// Set the maximum informational heads per exchange, including automatic
    /// 100 Continue. Defaults to 16; zero disables informational responses.
    pub fn max_informational(&mut self, count: usize) -> &mut Self {
        self.config.protocol.max_informational = count;
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
        self.config.head_timeout = timeout;
        Ok(self)
    }

    /// Set the total budget for discarding an incoming body with `Incoming::drain`.
    /// Defaults to five seconds. Progress does not reset this finite budget.
    /// Zero permits only immediately ready completion.
    ///
    /// # Errors
    /// Returns `LocalMessage` for an unrepresentable deadline without changing configuration.
    pub fn drain_timeout(&mut self, timeout: std::time::Duration) -> Result<&mut Self, Error> {
        crate::io::deadline::configured_after(Some(timeout))?;
        self.config.drain.timeout = timeout;
        Ok(self)
    }

    /// Set extra wire bytes allowed above the payload limit passed to `Incoming::drain`.
    /// Defaults to 64 KiB. Zero is allowed; the combined wire budget saturates at `u64::MAX`.
    /// This bounds framing overhead without changing ordinary body decoding limits.
    pub fn drain_wire_allowance(&mut self, bytes: u64) -> &mut Self {
        self.config.drain.wire_allowance = bytes;
        self
    }

    /// Set the maximum wait for each demanded body read to make peer progress.
    /// Disabled by default. Application pauses without demand are excluded.
    ///
    /// # Errors
    /// Returns `LocalMessage` if the duration cannot be represented as a deadline.
    /// Rejection preserves the previous setting. Zero is an immediately expired budget.
    pub fn body_progress_timeout(&mut self, timeout: Option<std::time::Duration>) -> Result<&mut Self, Error> {
        crate::io::deadline::configured_after(timeout)?;
        self.config.body_progress_timeout = timeout;
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
        self.config.write_progress_timeout = timeout;
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
        self.config
            .protocol
            .body_limits(chunk_line, trailer_bytes, trailer_fields)?;
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
        self.config.preferred_read = preferred;
        self.config.max_retained = retained;
        Ok(self)
    }

    /// Enable or disable automatic Date insertion. Supplied values are preserved.
    pub fn auto_date(&mut self, enabled: bool) -> &mut Self {
        self.config.auto_date = enabled;
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
        let control = ConnectionControl::new("server");
        Connection {
            future: server::run(io, service, Portable, self.config.clone(), control.clone()),
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

        let mut builder = self.clone();
        builder.config.tls_info = Some(crate::tls::TlsInfo::new(
            io.alpn_protocol(),
            io.protocol_version(),
            io.negotiated_cipher_suite(),
            io.handshake_kind(),
            io.server_name(),
        ));

        Ok(builder.serve_connection(io, service))
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
        let control = ConnectionControl::new("server");
        Connection {
            future: server::run(io, service, Tcp, self.config.clone(), control.clone()),
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
                let previous = builder.config.$method;
                assert_eq!(
                    builder.$method(Some(Duration::MAX)).unwrap_err().kind(),
                    ErrorKind::LocalMessage
                );
                assert_eq!(builder.config.$method, previous);
                for timeout in [None, Some(Duration::ZERO), Some(Duration::from_secs(1))] {
                    builder.$method(timeout).unwrap();
                    assert_eq!(builder.config.$method, timeout);
                }
            }};
        }
        let previous = builder.config.drain.timeout;
        assert_eq!(
            builder.drain_timeout(Duration::MAX).unwrap_err().kind(),
            ErrorKind::LocalMessage
        );
        assert_eq!(builder.config.drain.timeout, previous);
        builder.drain_timeout(Duration::ZERO).unwrap();
        assert_eq!(builder.config.drain.timeout, Duration::ZERO);
        check!(head_timeout);
        check!(body_progress_timeout);
        check!(write_progress_timeout);
    }
}
