use super::Connection;
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
    pub(crate) preferred_read: usize,
    pub(crate) max_retained: usize,
    pub(crate) auto_date: bool,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            protocol: Config::default(),
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
    ///     Builder::new().serve_connection(io, service).run().await
    /// }
    /// ```
    pub fn serve_connection<I, S, B>(
        &self,
        io: I,
        service: S,
    ) -> Connection<impl Future<Output = Result<(), Error>> + use<I, S, B>>
    where
        I: IntoOwnedSplit,
        S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
        S::Error: std::error::Error + 'static,
        B: Body,
        B::Error: std::error::Error + 'static,
    {
        Connection {
            future: server::run(io, service, Portable, self.clone()),
        }
    }

    /// Create a connection from established TCP using the explicit TCP receive
    /// strategy. It currently uses portable reads; managed receive is internal.
    pub fn serve_tcp<S, B>(
        &self,
        io: TcpStream,
        service: S,
    ) -> Connection<impl Future<Output = Result<(), Error>> + use<S, B>>
    where
        S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
        S::Error: std::error::Error + 'static,
        B: Body,
        B::Error: std::error::Error + 'static,
    {
        Connection {
            future: server::run(io, service, Tcp, self.clone()),
        }
    }
}
