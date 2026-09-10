use super::Connection;
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
    pub(crate) preferred_read: usize,
    pub(crate) max_retained: usize,
    pub(crate) continue_wait: Option<std::time::Duration>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            protocol: Config::default(),
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
    pub fn continue_wait(&mut self, timeout: Option<std::time::Duration>) -> &mut Self {
        self.continue_wait = timeout;
        self
    }

    /// Create a sender and unboxed driver over established Karmaio I/O.
    /// The caller selects HTTP/1 on negotiated transports and drives `run()`
    /// concurrently with sender operations. No dialing or spawning occurs.
    pub fn handshake<I, B>(
        &self,
        io: I,
    ) -> (
        SendRequest<B>,
        Connection<impl Future<Output = Result<(), Error>> + use<I, B>>,
    )
    where
        I: IntoOwnedSplit,
        B: Body,
        B::Error: std::error::Error + 'static,
    {
        let (sender, receiver) = dispatch::channel();
        (
            sender,
            Connection {
                future: client::run(io, receiver, Portable, self.clone()),
            },
        )
    }

    /// Create a sender and driver using the explicit TCP receive strategy.
    pub fn handshake_tcp<B>(
        &self,
        io: TcpStream,
    ) -> (
        SendRequest<B>,
        Connection<impl Future<Output = Result<(), Error>> + use<B>>,
    )
    where
        B: Body,
        B::Error: std::error::Error + 'static,
    {
        let (sender, receiver) = dispatch::channel();
        (
            sender,
            Connection {
                future: client::run(io, receiver, Tcp, self.clone()),
            },
        )
    }
}
