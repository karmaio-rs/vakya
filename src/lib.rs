//! Completion-native HTTP building blocks for Karmaio.
//!
//! Message metadata uses the standard [`http`] types. Vakya targets Karmaio's
//! local execution and owned-buffer model, without requiring `Send` or `Sync`
//! on application work.

#![warn(missing_docs)]

pub mod body;
pub mod error;
pub mod service;

mod future;

#[cfg(feature = "http1")]
mod trace;

pub use body::{
    Body, BodyDataStream, BodyExt, BodyStream, BoxBody, CollectError, Collected, Either, Empty, Frame, Full, Incoming,
    IncomingData, InspectFrame, MapError, MapFrame, SizeHint, StreamBody, TrailerHint, WithTrailers, collect,
};
pub use error::{Error, ErrorKind};
pub use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri, Version};
pub use service::{Service, service_fn};

#[cfg(feature = "http1")]
mod proto;

#[cfg(feature = "http1")]
mod io;

#[cfg(all(test, feature = "http1"))]
#[path = "../tests/support/transport.rs"]
mod test_transport;

#[cfg(feature = "server")]
pub mod server;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "http1")]
pub mod connection;
#[cfg(feature = "http1")]
pub mod upgrade;

#[cfg(feature = "tls")]
pub mod tls;
