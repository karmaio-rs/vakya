//! Completion-native HTTP building blocks for Karmaio.
//!
//! Message metadata uses the standard [`http`] types. Vakya targets Karmaio's
//! local execution and owned-buffer model, without requiring `Send` or `Sync`
//! on application work.
//!
//! # Features
//!
//! No features are enabled by default. `client` and `server` each enable
//! `http1`. `tls` adds supplied-TLS-stream integration without selecting a role,
//! crypto provider, or trust store. `tracing` observes lifecycle events, and
//! `full` enables both roles, TLS, and tracing.
//!
//! # Execution and ownership
//!
//! Applications supply established transports and drive the returned connection
//! future concurrently with their service or client operations. Vakya does not
//! spawn a driver, establish TLS, resolve names, pool connections, or retry
//! accepted requests. See the `client` and `server` modules for role entry points.
//!
//! [`body::Body`] uses native async methods and Karmaio-owned buffers. [`body::Incoming`]
//! applies bounded demand; dropping it abandons unfinished receiving. Explicit
//! shutdown and timeout paths settle retained I/O. Dropping a driver preserves
//! runtime buffer safety without promising graceful flush or recycling callbacks.
//!
//! HTTP/1 is implemented; HTTP/2 and HTTP/3 are outside the current API.

#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod body;
pub mod error;
pub mod service;

mod future;

#[cfg(feature = "http1")]
#[cfg_attr(not(any(feature = "client", feature = "server")), allow(dead_code))]
mod trace;

#[doc(no_inline)]
pub use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri, Version};

#[cfg(feature = "http1")]
mod proto;

#[cfg(feature = "http1")]
mod io;

#[cfg(all(test, feature = "http1"))]
#[path = "../tests/support/transport.rs"]
mod test_transport;

#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub mod server;

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub mod client;

#[cfg(feature = "http1")]
#[cfg_attr(docsrs, doc(cfg(feature = "http1")))]
pub mod connection;
#[cfg(feature = "http1")]
#[cfg_attr(docsrs, doc(cfg(feature = "http1")))]
pub mod upgrade;

#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub mod tls;
