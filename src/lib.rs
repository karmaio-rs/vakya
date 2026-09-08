//! Completion-native HTTP building blocks for Karmaio.
//!
//! Message metadata uses the standard [`http`] types. Vakya targets Karmaio's
//! local execution and owned-buffer model, without requiring `Send` or `Sync`
//! on application work.

#![warn(missing_docs)]

pub mod body;
pub mod error;
pub mod service;

pub use body::{Frame, SizeHint, TrailerHint};
pub use error::{Error, ErrorKind};
pub use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri, Version};
pub use service::{Service, service_fn};
