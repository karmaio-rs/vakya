//! Low-level HTTP servers over established Karmaio transports.
//!
//! Applications own listeners, admission, and task supervision. Vakya owns the
//! HTTP exchange while the caller drives the returned connection.
pub mod conn;
pub(crate) mod context;
pub use context::RequestContext;

mod request_service;
pub use request_service::{RequestServiceFn, request_service_fn};
