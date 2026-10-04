//! HTTP/1 connections over independently progressing Karmaio I/O halves.
mod builder;
pub(crate) mod config;
pub use builder::Builder;

/// An unboxed, caller-driven HTTP/1 client activity.
///
/// The builder retains concrete I/O and outgoing body types in this driver.
/// No task is spawned. Bodies may borrow local state and need not be `Send`.
/// Dropping the connection preserves Karmaio buffer safety but does not promise
/// graceful flushing or producer recycling.
pub use crate::connection::Connection;

/// A nameable local client connection with one boxed driver future.
/// Concrete transport outcomes and borrowed application lifetimes are retained.
/// No `Send` bound is added; use [`Connection::boxed`] to opt into erasure.
///
/// ```compile_fail
/// use vakya::client::conn::http1::BoxedConnection;
/// fn require_send<T: Send>() {}
/// require_send::<BoxedConnection<'static, (), ()>>();
/// ```
pub use crate::connection::BoxedConnection;
