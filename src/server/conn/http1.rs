//! HTTP/1 connections over independently progressing Karmaio I/O halves.
mod builder;
pub(crate) mod config;
pub use builder::Builder;

/// An unboxed, caller-driven HTTP/1 server activity.
///
/// The builder retains concrete I/O, service, and body futures in this type.
/// No task is spawned. Services may borrow local state and need not be `Send`.
/// Dropping the connection preserves Karmaio buffer safety but does not promise
/// graceful flushing or producer recycling.
pub use crate::connection::Connection;

/// A nameable local server connection with one boxed driver future.
/// Concrete transport outcomes and borrowed application lifetimes are retained.
/// No `Send` bound is added; use [`Connection::boxed`] to opt into erasure.
pub use crate::connection::BoxedConnection;
