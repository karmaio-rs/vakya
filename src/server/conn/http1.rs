//! HTTP/1 connections over independently progressing Karmaio I/O halves.
mod builder;
pub(crate) mod config;
use crate::{
    Error,
    connection::{ConnectionControl, ConnectionOutcome},
};
pub use builder::Builder;
use std::future::Future;

/// An unboxed, caller-driven HTTP/1 server activity.
///
/// The builder retains concrete I/O, service, and body futures in this type.
/// No task is spawned. Services may borrow local state and need not be `Send`.
/// Dropping the connection preserves Karmaio buffer safety but does not promise
/// graceful flushing or producer recycling.
#[must_use = "the connection makes progress only while run is driven"]
pub struct Connection<F> {
    future: F,
    control: ConnectionControl,
}

impl<R, W, F: Future<Output = Result<ConnectionOutcome<R, W>, Error>>> Connection<F> {
    /// Obtain a local handle for graceful shutdown or explicit abort.
    pub fn control(&self) -> ConnectionControl {
        self.control.clone()
    }

    /// Drive requests sequentially, with independent request-body reads and
    /// response writes, until closure or error.
    ///
    /// A validated 101 or successful CONNECT returns settled transport halves
    /// in `ConnectionOutcome::Upgraded`; ordinary closure returns `Closed`.
    ///
    /// # Errors
    /// Explicit abort reports `Canceled`; expired deadlines report `Timeout`.
    /// Retained transport operations settle before either is returned.
    /// Returns protocol, service, body, and transport failures with their sources.
    // Return the already constructed future directly: an extra async wrapper
    // duplicates the entire concrete driver state in unoptimized builds.
    pub fn run(self) -> F {
        self.future
    }
}
