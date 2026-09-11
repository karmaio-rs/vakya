//! HTTP/1 connections over independently progressing Karmaio I/O halves.
mod builder;
use crate::{Error, connection::ConnectionOutcome};
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
}

impl<R, W, F: Future<Output = Result<ConnectionOutcome<R, W>, Error>>> Connection<F> {
    /// Drive requests sequentially, with independent request-body reads and
    /// response writes, until closure or error.
    ///
    /// A validated 101 or successful CONNECT returns settled transport halves
    /// in `ConnectionOutcome::Upgraded`; ordinary closure returns `Closed`.
    ///
    /// # Errors
    /// Returns protocol, service, body, and transport failures with their sources.
    pub async fn run(self) -> Result<ConnectionOutcome<R, W>, Error> {
        self.future.await
    }
}
