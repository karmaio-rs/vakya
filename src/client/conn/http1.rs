//! HTTP/1 connections over independently progressing Karmaio I/O halves.
mod builder;
pub(crate) mod config;
use crate::{
    connection::{ConnectionControl, ConnectionOutcome},
    error::Error,
};
pub use builder::Builder;
use std::{future::Future, pin::Pin};

/// An unboxed, caller-driven HTTP/1 client activity.
///
/// The builder retains concrete I/O and outgoing body types in this driver.
/// No task is spawned. Bodies may borrow local state and need not be `Send`.
/// Dropping the connection preserves Karmaio buffer safety but does not promise
/// graceful flushing or producer recycling.
#[must_use = "the connection makes progress only while run is driven"]
pub struct Connection<F> {
    future: F,
    control: ConnectionControl,
}

/// A nameable local client connection with one boxed driver future.
/// Concrete transport outcomes and borrowed application lifetimes are retained.
/// No `Send` bound is added; use [`Connection::boxed`] to opt into erasure.
///
/// ```compile_fail
/// use vakya::client::conn::http1::BoxedConnection;
/// fn require_send<T: Send>() {}
/// require_send::<BoxedConnection<'static, (), ()>>();
/// ```
pub type BoxedConnection<'a, R, W> =
    Connection<Pin<Box<dyn Future<Output = Result<ConnectionOutcome<R, W>, Error>> + 'a>>>;

impl<R, W, F: Future<Output = Result<ConnectionOutcome<R, W>, Error>>> Connection<F> {
    /// Box this driver once, retaining its control and concrete transport outcome.
    /// The lifetime permits borrowing local bodies and services. No task is spawned.
    pub fn boxed<'a>(self) -> BoxedConnection<'a, R, W>
    where
        F: 'a,
    {
        Connection {
            future: Box::pin(self.future),
            control: self.control,
        }
    }

    /// Obtain a local handle for graceful shutdown or explicit abort.
    pub fn control(&self) -> ConnectionControl {
        self.control.clone()
    }

    /// Drive HTTP and recover concrete halves at a clean terminal boundary.
    ///
    /// This selects recovery before driving starts and avoids local transport
    /// shutdown on clean closure. Pending reads and flushes still settle, and
    /// unread input is preserved. Upgrade outcomes remain unchanged. Protocol,
    /// body, and I/O failures, explicit abort, and unfinished body abandonment
    /// do not return recovered parts. The peer may already have closed its side.
    ///
    /// # Errors
    /// Returns the same originating failures and settled cancellation as [`Self::run`].
    pub fn run_without_shutdown(self) -> F {
        self.control.recover_transport();
        self.future
    }

    /// Drive requests sequentially, with independent request-body writes and
    /// response reads, until closure or error.
    ///
    /// A validated 101 or successful CONNECT returns settled transport halves
    /// in `ConnectionOutcome::Upgraded`, or `UpgradeClaimed` when a correlated
    /// [`crate::upgrade::OnUpgrade`] owns them. Ordinary closure returns `Closed`.
    ///
    /// # Errors
    /// Explicit abort reports `Canceled`; expired deadlines report `Timeout`.
    /// Retained transport operations settle before either is returned.
    /// Returns protocol, body, and transport failures with their sources.
    // Return the already constructed future directly: an extra async wrapper
    // duplicates the entire concrete driver state in unoptimized builds.
    pub fn run(self) -> F {
        self.future
    }
}
