//! Controls and outcomes of a caller-driven HTTP connection.
use crate::error::{Error, ErrorDetail, ErrorKind, TimeoutPhase};
use crate::upgrade::{UpgradeKind, Upgraded};
use std::fmt;

/// Concrete transport halves and unconsumed read-ahead after HTTP settlement.
/// Recovery avoids local shutdown; it does not promise that the peer kept the
/// transport open. Received bytes from a canceled idle read are preserved.
pub struct ConnectionParts<R, W> {
    /// The original settled read half.
    pub reader: R,
    /// The original settled write half.
    pub writer: W,
    /// Unconsumed bytes received beyond the last admitted HTTP message.
    pub read_ahead: bytes::Bytes,
}

impl<R, W> fmt::Debug for ConnectionParts<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionParts")
            .field("read_ahead_bytes", &self.read_ahead.len())
            .finish_non_exhaustive()
    }
}

/// Ownership returned after all retained HTTP operations have settled.
/// The caller continues driving an upgraded transport using its concrete halves.
pub enum ConnectionOutcome<R, W> {
    /// HTTP completed and the connection was closed.
    Closed,
    /// HTTP retired cleanly without local shutdown, returning settled transport.
    Recovered(ConnectionParts<R, W>),
    /// HTTP transferred the transport and preserved unread protocol bytes.
    Upgraded(Upgraded<R, W>),
    /// HTTP transferred the transport to a claimed [`crate::upgrade::OnUpgrade`].
    UpgradeClaimed(UpgradeKind),
}

impl<R, W> fmt::Debug for ConnectionOutcome<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("Closed"),
            Self::Recovered(parts) => f.debug_tuple("Recovered").field(parts).finish(),
            Self::Upgraded(io) => f.debug_tuple("Upgraded").field(io).finish(),
            Self::UpgradeClaimed(kind) => f.debug_tuple("UpgradeClaimed").field(kind).finish(),
        }
    }
}

/// A cloneable local control handle for a caller-driven connection.
///
/// Requests take effect while `run()` is driven. Dropping this handle does not
/// stop the connection. Graceful shutdown completes accepted work; abort drops
/// application waits but cancels and awaits retained transport operations.
/// Custom transports must cooperate with Karmaio cancellation to finish.
#[derive(Clone, Debug)]
pub struct ConnectionControl {
    shared: std::rc::Rc<ControlState>,
}

#[derive(Debug, Default)]
struct ControlState {
    trace: crate::trace::Scope,
    shutdown: std::cell::Cell<Shutdown>,
    recover_transport: std::cell::Cell<bool>,
    driver: std::cell::RefCell<Option<std::task::Waker>>,
    admission: std::cell::RefCell<Option<std::task::Waker>>,
    idle: std::cell::RefCell<Option<std::task::Waker>>,
}

/// Control handles request monotonic shutdown transitions. The driver escalates
/// an expired graceful deadline to Aborted and stops admission on release.
/// This state requests work; only the retained driver future certifies settlement.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum Shutdown {
    #[default]
    Running,
    Graceful {
        deadline: Option<std::time::Instant>,
    },
    Aborted,
}

impl Shutdown {
    fn graceful(self, requested: Option<std::time::Instant>) -> Self {
        match self {
            Self::Aborted => Self::Aborted,
            Self::Running => Self::Graceful { deadline: requested },
            Self::Graceful { deadline } => Self::Graceful {
                deadline: match (deadline, requested) {
                    (Some(old), Some(new)) => Some(old.min(new)),
                    (old, new) => old.or(new),
                },
            },
        }
    }
}

#[cfg_attr(not(any(feature = "client", feature = "server")), allow(dead_code))]
impl ConnectionControl {
    pub(crate) fn new(role: &'static str) -> Self {
        Self {
            shared: std::rc::Rc::new(ControlState {
                trace: crate::trace::Scope::connection(role),
                ..ControlState::default()
            }),
        }
    }

    // Set only by consuming Connection::run_without_shutdown before driving.
    pub(crate) fn recover_transport(&self) {
        self.shared.recover_transport.set(true);
    }

    pub(crate) fn recovering(&self) -> bool {
        self.shared.recover_transport.get()
    }

    /// Stop new admission and finish accepted work without an implicit deadline.
    /// This never relaxes an earlier deadline or abort request.
    ///
    /// Completion is local: it does not acknowledge the peer's transport teardown
    /// or guarantee that the peer's driver also returns success. Transport errors
    /// observed while closing are still reported by `run()`.
    pub fn graceful_shutdown(&self) {
        self.shared.trace.lifecycle("graceful shutdown requested");
        self.shared.shutdown.set(self.shared.shutdown.get().graceful(None));
        self.wake();
    }

    /// Request immediate cancellation and settlement of the connection.
    /// `run()` reports `Canceled` unless an originating failure is retained.
    pub fn abort(&self) {
        self.shared.trace.lifecycle("abort requested");
        self.shared.shutdown.set(Shutdown::Aborted);
        self.wake();
    }

    /// Stop admission now and escalate to abort at this absolute deadline.
    /// Repeated requests retain the earliest deadline. Expiration is reported
    /// as `Timeout` after transport operations settle.
    pub fn graceful_shutdown_with_deadline(&self, deadline: std::time::Instant) {
        self.shared.trace.lifecycle("graceful shutdown requested");
        self.shared
            .shutdown
            .set(self.shared.shutdown.get().graceful(Some(deadline)));
        self.wake();
    }

    fn wake(&self) {
        for slot in [&self.shared.driver, &self.shared.admission, &self.shared.idle] {
            let wake = slot.borrow_mut().take();
            if let Some(wake) = wake {
                wake.wake();
            }
        }
    }

    pub(crate) fn stopping(&self) -> bool {
        self.shared.shutdown.get() != Shutdown::Running
    }

    #[cfg(feature = "client")]
    pub(crate) fn admission_waker(&self, waker: &std::task::Waker) {
        self.shared.admission.borrow_mut().replace(waker.clone());
    }

    #[cfg(feature = "server")]
    pub(crate) async fn stopped(&self) {
        std::future::poll_fn(|cx| {
            if self.stopping() {
                return std::task::Poll::Ready(());
            }
            self.shared.idle.borrow_mut().replace(cx.waker().clone());
            std::task::Poll::Pending
        })
        .await
    }

    async fn abort_requested(&self) -> ErrorKind {
        use std::{future::poll_fn, task::Poll};
        loop {
            let deadline = poll_fn(|cx| {
                self.shared.driver.borrow_mut().replace(cx.waker().clone());
                match self.shared.shutdown.get() {
                    Shutdown::Aborted => Poll::Ready(None),
                    Shutdown::Graceful {
                        deadline: Some(deadline),
                    } => Poll::Ready(Some(deadline)),
                    Shutdown::Running | Shutdown::Graceful { deadline: None } => Poll::Pending,
                }
            })
            .await;

            let Some(deadline) = deadline else {
                return ErrorKind::Canceled;
            };

            let mut timer = std::pin::pin!(karmaio::time::sleep_until(deadline));

            let changed = poll_fn(|cx| {
                self.shared.driver.borrow_mut().replace(cx.waker().clone());
                if self.shared.shutdown.get()
                    != (Shutdown::Graceful {
                        deadline: Some(deadline),
                    })
                {
                    return Poll::Ready(true);
                }
                use std::future::Future;
                timer.as_mut().poll(cx).map(|()| false)
            })
            .await;

            if !changed {
                self.shared.shutdown.set(Shutdown::Aborted);
                self.wake();
                return ErrorKind::Timeout;
            }
        }
    }

    pub(crate) fn instrument<F: std::future::Future>(
        &self,
        future: F,
    ) -> impl std::future::Future<Output = F::Output> + use<F> {
        self.shared.trace.instrument(future)
    }

    pub(crate) async fn drive<F, T>(
        &self,
        source: &karmaio::runtime::CancellationSource,
        future: std::pin::Pin<&mut F>,
    ) -> Result<T, Error>
    where
        F: std::future::Future<Output = Result<T, Error>>,
    {
        use crate::future::{Race, race};
        struct Guard<'a>(&'a ConnectionControl);

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.shared.shutdown.set(self.0.shared.shutdown.get().graceful(None));
                self.0.wake();
                self.0.shared.trace.lifecycle("driver released");
            }
        }

        let _guard = Guard(self);

        self.shared.trace.lifecycle("driver started");
        let mut future = std::pin::pin!(future);
        let mut abort = std::pin::pin!(self.abort_requested());

        let result = match race(abort.as_mut(), future.as_mut()).await {
            Race::Second(result) => result,
            Race::First(kind) => {
                self.shared.trace.lifecycle("canceling retained operations");
                source.cancel();
                let result = future.await;
                match result {
                    Err(error) if !error.is_canceled() => Err(error),
                    _ => Err(Error::new(kind, "connection shutdown requested").classified(
                        (kind == ErrorKind::Timeout).then_some(ErrorDetail::Timeout(TimeoutPhase::Shutdown)),
                    )),
                }
            }
        };

        if let Err(error) = &result {
            self.shared.trace.failure(error.kind());
        } else {
            self.shared.trace.lifecycle("driver settled");
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn shutdown_requests_preserve_the_earliest_deadline_and_abort() {
        let control = ConnectionControl::new("test");
        let early = Instant::now() + Duration::from_secs(1);
        let late = early + Duration::from_secs(1);
        assert!(!control.stopping());
        control.graceful_shutdown();
        assert!(control.stopping());
        assert_eq!(control.shared.shutdown.get(), Shutdown::Graceful { deadline: None });
        control.graceful_shutdown_with_deadline(late);
        control.graceful_shutdown_with_deadline(early);
        control.graceful_shutdown();
        control.graceful_shutdown_with_deadline(late);
        assert_eq!(
            control.shared.shutdown.get(),
            Shutdown::Graceful { deadline: Some(early) }
        );
        control.abort();
        control.graceful_shutdown();
        control.graceful_shutdown_with_deadline(early);
        assert_eq!(control.shared.shutdown.get(), Shutdown::Aborted);
    }
}
