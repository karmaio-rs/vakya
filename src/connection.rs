//! Controls and outcomes of a caller-driven HTTP connection.
use crate::upgrade::Upgraded;
use std::fmt;

/// Ownership returned after all retained HTTP operations have settled.
/// The caller continues driving an upgraded transport using its concrete halves.
pub enum ConnectionOutcome<R, W> {
    /// HTTP completed and the connection was closed.
    Closed,
    /// HTTP transferred the transport and preserved unread protocol bytes.
    Upgraded(Upgraded<R, W>),
}

impl<R, W> fmt::Debug for ConnectionOutcome<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("Closed"),
            Self::Upgraded(io) => f.debug_tuple("Upgraded").field(io).finish(),
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
    stopping: std::cell::Cell<bool>,
    abort: std::cell::Cell<bool>,
    deadline: std::cell::Cell<Option<std::time::Instant>>,
    driver: std::cell::RefCell<Option<std::task::Waker>>,
    admission: std::cell::RefCell<Option<std::task::Waker>>,
    idle: std::cell::RefCell<Option<std::task::Waker>>,
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

    /// Stop new admission and finish accepted work without an implicit deadline.
    /// This never relaxes an earlier deadline or abort request.
    ///
    /// Completion is local: it does not acknowledge the peer's transport teardown
    /// or guarantee that the peer's driver also returns success. Transport errors
    /// observed while closing are still reported by `run()`.
    pub fn graceful_shutdown(&self) {
        self.shared.trace.lifecycle("graceful shutdown requested");
        self.shared.stopping.set(true);
        self.wake();
    }

    /// Request immediate cancellation and settlement of the connection.
    /// `run()` reports `Canceled` unless an originating failure is retained.
    pub fn abort(&self) {
        self.shared.trace.lifecycle("abort requested");
        self.shared.abort.set(true);
        self.graceful_shutdown();
    }

    /// Stop admission now and escalate to abort at this absolute deadline.
    /// Repeated requests retain the earliest deadline. Expiration is reported
    /// as `Timeout` after transport operations settle.
    pub fn graceful_shutdown_with_deadline(&self, deadline: std::time::Instant) {
        self.shared.deadline.set(Some(
            self.shared.deadline.get().map_or(deadline, |old| old.min(deadline)),
        ));
        self.graceful_shutdown();
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
        self.shared.stopping.get()
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

    async fn abort_requested(&self) -> crate::ErrorKind {
        use std::{future::poll_fn, task::Poll};
        loop {
            let deadline = poll_fn(|cx| {
                self.shared.driver.borrow_mut().replace(cx.waker().clone());
                if self.shared.abort.get() {
                    return Poll::Ready(None);
                }
                match self.shared.deadline.get() {
                    Some(deadline) => Poll::Ready(Some(deadline)),
                    None => Poll::Pending,
                }
            })
            .await;

            let Some(deadline) = deadline else {
                return crate::ErrorKind::Canceled;
            };

            let mut timer = std::pin::pin!(karmaio::time::sleep_until(deadline));

            let changed = poll_fn(|cx| {
                self.shared.driver.borrow_mut().replace(cx.waker().clone());
                if self.shared.abort.get() || self.shared.deadline.get() != Some(deadline) {
                    return Poll::Ready(true);
                }
                use std::future::Future;
                timer.as_mut().poll(cx).map(|()| false)
            })
            .await;

            if !changed {
                return crate::ErrorKind::Timeout;
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
    ) -> Result<T, crate::Error>
    where
        F: std::future::Future<Output = Result<T, crate::Error>>,
    {
        use crate::future::{Race, race};
        struct Guard<'a>(&'a ConnectionControl);

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.shared.stopping.set(true);
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
                    _ => Err(crate::Error::new(kind, "connection shutdown requested")),
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
