use std::{future::poll_fn, task::Poll};

/// Bounds consecutive ready iterations, including frames that submit no I/O.
pub(crate) struct WorkBudget(u8);

impl WorkBudget {
    pub(crate) const fn new() -> Self {
        Self(16)
    }

    pub(crate) async fn step(&mut self) {
        if self.0 == 0 {
            let mut first_poll = Some(());
            poll_fn(|context| {
                if first_poll.take().is_some() {
                    context.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            self.0 = 16;
        }
        self.0 -= 1;
    }
}

#[cfg(feature = "http1")]
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Race<A, B> {
    First(A),
    Second(B),
}

/// Polls the first future preferentially without owning or dropping either.
/// Submitted I/O must remain pinned and be driven to completion after a control
/// event wins. A cancellation request alone does not recover its owned buffers.
#[cfg(feature = "http1")]
#[allow(dead_code)] // Used by scoped connection driving in subsequent phases.
pub(crate) async fn race<A: std::future::Future, B: std::future::Future>(
    mut first: std::pin::Pin<&mut A>,
    mut second: std::pin::Pin<&mut B>,
) -> Race<A::Output, B::Output> {
    poll_fn(|cx| {
        if let Poll::Ready(output) = first.as_mut().poll(cx) {
            return Poll::Ready(Race::First(output));
        }
        second.as_mut().poll(cx).map(Race::Second)
    })
    .await
}

/// Cancels a drop-safe control wait. Never use this helper for submitted I/O:
/// unlike `race`, it owns the losing future and drops it on cancellation.
#[cfg(feature = "http1")]
#[allow(dead_code)] // Admission and body-demand waits are integrated later.
pub(crate) async fn cancellable_wait<F: std::future::Future>(
    future: F,
    token: Option<karmaio::runtime::CancellationToken>,
) -> Option<F::Output> {
    let Some(token) = token else { return Some(future.await) };
    let mut canceled = std::pin::pin!(token.cancelled());
    let mut future = std::pin::pin!(future);
    match race(canceled.as_mut(), future.as_mut()).await {
        Race::First(()) => None,
        Race::Second(output) => Some(output),
    }
}

#[cfg(all(test, feature = "http1"))]
mod tests {
    use super::*;
    use crate::test_transport::Gate;
    use karmaio::runtime::CancellationSource;
    use std::{
        cell::Cell,
        future::Future,
        pin::pin,
        task::{Context, Waker},
    };

    #[test]
    fn canceling_control_wait_drops_waiter_and_ready_cancellation_wins() {
        struct Dropped<'a>(&'a Cell<bool>);
        impl Drop for Dropped<'_> {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        karmaio::Runtime::new().unwrap().block_on(async {
            let source = CancellationSource::new();
            let gate = Gate::default();
            let dropped = Cell::new(false);
            let wait = async {
                let _guard = Dropped(&dropped);
                gate.wait().await;
                42
            };
            let mut future = pin!(cancellable_wait(wait, Some(source.token())));
            let mut cx = Context::from_waker(Waker::noop());
            assert!(future.as_mut().poll(&mut cx).is_pending());
            assert!(!dropped.get());
            source.cancel();
            gate.open();
            assert_eq!(future.await, None);
            assert!(dropped.get());
            assert_eq!(cancellable_wait(async { 42 }, None).await, Some(42));
        });
    }
}
