//! Settled, single-owner observations stored in the exchange's existing control.
use super::response::Control;
use crate::{
    error::{Error, ErrorDetail, ErrorKind},
    upgrade::UpgradeKind,
};
use std::{
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

/// A successful exchange's settled disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExchangeOutcome {
    /// Both directions completed, read-ahead was validated, and admission reopened.
    Reusable,
    /// HTTP admission retired and transport shutdown settled. When the driver
    /// selects `run_without_shutdown`, clean retirement recovers transport instead.
    Closed(CloseReason),
    /// Both directions settled and transport ownership was handed off.
    Upgraded(UpgradeKind),
}

/// The reason a successfully settled exchange retired its connection.
/// When several reasons apply, body abandonment and upload abort take precedence
/// over message policy, which takes precedence over shutdown or sender closure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CloseReason {
    /// HTTP version, framing, or a Connection field requires closure.
    MessagePolicy,
    /// All request senders were dropped before admission could reopen.
    ApplicationClosure,
    /// The application abandoned an unfinished incoming response body.
    BodyAbandonment,
    /// The application explicitly aborted an unfinished request upload.
    UploadAbort,
    /// Graceful connection shutdown stopped admission after accepted work.
    Shutdown,
}

#[derive(Default)]
pub(crate) struct CompletionState {
    taken: bool,
    abandoned: bool,
    result: Option<Result<ExchangeOutcome, Error>>,
    waker: Option<Waker>,
}

/// A single-owner receipt for an accepted request's settled exchange.
///
/// Obtain it once through [`super::PendingResponse::take_completion`], before
/// consuming that observation. Awaiting it neither drives the connection nor
/// consumes the response body. Drive the connection concurrently and consume or
/// abandon the body so receiving can finish. Final-head delivery alone does not
/// certify upload completion or reuse. Dropping a receipt has no cancellation
/// effect. A completed payload may remain application-owned after settlement.
///
/// With `run_without_shutdown`, clean `Closed` describes HTTP admission retirement
/// and the driver returns recovered transport instead of calling shutdown.
///
/// The future reports the originating failure when available. A dropped driver
/// returns `Closed` with [`ErrorDetail::DriverLost`]; this explicitly does not
/// certify I/O settlement. `Reusable` describes admission at publication time,
/// not a promise that a later request or shutdown will succeed.
///
/// Receipts stay local, just like the control allocation they observe.
///
/// ```compile_fail
/// use vakya::client::ExchangeCompletion;
/// fn require_send<T: Send>() {}
/// require_send::<ExchangeCompletion>();
/// ```
#[must_use = "a completion receipt observes settlement only while polled"]
pub struct ExchangeCompletion {
    control: Rc<Control>,
    finished: bool,
}

impl ExchangeCompletion {
    pub(crate) fn take(control: &Rc<Control>) -> Option<Self> {
        let mut state = control.completion.borrow_mut();
        if state.taken {
            return None;
        }
        state.taken = true;
        Some(Self {
            control: control.clone(),
            finished: false,
        })
    }
}

impl Future for ExchangeCompletion {
    type Output = Result<ExchangeOutcome, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.finished, "exchange completion polled after completion");
        let result = {
            let mut state = self.control.completion.borrow_mut();
            if let Some(result) = state.result.take() {
                state.waker = None;
                Some(result)
            } else {
                state.waker = Some(cx.waker().clone());
                None
            }
        };
        if let Some(result) = result {
            self.finished = true;
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ExchangeCompletion {
    fn drop(&mut self) {
        let mut state = self.control.completion.borrow_mut();
        state.abandoned = true;
        state.waker = None;
        state.result = None;
    }
}

/// Travels with the queued job, then the active driver, and finally its outer
/// shutdown composition. Only settled driver paths disarm this guard.
pub(crate) struct CompletionGuard(Option<Rc<Control>>);

impl CompletionGuard {
    pub(crate) fn new(control: Rc<Control>) -> Self {
        Self(Some(control))
    }

    pub(crate) fn finish(mut self, result: Result<ExchangeOutcome, Error>) {
        Self::publish(self.0.take().expect("completion guard owns control"), result);
    }

    fn publish(control: Rc<Control>, result: Result<ExchangeOutcome, Error>) {
        let wake = {
            let mut state = control.completion.borrow_mut();
            if !state.abandoned {
                state.result = Some(result);
            }
            state.waker.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if let Some(control) = self.0.take() {
            Self::publish(
                control,
                Err(Error::new(ErrorKind::Closed, "exchange driver dropped").with_detail(ErrorDetail::DriverLost)),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_receipt_uses_the_existing_control_and_keeps_its_terminal_result() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let (_, mut pending, control, _upgrade) = super::super::response::channel();
            CompletionGuard::new(control.clone()).finish(Ok(ExchangeOutcome::Reusable));
            let receipt = pending.take_completion().unwrap();
            assert!(Rc::ptr_eq(&receipt.control, &control));
            assert!(pending.take_completion().is_none());
            assert_eq!(receipt.await.unwrap(), ExchangeOutcome::Reusable);
        });
    }
}
