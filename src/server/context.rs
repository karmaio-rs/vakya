//! Request-local coordination between a service and its connection driver.
use crate::{
    Response,
    error::{Error, ErrorKind},
    upgrade::{self, BoxedOnUpgrade, OnUpgrade, PendingUpgrade, UpgradeSlot, UpgradeState, UpgradeStorage},
};
use std::{
    cell::{Cell, RefCell},
    future::poll_fn,
    rc::Rc,
    task::{Poll, Waker},
};

/// Request-local controls, separate from the incoming body's lifetime.
///
/// This handle is not cloneable. Dropping it has no effect on body consumption.
/// It may be retained while producing a response; close requests made after a
/// head was sent affect connection reuse rather than rewriting that head.
pub struct RequestContext {
    shared: Rc<Shared>,
    upgrade: UpgradeSlot,
}

impl std::fmt::Debug for RequestContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestContext").finish_non_exhaustive()
    }
}

impl RequestContext {
    /// Finish this exchange and close instead of accepting another request.
    /// This does not abort a body or cancel pending I/O. Calls after this
    /// exchange has completed have no effect.
    #[inline]
    pub fn close_connection(&self) {
        self.shared.close.set(true);
    }

    /// Claim local upgraded I/O without naming the transport's concrete halves.
    ///
    /// Claim before HTTP hands off ownership. The future resolves only after
    /// retained HTTP operations settle. A duplicate claim fails with `Upgrade`;
    /// dropping the claim before delivery preserves the concrete driver outcome.
    /// Claims after driver retirement or transport transfer fail with `Upgrade`.
    #[inline]
    pub fn on_upgrade(&mut self) -> BoxedOnUpgrade {
        self.upgrade.claim_boxed()
    }

    /// Claim the original concrete transport halves after HTTP settlement.
    /// `R` and `W` must match the supplied transport's owned halves. A mismatch
    /// or duplicate claim fails with `Upgrade` and preserves driver ownership.
    #[inline]
    pub fn on_upgrade_typed<R: 'static, W: 'static>(&mut self) -> OnUpgrade<R, W> {
        self.upgrade.claim()
    }

    /// Write an informational response before the final response is selected.
    /// Completes after the head is written and the transport is flushed.
    /// Automatic 100 Continue and 101 handoff use their own protocol paths.
    ///
    /// Dropping a pending send withdraws a queued head. If writing has started,
    /// the driver settles that write; cancellation cannot retract wire bytes.
    ///
    /// # Errors
    /// Rejects non-informational statuses, 100, 101, late sends, invalid heads,
    /// count/size limits, and transport failures. A canceled in-flight send
    /// occupies the command slot until its write completes (`Limit` meanwhile).
    pub async fn send_informational(&mut self, response: Response<()>) -> Result<(), Error> {
        let status = response.status();
        if !status.is_informational() || status == 100 || status == 101 {
            return Err(Error::new(
                ErrorKind::LocalMessage,
                "application informational status must exclude 100 and 101",
            ));
        }
        if self.shared.admission.get() == Admission::Closed {
            return Err(closed());
        }
        if self.shared.admission.get() == Admission::FinalSelected {
            return Err(late());
        }
        {
            let mut state = self.shared.command.borrow_mut();
            if !matches!(*state, CommandState::Idle) {
                return Err(Error::new(ErrorKind::Limit, "informational write is still settling"));
            }
            *state = CommandState::Queued(response);
        }
        let _guard = SendGuard(&self.shared);
        wake(&self.shared.writer);
        poll_fn(|cx| {
            let mut state = self.shared.command.borrow_mut();
            if matches!(*state, CommandState::Complete(_)) {
                let CommandState::Complete(result) = std::mem::replace(&mut *state, CommandState::Idle) else {
                    unreachable!()
                };
                return Poll::Ready(result);
            }
            if self.shared.admission.get() == Admission::Closed {
                return Poll::Ready(Err(closed()));
            }
            self.shared.application.borrow_mut().replace(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

#[derive(Debug)]
struct Shared {
    close: Cell<bool>,
    admission: Cell<Admission>,
    automatic_continue: Cell<AutomaticContinue>,
    command: RefCell<CommandState>,
    application: RefCell<Option<Waker>>,
    writer: RefCell<Option<Waker>>,
    receiver: RefCell<Option<Waker>>,
    upgrade: RefCell<UpgradeState>,
}

impl UpgradeStorage for Shared {
    fn upgrade_state(&self) -> &RefCell<UpgradeState> {
        &self.upgrade
    }
}

// The service may queue commands while Open. The service-call coordinator
// selects FinalSelected; only dropping the driver endpoint enters Closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Admission {
    Open,
    FinalSelected,
    Closed,
}

// Receive demand requests Continue once; the writer completes it after flush.
// Final selection releases demand without pretending a Continue was written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AutomaticContinue {
    Idle,
    Requested,
    Complete,
}

#[derive(Debug)]
enum CommandState {
    Idle,
    Queued(Response<()>),
    Writing,
    WritingAbandoned,
    Complete(Result<(), Error>),
}

struct SendGuard<'a>(&'a Shared);
impl Drop for SendGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.0.command.borrow_mut();
        *state = match std::mem::replace(&mut *state, CommandState::Idle) {
            CommandState::Writing | CommandState::WritingAbandoned => CommandState::WritingAbandoned,
            _ => CommandState::Idle,
        };
        self.0.application.borrow_mut().take();
    }
}

/// One driver-owned command endpoint shares the existing request-control
/// allocation. Neither queued heads nor acknowledgments allocate another pipe.
pub(crate) struct InformationalReceiver {
    shared: Rc<Shared>,
}
pub(crate) enum Command {
    Continue,
    Application(Response<()>),
}

pub(crate) fn channel(close: bool) -> (RequestContext, InformationalReceiver, PendingUpgrade) {
    let shared = Rc::new(Shared {
        close: Cell::new(close),
        admission: Cell::new(Admission::Open),
        automatic_continue: Cell::new(AutomaticContinue::Idle),
        command: RefCell::new(CommandState::Idle),
        application: RefCell::new(None),
        writer: RefCell::new(None),
        receiver: RefCell::new(None),
        upgrade: RefCell::new(UpgradeState::default()),
    });
    let (upgrade, upgrade_slot) = upgrade::channel(shared.clone());
    (
        RequestContext {
            shared: shared.clone(),
            upgrade: upgrade_slot,
        },
        InformationalReceiver { shared },
        upgrade,
    )
}

impl InformationalReceiver {
    pub(crate) fn close_flag(&self) -> &Cell<bool> {
        &self.shared.close
    }

    /// Stop admission of new heads, reject any unstarted command, and release
    /// Continue demand. A head already being written must still be settled.
    pub(crate) fn select_final(&self) {
        if self.shared.admission.get() == Admission::Open {
            self.shared.admission.set(Admission::FinalSelected);
        }
        {
            let mut state = self.shared.command.borrow_mut();
            if matches!(*state, CommandState::Queued(_)) {
                *state = CommandState::Complete(Err(late()));
            }
        }
        wake(&self.shared.application);
        wake(&self.shared.writer);
        wake(&self.shared.receiver);
    }

    pub(crate) async fn continue_permission(&self) -> Result<(), Error> {
        if self.shared.automatic_continue.get() == AutomaticContinue::Idle {
            self.shared.automatic_continue.set(AutomaticContinue::Requested);
        }
        wake(&self.shared.writer);
        poll_fn(|cx| {
            if self.shared.admission.get() == Admission::Closed {
                return Poll::Ready(Err(closed()));
            }
            if self.shared.admission.get() == Admission::FinalSelected
                || self.shared.automatic_continue.get() == AutomaticContinue::Complete
            {
                return Poll::Ready(Ok(()));
            }
            self.shared.receiver.borrow_mut().replace(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    pub(crate) fn continued(&self) {
        self.shared.automatic_continue.set(AutomaticContinue::Complete);
        wake(&self.shared.receiver);
    }

    pub(crate) async fn next(&self) -> Option<Command> {
        poll_fn(|cx| {
            if self.shared.admission.get() != Admission::Open {
                return Poll::Ready(None);
            }
            if self.shared.automatic_continue.get() == AutomaticContinue::Requested {
                return Poll::Ready(Some(Command::Continue));
            }
            let mut state = self.shared.command.borrow_mut();
            if matches!(*state, CommandState::Queued(_)) {
                let CommandState::Queued(response) = std::mem::replace(&mut *state, CommandState::Writing) else {
                    unreachable!()
                };
                return Poll::Ready(Some(Command::Application(response)));
            }
            self.shared.writer.borrow_mut().replace(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    pub(crate) fn complete(&self, result: Result<(), Error>) {
        {
            let mut state = self.shared.command.borrow_mut();
            *state = match &*state {
                CommandState::Writing => CommandState::Complete(result),
                CommandState::WritingAbandoned => CommandState::Idle,
                _ => unreachable!("only the active writer acknowledges a head"),
            };
        }
        wake(&self.shared.application);
    }
}

impl Drop for InformationalReceiver {
    fn drop(&mut self) {
        self.shared.admission.set(Admission::Closed);
        wake(&self.shared.application);
        wake(&self.shared.receiver);
    }
}
fn wake(slot: &RefCell<Option<Waker>>) {
    let waker = slot.borrow_mut().take();
    if let Some(waker) = waker {
        waker.wake();
    }
}
fn closed() -> Error {
    Error::new(ErrorKind::Closed, "informational writer closed")
}
fn late() -> Error {
    Error::new(ErrorKind::LocalMessage, "final response already selected")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        pin::{Pin, pin},
        task::Context,
    };
    fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }
    fn head() -> Response<()> {
        Response::builder().status(103).body(()).unwrap()
    }

    #[test]
    fn upgrade_endpoints_share_the_existing_server_context_allocation() {
        let (context, receiver, upgrade) = channel(false);
        assert!(upgrade.uses_storage(context.shared.as_ref()));
        assert!(Rc::ptr_eq(&context.shared, &receiver.shared));
    }

    #[test]
    fn continue_completion_is_not_reset_by_repeated_demand() {
        let (_, receiver, _upgrade) = channel(false);
        {
            let mut permission = pin!(receiver.continue_permission());
            assert!(poll(permission.as_mut()).is_pending());
            let mut command = pin!(receiver.next());
            assert!(matches!(poll(command.as_mut()), Poll::Ready(Some(Command::Continue))));
            receiver.continued();
            assert!(matches!(poll(permission.as_mut()), Poll::Ready(Ok(()))));
        }
        let mut permission = pin!(receiver.continue_permission());
        assert!(matches!(poll(permission.as_mut()), Poll::Ready(Ok(()))));
        let mut command = pin!(receiver.next());
        assert!(poll(command.as_mut()).is_pending());
        assert_eq!(receiver.shared.automatic_continue.get(), AutomaticContinue::Complete);
    }

    #[test]
    fn final_selection_releases_continue_without_claiming_it_was_written() {
        let (mut context, receiver, _upgrade) = channel(false);
        {
            let mut permission = pin!(receiver.continue_permission());
            assert!(poll(permission.as_mut()).is_pending());
            receiver.select_final();
            receiver.select_final();
            assert!(matches!(poll(permission.as_mut()), Poll::Ready(Ok(()))));
            assert_eq!(receiver.shared.automatic_continue.get(), AutomaticContinue::Requested);
            let mut command = pin!(receiver.next());
            assert!(matches!(poll(command.as_mut()), Poll::Ready(None)));
        }
        {
            let mut send = pin!(context.send_informational(head()));
            assert!(matches!(poll(send.as_mut()), Poll::Ready(Err(error)) if error.kind() == ErrorKind::LocalMessage));
        }
        drop(receiver);
        assert_eq!(context.shared.admission.get(), Admission::Closed);
        let mut send = pin!(context.send_informational(head()));
        assert!(matches!(poll(send.as_mut()), Poll::Ready(Err(error)) if error.kind() == ErrorKind::Closed));
    }

    #[test]
    fn canceled_commands_release_queue_but_retain_in_flight_writes() {
        let (mut context, receiver, _upgrade) = channel(false);
        {
            let mut send = pin!(context.send_informational(head()));
            assert!(poll(send.as_mut()).is_pending());
        }
        {
            let mut next = pin!(receiver.next());
            assert!(poll(next.as_mut()).is_pending());
        }
        {
            let mut send = pin!(context.send_informational(head()));
            assert!(poll(send.as_mut()).is_pending());
            let mut next = pin!(receiver.next());
            assert!(matches!(
                poll(next.as_mut()),
                Poll::Ready(Some(Command::Application(_)))
            ));
        }
        {
            let mut send = pin!(context.send_informational(head()));
            assert!(matches!(poll(send.as_mut()), Poll::Ready(Err(error)) if error.kind()==ErrorKind::Limit));
        }
        receiver.complete(Ok(()));
        {
            let mut send = pin!(context.send_informational(head()));
            assert!(poll(send.as_mut()).is_pending());
            receiver.select_final();
            assert!(matches!(poll(send.as_mut()), Poll::Ready(Err(error)) if error.kind()==ErrorKind::LocalMessage));
        }
        let (mut context, receiver, _upgrade) = channel(false);
        let mut send = pin!(context.send_informational(head()));
        assert!(poll(send.as_mut()).is_pending());
        drop(receiver);
        assert!(matches!(poll(send.as_mut()), Poll::Ready(Err(error)) if error.kind()==ErrorKind::Closed));
    }
}
