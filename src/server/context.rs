//! Request-local coordination between a service and its connection driver.
use crate::{Error, ErrorKind, Response};
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
#[derive(Debug)]
pub struct RequestContext {
    shared: Rc<Shared>,
}

impl RequestContext {
    /// Finish this exchange and close instead of accepting another request.
    /// This does not abort a body or cancel pending I/O. Calls after this
    /// exchange has completed have no effect.
    #[inline]
    pub fn close_connection(&self) {
        self.shared.close.set(true);
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
        if self.shared.closed.get() {
            return Err(closed());
        }
        if self.shared.final_selected.get() {
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
            if self.shared.closed.get() {
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
    final_selected: Cell<bool>,
    closed: Cell<bool>,
    continue_requested: Cell<bool>,
    continue_done: Cell<bool>,
    command: RefCell<CommandState>,
    application: RefCell<Option<Waker>>,
    writer: RefCell<Option<Waker>>,
    receiver: RefCell<Option<Waker>>,
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

pub(crate) fn channel(close: bool) -> (RequestContext, InformationalReceiver) {
    let shared = Rc::new(Shared {
        close: Cell::new(close),
        final_selected: Cell::new(false),
        closed: Cell::new(false),
        continue_requested: Cell::new(false),
        continue_done: Cell::new(false),
        command: RefCell::new(CommandState::Idle),
        application: RefCell::new(None),
        writer: RefCell::new(None),
        receiver: RefCell::new(None),
    });
    (
        RequestContext { shared: shared.clone() },
        InformationalReceiver { shared },
    )
}

impl InformationalReceiver {
    pub(crate) fn close_flag(&self) -> &Cell<bool> {
        &self.shared.close
    }

    /// Stop admission of new heads, reject any unstarted command, and release
    /// Continue demand. A head already being written must still be settled.
    pub(crate) fn select_final(&self) {
        self.shared.final_selected.set(true);
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
        self.shared.continue_requested.set(true);
        wake(&self.shared.writer);
        poll_fn(|cx| {
            if self.shared.closed.get() {
                return Poll::Ready(Err(closed()));
            }
            if self.shared.final_selected.get() || self.shared.continue_done.get() {
                return Poll::Ready(Ok(()));
            }
            self.shared.receiver.borrow_mut().replace(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    pub(crate) fn continued(&self) {
        self.shared.continue_done.set(true);
        wake(&self.shared.receiver);
    }

    pub(crate) async fn next(&self) -> Option<Command> {
        poll_fn(|cx| {
            if self.shared.closed.get() || self.shared.final_selected.get() {
                return Poll::Ready(None);
            }
            if self.shared.continue_requested.get() && !self.shared.continue_done.get() {
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
        self.shared.closed.set(true);
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
    fn canceled_commands_release_queue_but_retain_in_flight_writes() {
        let (mut context, receiver) = channel(false);
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
        let (mut context, receiver) = channel(false);
        let mut send = pin!(context.send_informational(head()));
        assert!(poll(send.as_mut()).is_pending());
        drop(receiver);
        assert!(matches!(poll(send.as_mut()), Poll::Ready(Err(error)) if error.kind()==ErrorKind::Closed));
    }
}
