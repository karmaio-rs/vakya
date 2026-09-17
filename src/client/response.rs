//! Response observation and upload control for accepted client requests.
use crate::{
    Response,
    body::{
        Incoming,
        pipe::{self, Consumer, Producer, TakeError},
    },
    error::{Error, ErrorKind},
    upgrade::{OnUpgrade, UpgradeSlot},
};
use karmaio::runtime::CancellationSource;
use std::{
    cell::{Cell, RefCell},
    future::poll_fn,
    rc::Rc,
    task::{Poll, Waker},
};

pub(crate) struct Control {
    pub(crate) upload: CancellationSource,
    pub(crate) exchange: CancellationSource,
    pub(crate) upload_complete: Cell<bool>,
    pub(crate) aborted: Cell<bool>,
    continue_allowed: Cell<bool>,
    continue_waker: RefCell<Option<Waker>>,
}

struct ContinueWait<'a>(&'a RefCell<Option<Waker>>);

impl Drop for ContinueWait<'_> {
    fn drop(&mut self) {
        self.0.borrow_mut().take();
    }
}

/// Explicit control of an accepted request's upload, including after delivery
/// of its final response. Dropping this handle has no effect.
#[derive(Clone)]
pub struct UploadControl {
    pub(crate) inner: Rc<Control>,
}

impl Control {
    pub(crate) fn allow_continue(&self) {
        self.continue_allowed.set(true);
        let waker = self.continue_waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(crate) async fn wait_continue(&self) {
        let _guard = ContinueWait(&self.continue_waker);
        poll_fn(|cx| {
            if self.continue_allowed.get() {
                Poll::Ready(())
            } else {
                self.continue_waker.borrow_mut().replace(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    pub(crate) fn cancel(&self) {
        self.exchange.cancel();
        self.upload.cancel();
    }
}

impl UploadControl {
    /// Cancel an unfinished upload and retire its HTTP/1 connection after the
    /// response is settled. This requests cancellation; the driver must keep
    /// running to recover submitted buffers. Completed uploads are unaffected.
    pub fn abort(&self) {
        if !self.inner.upload_complete.get() {
            self.inner.aborted.set(true);
            self.inner.upload.cancel();
        }
    }
}

/// One informational head or the final response of an accepted request.
#[derive(Debug)]
pub enum ResponseEvent {
    /// A bodyless informational response, including peer 100 Continue.
    Informational(Response<()>),
    /// The final response transfers body receiving to its `Incoming` value.
    Final(Response<Incoming>),
}

/// A final-response observation for an accepted request.
///
/// Dropping this before final delivery cancels the exchange and retires the
/// connection. After delivery, the returned body's lifetime controls receiving;
/// the upload continues independently. Submission does not imply retry safety.
#[must_use = "dropping a pending response abandons the exchange"]
pub struct PendingResponse {
    receiver: Consumer<ResponseEvent, Error>,
    control: UploadControl,
    delivered: bool,
    finished: bool,
    upgrade: UpgradeSlot,
}

impl PendingResponse {
    /// Obtain an upload-abort handle that may outlive final-response delivery.
    pub fn control(&self) -> UploadControl {
        self.control.clone()
    }

    /// Claim the transport if the final response completes an HTTP upgrade or tunnel.
    ///
    /// Claim this before awaiting the final response. The future resolves only
    /// after the HTTP driver has settled retained operations and relinquished
    /// the transport. `R` and `W` must match the supplied transport's concrete
    /// halves. A mismatch or repeated claim resolves with `Upgrade`.
    pub fn on_upgrade<R: 'static, W: 'static>(&mut self) -> OnUpgrade<R, W> {
        self.upgrade.claim()
    }

    /// Observe informational heads in wire order, followed by one final response.
    /// Observation is bounded to one offered event and applies backpressure.
    /// After final delivery or failure, later calls return `Ok(None)`.
    /// Dropping this method's future preserves the observation; dropping the
    /// whole pending response before final delivery abandons the exchange.
    ///
    /// # Errors
    /// Returns the original exchange failure, or `Closed` if its driver is dropped.
    pub async fn next_event(&mut self) -> Result<Option<ResponseEvent>, Error> {
        if self.finished {
            return Ok(None);
        }

        match self.receiver.take().await {
            Ok(Some(event)) => {
                if matches!(event, ResponseEvent::Final(_)) {
                    self.delivered = true;
                    self.finished = true;
                }
                Ok(Some(event))
            }
            Err(TakeError::Failed(error)) => {
                self.finished = true;
                Err(error)
            }
            _ => {
                self.finished = true;
                Err(Error::new(ErrorKind::Closed, "response driver closed"))
            }
        }
    }

    /// Discard informational observations and wait for the final response while
    /// the connection driver progresses independently.
    ///
    /// # Errors
    /// Returns an exchange failure, or `Closed` if no final response remains.
    pub async fn response(mut self) -> Result<Response<Incoming>, Error> {
        while let Some(event) = self.next_event().await? {
            if let ResponseEvent::Final(response) = event {
                return Ok(response);
            }
        }

        Err(Error::new(ErrorKind::Closed, "final response already consumed"))
    }

    /// Wait for the final response and return its correlated upgrade future.
    ///
    /// Await the second value only when the final response accepts an upgrade or CONNECT tunnel.
    /// `R` and `W` must match the supplied
    /// transport's concrete halves.
    ///
    /// # Errors
    /// Returns an exchange failure, or `Closed` if no final response remains.
    pub async fn response_with_upgrade<R: 'static, W: 'static>(
        mut self,
    ) -> Result<(Response<Incoming>, OnUpgrade<R, W>), Error> {
        let upgrade = self.on_upgrade();
        while let Some(event) = self.next_event().await? {
            if let ResponseEvent::Final(response) = event {
                return Ok((response, upgrade));
            }
        }

        Err(Error::new(ErrorKind::Closed, "final response already consumed"))
    }
}

impl Drop for PendingResponse {
    fn drop(&mut self) {
        if !self.delivered {
            self.control.inner.cancel();
        }
    }
}

pub(crate) fn channel(upgrade: UpgradeSlot) -> (Producer<ResponseEvent, Error>, PendingResponse, Rc<Control>) {
    let (sender, receiver) = pipe::channel();
    let control = Rc::new(Control {
        upload: CancellationSource::new(),
        exchange: CancellationSource::new(),
        upload_complete: Cell::new(false),
        aborted: Cell::new(false),
        continue_allowed: Cell::new(false),
        continue_waker: RefCell::new(None),
    });

    (
        sender,
        PendingResponse {
            receiver,
            control: UploadControl { inner: control.clone() },
            delivered: false,
            finished: false,
            upgrade,
        },
        control,
    )
}
