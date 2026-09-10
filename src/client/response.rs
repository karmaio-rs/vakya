use crate::{
    Error, ErrorKind, Incoming, Response,
    body::pipe::{self, Consumer, Producer, TakeError},
};
use karmaio::runtime::CancellationSource;
use std::{cell::Cell, rc::Rc};

pub(crate) struct Control {
    pub(crate) upload: CancellationSource,
    pub(crate) exchange: CancellationSource,
    pub(crate) upload_complete: Cell<bool>,
    pub(crate) aborted: Cell<bool>,
}

/// Explicit control of an accepted request's upload, including after delivery
/// of its final response. Dropping this handle has no effect.
#[derive(Clone)]
pub struct UploadControl {
    pub(crate) inner: Rc<Control>,
}

impl Control {
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

/// A final-response observation for an accepted request.
///
/// Dropping this before final delivery cancels the exchange and retires the
/// connection. After delivery, the returned body's lifetime controls receiving;
/// the upload continues independently. Submission does not imply retry safety.
#[must_use = "dropping a pending response abandons the exchange"]
pub struct PendingResponse {
    receiver: Consumer<Response<Incoming>, Error>,
    control: UploadControl,
    delivered: bool,
}

impl PendingResponse {
    /// Obtain an upload-abort handle that may outlive final-response delivery.
    pub fn control(&self) -> UploadControl {
        self.control.clone()
    }

    /// Wait for the final response while the connection driver progresses.
    ///
    /// # Errors
    /// Returns the original exchange failure, or `Closed` if its driver is
    /// dropped. Informational response observation is not yet available.
    pub async fn response(mut self) -> Result<Response<Incoming>, Error> {
        match self.receiver.take().await {
            Ok(Some(response)) => {
                self.delivered = true;
                Ok(response)
            }
            Err(TakeError::Failed(error)) => Err(error),
            _ => Err(Error::new(ErrorKind::Closed, "response driver closed")),
        }
    }
}

impl Drop for PendingResponse {
    fn drop(&mut self) {
        if !self.delivered {
            self.control.inner.cancel();
        }
    }
}

pub(crate) fn channel() -> (Producer<Response<Incoming>, Error>, PendingResponse, Rc<Control>) {
    let (sender, receiver) = pipe::channel();
    let control = Rc::new(Control {
        upload: CancellationSource::new(),
        exchange: CancellationSource::new(),
        upload_complete: Cell::new(false),
        aborted: Cell::new(false),
    });
    (
        sender,
        PendingResponse {
            receiver,
            control: UploadControl { inner: control.clone() },
            delivered: false,
        },
        control,
    )
}
