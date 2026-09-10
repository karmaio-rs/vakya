use super::{
    PendingResponse, ResponseEvent,
    response::{self, Control},
};
use crate::{Error, ErrorKind, Incoming, Request, Response, body::pipe::Producer};
use std::{
    cell::RefCell,
    fmt,
    future::poll_fn,
    rc::Rc,
    task::{Poll, Waker},
};

struct State<B> {
    closed: bool,
    busy: bool,
    senders: usize,
    waiting: bool,
    waiter: Option<Waker>,
    driver: Option<Waker>,
    queued: Option<Job<B>>,
}

pub(crate) struct Job<B> {
    pub(crate) request: Request<B>,
    pub(crate) response: Producer<ResponseEvent, Error>,
    pub(crate) control: Rc<Control>,
}

/// A cloneable, local request sender for one connection and outgoing body type.
///
/// One exchange (or unsubmitted permit) may own admission, with at most one
/// waiting reservation. There is no unbounded request queue. No `Send` or
/// `'static` bound is imposed; spawning the driver may impose its own lifetime.
pub struct SendRequest<B> {
    shared: Rc<RefCell<State<B>>>,
}

impl<B> Clone for SendRequest<B> {
    fn clone(&self) -> Self {
        self.shared.borrow_mut().senders += 1;
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<B> Drop for SendRequest<B> {
    fn drop(&mut self) {
        let wake = {
            let mut state = self.shared.borrow_mut();
            state.senders -= 1;
            state.driver.take()
        };
        wake_one(wake);
    }
}

impl<B> SendRequest<B> {
    /// Reserve exclusive admission, waiting for the current exchange to settle.
    /// Dropping this wait unregisters it; dropping its permit releases admission.
    ///
    /// # Errors
    /// Returns `Limit` if another reservation already waits, or `Closed` when
    /// the driver has closed. A waiting reservation has priority over newcomers.
    pub async fn reserve(&self) -> Result<RequestPermit<B>, Error> {
        let mut wait = Reservation {
            shared: &self.shared,
            registered: false,
        };

        poll_fn(|cx| {
            let mut state = wait.shared.borrow_mut();
            if state.closed {
                return Poll::Ready(Err(closed()));
            }
            if !wait.registered && state.waiting {
                return Poll::Ready(Err(Error::new(
                    ErrorKind::Limit,
                    "request reservation already occupied",
                )));
            }
            if !state.busy {
                state.busy = true;
                state.waiting = false;
                state.waiter = None;
                wait.registered = false;
                return Poll::Ready(Ok(RequestPermit {
                    shared: self.shared.clone(),
                    active: true,
                }));
            }
            state.waiting = true;
            state.waiter = Some(cx.waker().clone());
            wait.registered = true;
            Poll::Pending
        })
        .await
    }

    /// Reserve and transfer a request into driver ownership.
    ///
    /// # Errors
    /// Returns the unchanged request if admission fails before ownership transfer.
    ///
    /// # Panics
    /// Panics when submitting outside an active Karmaio runtime.
    pub async fn start_request(&self, request: Request<B>) -> Result<PendingResponse, SubmitError<B>> {
        match self.reserve().await {
            Ok(permit) => permit.send(request),
            Err(error) => Err(SubmitError {
                request: Box::new(request),
                error,
            }),
        }
    }

    /// Submit a request and wait for its final response. Drive the connection
    /// concurrently. Once accepted, failures do not imply that retry is safe.
    ///
    /// # Errors
    /// Admission failures retain the unchanged request in [`SendError::Submission`].
    /// Failures after transfer are returned as [`SendError::Exchange`].
    ///
    /// # Panics
    /// Panics when submitting outside an active Karmaio runtime.
    pub async fn send_request(&self, request: Request<B>) -> Result<Response<Incoming>, SendError<B>> {
        self.start_request(request)
            .await
            .map_err(SendError::Submission)?
            .response()
            .await
            .map_err(SendError::Exchange)
    }
}

struct Reservation<'a, B> {
    shared: &'a Rc<RefCell<State<B>>>,
    registered: bool,
}

impl<B> Drop for Reservation<'_, B> {
    fn drop(&mut self) {
        if self.registered {
            let mut state = self.shared.borrow_mut();
            state.waiting = false;
            state.waiter = None;
        }
    }
}

/// Exclusive admission that transfers a single request when consumed.
#[must_use = "dropping the permit releases admission"]
pub struct RequestPermit<B> {
    shared: Rc<RefCell<State<B>>>,
    active: bool,
}

impl<B> RequestPermit<B> {
    /// Transfer the request to the driver and return its response observation.
    /// No bytes are written by this synchronous ownership transfer.
    ///
    /// # Errors
    /// Returns the unchanged request if the driver closed before submission.
    ///
    /// # Panics
    /// Panics when submitting outside an active Karmaio runtime.
    pub fn send(mut self, request: Request<B>) -> Result<PendingResponse, SubmitError<B>> {
        let mut state = self.shared.borrow_mut();
        if state.closed {
            return Err(SubmitError {
                request: Box::new(request),
                error: closed(),
            });
        }
        let (response, pending, control) = response::channel();

        state.queued = Some(Job {
            request,
            response,
            control,
        });

        self.active = false;

        let wake = state.driver.take();
        drop(state);
        wake_one(wake);

        Ok(pending)
    }
}

impl<B> Drop for RequestPermit<B> {
    fn drop(&mut self) {
        if self.active {
            release(&self.shared);
        }
    }
}

/// A failure before request ownership transfers into the driver.
/// The contained request has not been written by this submission attempt.
pub struct SubmitError<B> {
    // Allocate only on rejected admission, keeping success-path futures small
    // even when the application body type is large.
    request: Box<Request<B>>,
    error: Error,
}

impl<B> SubmitError<B> {
    /// Inspect the admission failure.
    pub fn error(&self) -> &Error {
        &self.error
    }
    /// Recover the original request and failure without cloning either.
    pub fn into_parts(self) -> (Request<B>, Error) {
        (*self.request, self.error)
    }
}
impl<B> fmt::Debug for SubmitError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubmitError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl<B> fmt::Display for SubmitError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}
impl<B> std::error::Error for SubmitError<B> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Distinguishes recoverable admission failure from failure after ownership
/// transfer. An accepted exchange failure never promises that retry is safe.
pub enum SendError<B> {
    /// The request was not submitted and can be recovered unchanged.
    Submission(SubmitError<B>),
    /// The driver accepted the request; some bytes may have reached the peer.
    Exchange(Error),
}

impl<B> SendError<B> {
    /// Inspect the underlying classification and source chain.
    pub fn error(&self) -> &Error {
        match self {
            Self::Submission(error) => error.error(),
            Self::Exchange(error) => error,
        }
    }
}

impl<B> fmt::Debug for SendError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Submission(error) => f.debug_tuple("Submission").field(error).finish(),
            Self::Exchange(error) => f.debug_tuple("Exchange").field(error).finish(),
        }
    }
}

impl<B> fmt::Display for SendError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error().fmt(f)
    }
}

impl<B> std::error::Error for SendError<B> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error())
    }
}

pub(crate) struct Receiver<B> {
    shared: Rc<RefCell<State<B>>>,
}

pub(crate) fn channel<B>() -> (SendRequest<B>, Receiver<B>) {
    let shared = Rc::new(RefCell::new(State {
        closed: false,
        busy: false,
        senders: 1,
        waiting: false,
        waiter: None,
        driver: None,
        queued: None,
    }));

    (SendRequest { shared: shared.clone() }, Receiver { shared })
}

impl<B> Receiver<B> {
    pub(crate) async fn next(&mut self) -> Option<Job<B>> {
        poll_fn(|cx| {
            let mut state = self.shared.borrow_mut();
            if let Some(job) = state.queued.take() {
                return Poll::Ready(Some(job));
            }
            if state.closed || (state.senders == 0 && !state.busy) {
                return Poll::Ready(None);
            }
            state.driver = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    pub(crate) fn release(&mut self) {
        release(&self.shared);
    }
}

impl<B> Drop for Receiver<B> {
    fn drop(&mut self) {
        let (wake, queued) = {
            let mut state = self.shared.borrow_mut();
            state.closed = true;
            state.driver = None;
            (state.waiter.take(), state.queued.take())
        };
        drop(queued);
        wake_one(wake);
    }
}

fn release<B>(shared: &RefCell<State<B>>) {
    let (waiter, driver) = {
        let mut state = shared.borrow_mut();
        state.busy = false;
        (state.waiter.take(), state.driver.take())
    };
    wake_one(waiter);
    wake_one(driver);
}

fn wake_one(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

fn closed() -> Error {
    Error::new(ErrorKind::Closed, "request driver closed")
}
