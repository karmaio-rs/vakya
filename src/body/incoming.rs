use super::{Body, Frame, SizeHint, TrailerHint, frame::Kind, pipe};
use crate::{Error, ErrorKind};
use bytes::Bytes;
use karmaio::buf::IoBuf;
use std::{fmt, marker::PhantomData, ops::RangeBounds, rc::Rc};

pub(crate) type IncomingProducer = pipe::Producer<Frame<IncomingData>, Error>;

/// A streaming body received from an HTTP connection.
///
/// Vakya constructs this after validating a message head. The body owns a local
/// receive endpoint, not a task or transport. The caller must keep driving the
/// connection while consuming it. Dropping an unfinished body abandons that
/// direction; dropping a pending frame wait leaves its bounded demand intact.
/// At most one frame awaits delivery. Retaining yielded data does not retain
/// the body endpoint or prevent later portable reads.
pub struct Incoming {
    consumer: Option<pipe::Consumer<Frame<IncomingData>, Error>>,
    size: SizeHint,
    trailers: TrailerHint,
    trailers_seen: bool,
}

impl Incoming {
    /// Consume and discard the remaining body within an explicit payload limit.
    /// Framing is additionally bounded to the payload limit plus 64 KiB, and a
    /// five-second total deadline covers demand, transport, and framing work.
    /// Successful draining permits connection reuse when the other direction
    /// also settles. Trailers are discarded. Keep driving the connection.
    ///
    /// # Errors
    /// Returns `Limit`, `Timeout`, or the original receive failure. Failure or
    /// dropping this future abandons the body; the driver cancels and settles
    /// pending reads. This operation itself does not wait for driver completion.
    pub async fn drain(mut self, max_payload_bytes: u64) -> Result<(), Error> {
        if let Some(consumer) = &self.consumer {
            consumer.drain_budget(max_payload_bytes);
        }
        let drain = async {
            let mut remaining = max_payload_bytes;
            let mut budget = crate::future::WorkBudget::new();
            while let Some(frame) = self.next_frame().await? {
                budget.step().await;
                if let Ok(data) = frame.into_data() {
                    remaining = remaining
                        .checked_sub(data.len() as u64)
                        .ok_or_else(|| Error::new(ErrorKind::Limit, "body drain payload limit exceeded"))?;
                }
            }
            Ok(())
        };
        karmaio::time::timeout(std::time::Duration::from_secs(5), drain)
            .await
            .map_err(|_| Error::new(ErrorKind::Timeout, "body drain deadline exceeded"))?
    }

    /// Used only when protocol semantics establish that no body frames remain.
    pub(crate) fn empty() -> Self {
        Self {
            consumer: None,
            size: SizeHint::with_exact(0),
            trailers: TrailerHint::None,
            trailers_seen: false,
        }
    }

    /// Even exact zero retains its endpoint: metadata alone is not completion.
    pub(crate) fn channel(size: SizeHint, trailers: TrailerHint) -> (IncomingProducer, Self) {
        let (producer, consumer) = pipe::channel();
        (
            producer,
            Self {
                consumer: Some(consumer),
                size,
                trailers,
                trailers_seen: false,
            },
        )
    }

    fn finish(&mut self) {
        self.consumer = None;
        self.size = SizeHint::with_exact(0);
        self.trailers = TrailerHint::None;
    }

    fn observe(&mut self, frame: &Frame<IncomingData>) -> Result<(), Error> {
        match &frame.kind {
            Kind::Data(data) => {
                let length = data.len() as u64;
                if self.trailers_seen || self.size.upper().is_some_and(|upper| length > upper) {
                    return Err(Error::new(
                        ErrorKind::InvalidMessage,
                        "received body violated its framing metadata",
                    ));
                }
                self.size = SizeHint::with_bounds(
                    self.size.lower().saturating_sub(length),
                    self.size.upper().map(|upper| upper - length),
                )?;
            }
            Kind::Trailers(_) => {
                if self.trailers == TrailerHint::None || self.trailers_seen || self.size.lower() != 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidMessage,
                        "received trailers violated body framing",
                    ));
                }
                self.trailers_seen = true;
                self.trailers = TrailerHint::None;
                self.size = SizeHint::with_exact(0);
            }
        }
        Ok(())
    }
}

impl Body for Incoming {
    type Data = IncomingData;
    type Error = Error;

    async fn next_frame(&mut self) -> Result<Option<Frame<IncomingData>>, Error> {
        let Some(consumer) = &mut self.consumer else {
            return Ok(None);
        };
        let result = match consumer.take().await {
            Ok(Some(frame)) => self.observe(&frame).map(|()| Some(frame)),
            Ok(None) if self.size.lower() != 0 => Err(Error::new(
                ErrorKind::InvalidMessage,
                "received body ended before its declared length",
            )),
            Ok(None) => Ok(None),
            Err(pipe::TakeError::Failed(error)) => Err(error),
            Err(pipe::TakeError::Disconnected) => Err(Error::new(
                ErrorKind::Closed,
                "incoming body driver ended without framing completion",
            )),
        };
        if !matches!(result, Ok(Some(_))) {
            self.finish();
        }
        result
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        self.size
    }

    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        self.trailers
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.consumer.is_none()
    }
}

impl fmt::Debug for Incoming {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Incoming").finish_non_exhaustive()
    }
}

/// An immutable owned segment of received payload.
///
/// Clones and slices share storage without copying. Data stays valid after the
/// body or connection is dropped. This value is local to the Karmaio execution
/// context, leaving room for managed storage without changing its public API.
/// Dropping the last view releases storage; no producer callback is required.
#[derive(Clone)]
pub struct IncomingData {
    bytes: Bytes,
    _local: PhantomData<Rc<()>>,
}

impl IncomingData {
    pub(crate) fn from_bytes(bytes: Bytes) -> Self {
        Self {
            bytes,
            _local: PhantomData,
        }
    }

    /// Returns the length of this payload view.
    #[inline]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns whether this payload view is empty.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Creates an owned subview without copying. Advancing either view leaves
    /// the other unchanged.
    ///
    /// # Panics
    /// Panics if the range is invalid or outside this view.
    #[inline]
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        Self::from_bytes(self.bytes.slice(range))
    }
}

impl AsRef<[u8]> for IncomingData {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl IoBuf for IncomingData {
    #[inline]
    fn as_init(&self) -> &[u8] {
        self.as_ref()
    }
}

impl bytes::Buf for IncomingData {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }
    #[inline]
    fn chunk(&self) -> &[u8] {
        self.as_ref()
    }
    #[inline]
    fn advance(&mut self, count: usize) {
        bytes::Buf::advance(&mut self.bytes, count);
    }
}

impl fmt::Debug for IncomingData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IncomingData")
            .field("length", &self.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        pin::{Pin, pin},
        task::{Context, Poll, Waker},
    };

    fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn exact_zero_does_not_hide_trailers_or_disconnect() {
        let (mut producer, mut incoming) = Incoming::channel(SizeHint::with_exact(0), TrailerHint::MayHave);
        assert!(!incoming.is_end_stream());
        {
            let mut next = pin!(incoming.next_frame());
            assert!(poll(next.as_mut()).is_pending());
        }
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-end", http::HeaderValue::from_static("yes"));
        {
            let mut offer = pin!(producer.offer(Frame::trailers(trailers)));
            assert!(poll(offer.as_mut()).is_pending());
            let mut next = pin!(incoming.next_frame());
            let Poll::Ready(Ok(Some(frame))) = poll(next.as_mut()) else {
                panic!("missing trailers")
            };
            assert_eq!(frame.trailers_ref().unwrap()["x-end"], "yes");
            assert!(matches!(poll(offer.as_mut()), Poll::Ready(Ok(()))));
        }
        assert_eq!(incoming.trailer_hint(), TrailerHint::None);
        assert!(!incoming.is_end_stream());
        drop(producer);
        {
            let mut next = pin!(incoming.next_frame());
            let Poll::Ready(Err(error)) = poll(next.as_mut()) else {
                panic!("disconnect hidden")
            };
            assert_eq!(error.kind(), ErrorKind::Closed);
        }
        assert!(incoming.is_end_stream());
        assert!(matches!(poll(pin!(incoming.next_frame())), Poll::Ready(Ok(None))));
    }

    #[test]
    fn terminal_failure_preserves_original_local_source_and_fuses() {
        #[derive(Debug)]
        struct Local(Rc<()>);
        impl fmt::Display for Local {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("local error")
            }
        }
        impl std::error::Error for Local {}
        let identity = Rc::new(());
        let (producer, mut incoming) = Incoming::channel(SizeHint::new(), TrailerHint::MayHave);
        producer.fail(Error::with_source(
            ErrorKind::Io,
            "receive failed",
            Local(identity.clone()),
        ));
        let Poll::Ready(Err(error)) = poll(pin!(incoming.next_frame())) else {
            panic!("missing error")
        };
        let source = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<Local>()
            .unwrap();
        assert!(Rc::ptr_eq(&identity, &source.0));
        assert!(matches!(poll(pin!(incoming.next_frame())), Poll::Ready(Ok(None))));
    }

    #[test]
    fn payload_views_outlive_endpoints_and_keep_independent_cursors() {
        use bytes::Buf;
        let (mut producer, mut incoming) = Incoming::channel(SizeHint::with_exact(7), TrailerHint::None);
        let bytes = Bytes::from(vec![b'p', b'a', b'y', b'l', b'o', b'a', b'd']);
        let pointer = bytes.as_ptr();
        let data = {
            let mut offer = pin!(producer.offer(Frame::data(IncomingData::from_bytes(bytes))));
            assert!(poll(offer.as_mut()).is_pending());
            let Poll::Ready(Ok(Some(frame))) = poll(pin!(incoming.next_frame())) else {
                panic!("missing payload")
            };
            assert!(matches!(poll(offer.as_mut()), Poll::Ready(Ok(()))));
            frame.into_data().unwrap()
        };
        assert_eq!(incoming.size_hint().exact(), Some(0));
        producer.close();
        drop(incoming);
        let mut clone = data.clone();
        clone.advance(3);
        assert_eq!(clone.as_ref(), b"load");
        assert_eq!(data.as_init().as_ptr(), pointer);
        assert_eq!(data.slice(1..4).as_ref(), b"ayl");
        assert_eq!(data.as_ref(), b"payload");
        assert!(!format!("{data:?}").contains("payload"));
    }
}
