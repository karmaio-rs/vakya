use super::{Body, Frame, SizeHint, TrailerHint, frame::Kind, pipe};
use crate::error::{Error, ErrorKind};
use bytes::Bytes;
use karmaio::buf::IoBuf;
#[cfg(target_os = "linux")]
use karmaio::buf::PooledBuf;
#[cfg(target_os = "linux")]
use std::{
    cell::Cell,
    ops::{Bound, Range},
};
use std::{fmt, marker::PhantomData, ops::RangeBounds, rc::Rc};

pub(crate) type IncomingProducer = pipe::Producer<Frame<IncomingData>, Error>;

#[derive(Clone, Copy, Debug)]
pub(crate) struct DrainConfig {
    pub(crate) timeout: std::time::Duration,
    pub(crate) wire_allowance: u64,
}
impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            timeout: std::time::Duration::from_secs(5),
            wire_allowance: 64 * 1024,
        }
    }
}

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
    drain: DrainConfig,
}

impl Incoming {
    /// Consume and discard the remaining body within an explicit payload limit.
    /// By default, wire bytes are bounded to the payload limit plus 64 KiB and
    /// a five-second total deadline covers demand, transport, and framing work.
    /// The originating HTTP/1 builder can configure these additional budgets.
    /// Successful draining permits connection reuse when the other direction
    /// also settles. Trailers are discarded. Keep driving the connection.
    ///
    /// # Errors
    /// Returns `Limit`, `Timeout`, or the original receive failure. Failure or
    /// dropping this future abandons the body; the driver cancels and settles
    /// pending reads. This operation itself does not wait for driver completion.
    pub async fn drain(mut self, max_payload_bytes: u64) -> Result<(), Error> {
        if let Some(consumer) = &self.consumer {
            consumer.drain_budget(max_payload_bytes, self.drain.wire_allowance);
        }
        let deadline = std::time::Instant::now()
            .checked_add(self.drain.timeout)
            .ok_or_else(|| Error::new(ErrorKind::LocalMessage, "invalid drain timeout duration"))?;
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
        karmaio::time::timeout_at(deadline, drain)
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
            drain: DrainConfig::default(),
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
                drain: DrainConfig::default(),
            },
        )
    }

    #[cfg(any(feature = "client", feature = "server"))]
    pub(crate) fn with_drain_config(mut self, config: DrainConfig) -> Self {
        self.drain = config;
        self
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

/// An immutable owned payload segment from a received HTTP body.
///
/// The private storage may be backed by ordinary [`Bytes`] or a Karmaio-managed receive buffer.
/// Moving or dropping the value preserves or releases that storage correctly;
/// callers do not return received data through [`crate::body::Body::recycle`].
/// Clones and slices share storage without copying.
/// Data remains valid after its body, connection, or runtime is dropped.
/// This type is local to the Karmaio execution context and is not `Send` or `Sync`.
/// Retaining a managed view makes that connection use portable reads until all views of the lease are dropped.
#[derive(Clone)]
pub struct IncomingData {
    storage: IncomingStorage,
    _local: PhantomData<Rc<()>>,
}

#[derive(Clone)]
enum IncomingStorage {
    Bytes(Bytes),
    #[cfg(target_os = "linux")]
    Managed {
        incoming: Rc<ManagedIncoming>,
        range: Range<usize>,
    },
}

#[cfg(target_os = "linux")]
/// Limits one connection to one managed lease retained by application data.
#[derive(Debug)]
pub(crate) struct ManagedLeasePermit {
    held: Cell<bool>,
}

#[cfg(target_os = "linux")]
impl ManagedLeasePermit {
    pub(crate) fn new() -> Self {
        Self { held: Cell::new(false) }
    }

    pub(crate) fn is_held(&self) -> bool {
        self.held.get()
    }
}

#[cfg(target_os = "linux")]
struct ManagedIncoming {
    buffer: PooledBuf,
    permit: Rc<ManagedLeasePermit>,
}

#[cfg(target_os = "linux")]
impl Drop for ManagedIncoming {
    fn drop(&mut self) {
        let was_held = self.permit.held.replace(false);
        debug_assert!(was_held, "managed lease permit was not held");
    }
}

impl IncomingData {
    pub(crate) fn from_bytes(bytes: Bytes) -> Self {
        Self {
            storage: IncomingStorage::Bytes(bytes),
            _local: PhantomData,
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn from_managed(
        buffer: PooledBuf,
        range: Range<usize>,
        permit: Rc<ManagedLeasePermit>,
    ) -> Result<Self, Error> {
        if range.start > range.end || range.end > buffer.len() {
            return Err(Error::new(
                ErrorKind::Internal,
                "managed incoming-data view lies outside initialized bytes",
            ));
        }

        if permit.held.replace(true) {
            return Err(Error::new(
                ErrorKind::Internal,
                "connection attempted to transfer multiple managed leases",
            ));
        }

        Ok(Self {
            storage: IncomingStorage::Managed {
                incoming: Rc::new(ManagedIncoming { buffer, permit }),
                range,
            },
            _local: PhantomData,
        })
    }

    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn managed_strong_count(&self) -> Option<usize> {
        match &self.storage {
            IncomingStorage::Managed { incoming, .. } => Some(Rc::strong_count(incoming)),
            IncomingStorage::Bytes(_) => None,
        }
    }

    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn is_managed(&self) -> bool {
        matches!(&self.storage, IncomingStorage::Managed { .. })
    }

    /// Returns the number of bytes remaining in this payload view.
    #[inline]
    pub const fn len(&self) -> usize {
        match &self.storage {
            IncomingStorage::Bytes(bytes) => bytes.len(),
            #[cfg(target_os = "linux")]
            IncomingStorage::Managed { range, .. } => range.end - range.start,
        }
    }

    /// Returns `true` when this payload view contains no remaining bytes.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an owned view of a subrange without copying its payload bytes.
    ///
    /// Advancing either value changes only that value's local view.
    ///
    /// # Panics
    ///
    /// Panics when the range is invalid or out of bounds.
    #[inline]
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        match &self.storage {
            IncomingStorage::Bytes(bytes) => Self::from_bytes(bytes.slice(range)),
            #[cfg(target_os = "linux")]
            IncomingStorage::Managed {
                incoming,
                range: current,
            } => {
                let range = normalize_range(range, current.end - current.start);
                Self {
                    storage: IncomingStorage::Managed {
                        incoming: Rc::clone(incoming),
                        range: current.start + range.start..current.start + range.end,
                    },
                    _local: PhantomData,
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn normalize_range(range: impl RangeBounds<usize>, len: usize) -> Range<usize> {
    let start = match range.start_bound() {
        Bound::Included(&start) => start,
        Bound::Excluded(&start) => start.checked_add(1).expect("payload range start overflowed"),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&end) => end.checked_add(1).expect("payload range end overflowed"),
        Bound::Excluded(&end) => end,
        Bound::Unbounded => len,
    };

    assert!(start <= end, "payload range end precedes its start");
    assert!(end <= len, "payload range exceeds initialized bytes");

    start..end
}

impl AsRef<[u8]> for IncomingData {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        match &self.storage {
            IncomingStorage::Bytes(bytes) => bytes,
            #[cfg(target_os = "linux")]
            IncomingStorage::Managed { incoming, range } => &incoming.buffer[range.clone()],
        }
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
        match &mut self.storage {
            IncomingStorage::Bytes(bytes) => bytes::Buf::advance(bytes, count),
            #[cfg(target_os = "linux")]
            IncomingStorage::Managed { range, .. } => {
                assert!(
                    count <= range.end - range.start,
                    "cannot advance beyond remaining payload bytes"
                );
                range.start += count;
            }
        }
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
