use super::{Body, SizeHint, TrailerHint, frame::Kind};
use crate::future::WorkBudget;
use bytes::Bytes;
use http::HeaderMap;
use karmaio::buf::IoBuf;
use std::{collections::TryReserveError, error::Error as StdError, fmt};

/// Collects remaining body data up to an explicit payload-byte limit.
///
/// Data is copied into contiguous storage, then immediately returned through
/// [`Body::recycle`]. Trailers are retained separately. The byte limit covers
/// payload only, not the producer's storage or trailer metadata; HTTP receivers
/// enforce their own trailer limits. Allocation requests stay within the
/// payload limit, although allocator bookkeeping and rounding are external.
///
/// Initial size bounds and trailer capability are validated against produced
/// frames. A lower bound above the limit rejects the body without polling it.
/// Collection yields cooperatively between bounded groups of ready frames.
///
/// # Errors
///
/// Returns a typed error for producer failure, invalid metadata/ordering,
/// excess payload, overflow, or allocation failure. No further frames are
/// requested after an error. Any data frame already obtained is recycled.
///
/// Dropping the collection future discards the partial result and may cancel
/// a pending producer call. It does not promise that collection can be retried
/// without losing data. No yielded frame is held across an await.
///
/// ```
/// use vakya::body::{Full, collect};
///
/// async fn example() -> Result<(), Box<dyn std::error::Error>> {
///     let mut body = Full::new(b"hello".to_vec());
///     let collected = collect(&mut body, 5).await?;
///     assert_eq!(collected.as_ref(), b"hello");
///     Ok(())
/// }
/// ```
pub async fn collect<B: Body + ?Sized>(body: &mut B, max_bytes: usize) -> Result<Collected, CollectError<B::Error>> {
    let hint = body.size_hint();
    let trailer_hint = body.trailer_hint();
    let max_length = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if hint.lower() > max_length {
        return Err(CollectError::LimitExceeded { limit: max_bytes });
    }
    let initial_capacity = hint.exact().map_or(Ok(0), |length| {
        usize::try_from(length).map_err(|_| CollectError::LengthOverflow)
    })?;
    let mut data = Vec::new();
    data.try_reserve_exact(initial_capacity)
        .map_err(CollectError::Allocation)?;
    let mut trailers = None;
    let mut budget = WorkBudget::new();

    loop {
        budget.step().await;
        let Some(frame) = body.next_frame().await.map_err(CollectError::Body)? else {
            if (data.len() as u64) < hint.lower() {
                return Err(CollectError::LengthMismatch);
            }
            return Ok(Collected {
                data: Bytes::from(data),
                trailers,
            });
        };
        match frame.kind {
            Kind::Data(frame) => {
                // Finish processing and reclaim the owned frame before any await.
                let result = append(&mut data, frame.as_init(), max_bytes, hint, trailers.is_some());
                body.recycle(frame);
                result?;
            }
            Kind::Trailers(fields) => {
                if trailers.is_some() {
                    return Err(CollectError::InvalidSequence);
                }
                if trailer_hint == TrailerHint::None {
                    return Err(CollectError::UnexpectedTrailers);
                }
                trailers = Some(fields);
            }
        }
    }
}

fn append<E>(
    data: &mut Vec<u8>,
    frame: &[u8],
    limit: usize,
    hint: SizeHint,
    after_trailers: bool,
) -> Result<(), CollectError<E>> {
    if after_trailers {
        return Err(CollectError::InvalidSequence);
    }
    let new_len = checked_total(data.len(), frame.len(), limit)?;
    if hint.upper().is_some_and(|upper| (new_len as u64) > upper) {
        return Err(CollectError::LengthMismatch);
    }
    reserve_for_append(data, new_len, limit).map_err(CollectError::Allocation)?;
    data.extend_from_slice(frame);
    Ok(())
}

fn reserve_for_append(data: &mut Vec<u8>, new_len: usize, limit: usize) -> Result<(), TryReserveError> {
    if new_len <= data.capacity() {
        return Ok(());
    }

    let target = data.capacity().saturating_mul(2).max(new_len).min(limit);
    data.try_reserve_exact(target - data.len())
}

/// A bounded body collected into contiguous bytes and optional trailers.
#[derive(Debug)]
pub struct Collected {
    data: Bytes,
    trailers: Option<HeaderMap>,
}

impl Collected {
    /// Returns the collected body bytes.
    #[inline]
    pub const fn bytes(&self) -> &Bytes {
        &self.data
    }

    /// Returns the received trailers, when the body supplied them.
    #[inline]
    pub const fn trailers(&self) -> Option<&HeaderMap> {
        self.trailers.as_ref()
    }

    /// Separates the collected bytes and trailers without copying.
    #[inline]
    pub fn into_parts(self) -> (Bytes, Option<HeaderMap>) {
        (self.data, self.trailers)
    }
}

impl AsRef<[u8]> for Collected {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// An error produced while collecting a bounded body.
#[derive(Debug)]
pub enum CollectError<E> {
    /// The source body failed while producing its next frame.
    Body(E),
    /// The collected data would exceed the caller's byte limit.
    LimitExceeded {
        /// The maximum number of bytes accepted by the operation.
        limit: usize,
    },
    /// Aggregate frame lengths overflowed the platform's address space.
    LengthOverflow,
    /// Reserving bounded output storage failed.
    Allocation(TryReserveError),
    /// The body emitted data outside its declared remaining-size bounds.
    LengthMismatch,
    /// The body produced trailers despite promising none.
    UnexpectedTrailers,
    /// The body emitted frames in an invalid order.
    InvalidSequence,
}

impl<E> fmt::Display for CollectError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Body(_) => formatter.write_str("body failed while being collected"),
            Self::LimitExceeded { limit } => write!(formatter, "body exceeds collection limit of {limit} bytes"),
            Self::LengthOverflow => formatter.write_str("collected body length overflowed"),
            Self::Allocation(_) => formatter.write_str("could not allocate bounded body storage"),
            Self::LengthMismatch => formatter.write_str("body length did not match its size hint"),
            Self::UnexpectedTrailers => formatter.write_str("body produced unexpected trailers"),
            Self::InvalidSequence => formatter.write_str("body emitted an invalid frame sequence"),
        }
    }
}

impl<E> StdError for CollectError<E>
where
    E: StdError + 'static,
{
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Body(error) => Some(error),
            Self::Allocation(error) => Some(error),
            Self::LimitExceeded { .. }
            | Self::LengthOverflow
            | Self::InvalidSequence
            | Self::LengthMismatch
            | Self::UnexpectedTrailers => None,
        }
    }
}

#[derive(Debug)]
enum AggregateError {
    Overflow,
    Limit { limit: usize },
}

fn checked_total(current: usize, additional: usize, limit: usize) -> Result<usize, AggregateError> {
    let total = current.checked_add(additional).ok_or(AggregateError::Overflow)?;
    if total > limit {
        return Err(AggregateError::Limit { limit });
    }
    Ok(total)
}

impl<E> From<AggregateError> for CollectError<E> {
    fn from(error: AggregateError) -> Self {
        match error {
            AggregateError::Overflow => Self::LengthOverflow,
            AggregateError::Limit { limit } => Self::LimitExceeded { limit },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AggregateError, CollectError, checked_total, collect, reserve_for_append};
    use crate::body::{Body, Frame, SizeHint, TrailerHint};
    use http::{HeaderMap, HeaderValue, header::CONTENT_TYPE};
    use karmaio::buf::IoBuf;
    use std::{cell::Cell, collections::VecDeque, rc::Rc};

    struct TestData {
        bytes: &'static [u8],
        recycled: Rc<Cell<usize>>,
    }

    impl IoBuf for TestData {
        fn as_init(&self) -> &[u8] {
            self.bytes
        }
    }

    struct TestBody {
        frames: VecDeque<Result<Frame<TestData>, &'static str>>,
        length: SizeHint,
        trailers: TrailerHint,
        recycled: Rc<Cell<usize>>,
        polls: Rc<Cell<usize>>,
    }

    impl TestBody {
        fn new(length: SizeHint, frames: impl IntoIterator<Item = Result<Frame<TestData>, &'static str>>) -> Self {
            Self {
                frames: frames.into_iter().collect(),
                length,
                trailers: TrailerHint::MayHave,
                recycled: Rc::new(Cell::new(0)),
                polls: Rc::new(Cell::new(0)),
            }
        }

        fn data(&self, bytes: &'static [u8]) -> TestData {
            TestData {
                bytes,
                recycled: Rc::clone(&self.recycled),
            }
        }
    }

    impl Body for TestBody {
        type Data = TestData;
        type Error = &'static str;

        async fn next_frame(&mut self) -> Result<Option<Frame<Self::Data>>, Self::Error> {
            self.polls.set(self.polls.get() + 1);
            self.frames.pop_front().transpose()
        }

        fn size_hint(&self) -> SizeHint {
            self.length
        }

        fn trailer_hint(&self) -> TrailerHint {
            self.trailers
        }

        fn recycle(&mut self, data: Self::Data) {
            data.recycled.set(data.recycled.get() + 1);
        }
    }

    #[test]
    fn empty_exact_and_multi_frame_bodies_respect_limits() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut empty = TestBody::new(SizeHint::with_exact(0), []);
            let collected = collect(&mut empty, 0).await.unwrap();
            assert!(collected.as_ref().is_empty());

            let mut body = TestBody::new(SizeHint::with_exact(5), []);
            body.frames.push_back(Ok(Frame::data(body.data(b"ab"))));
            body.frames.push_back(Ok(Frame::data(body.data(b"cde"))));
            let recycled = Rc::clone(&body.recycled);
            let collected = collect(&mut body, 5).await.unwrap();

            assert_eq!(collected.as_ref(), b"abcde");
            assert_eq!(recycled.get(), 2);
        });
    }

    #[test]
    fn trailers_are_returned_separately() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut trailers = HeaderMap::new();
            trailers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
            let mut body = TestBody::new(SizeHint::with_exact(0), [Ok(Frame::trailers(trailers))]);

            let collected = collect(&mut body, 0).await.unwrap();
            assert_eq!(collected.trailers().unwrap()[CONTENT_TYPE], "text/plain");
        });
    }

    #[test]
    fn over_limit_frame_is_recycled_and_stops_polling() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut body = TestBody::new(SizeHint::new(), []);
            body.frames.push_back(Ok(Frame::data(body.data(b"too big"))));
            body.frames.push_back(Err("must not poll"));
            let recycled = Rc::clone(&body.recycled);
            let polls = Rc::clone(&body.polls);

            let error = collect(&mut body, 3).await.unwrap_err();

            assert!(matches!(error, CollectError::LimitExceeded { limit: 3 }));
            assert_eq!(recycled.get(), 1);
            assert_eq!(polls.get(), 1);
        });
    }

    #[test]
    fn declared_over_limit_body_is_not_polled() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut body = TestBody::new(SizeHint::with_exact(8), [Err("must not poll")]);

            assert!(matches!(
                collect(&mut body, 7).await,
                Err(CollectError::LimitExceeded { limit: 7 })
            ));
            assert_eq!(body.polls.get(), 0);
        });
    }

    #[test]
    fn body_error_and_invalid_sequence_are_terminal() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut failed = TestBody::new(SizeHint::new(), [Err("broken")]);
            assert!(matches!(
                collect(&mut failed, 8).await,
                Err(CollectError::Body("broken"))
            ));

            let mut invalid = TestBody::new(SizeHint::new(), []);
            invalid.frames.push_back(Ok(Frame::trailers(HeaderMap::new())));
            invalid.frames.push_back(Ok(Frame::data(invalid.data(b"late"))));
            let recycled = Rc::clone(&invalid.recycled);
            assert!(matches!(
                collect(&mut invalid, 8).await,
                Err(CollectError::InvalidSequence)
            ));
            assert_eq!(recycled.get(), 1);
        });
    }

    #[test]
    fn aggregate_length_checks_overflow_before_the_limit() {
        assert!(matches!(
            checked_total(usize::MAX, 1, usize::MAX),
            Err(AggregateError::Overflow)
        ));
        assert!(matches!(
            checked_total(3, 2, 4),
            Err(AggregateError::Limit { limit: 4 })
        ));
        assert_eq!(checked_total(3, 2, 5).unwrap(), 5);
    }

    #[test]
    fn declared_bounds_are_checked_against_data_and_reclaimed_on_error() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for (hint, expected_polls) in [
                (SizeHint::with_exact(3), 2),
                (SizeHint::with_exact(1), 1),
                (SizeHint::with_bounds(0, Some(1)).unwrap(), 1),
                (SizeHint::with_bounds(3, None).unwrap(), 2),
            ] {
                let mut body = TestBody::new(hint, []);
                body.frames.push_back(Ok(Frame::data(body.data(b"ab"))));
                assert!(matches!(collect(&mut body, 8).await, Err(CollectError::LengthMismatch)));
                assert_eq!(body.recycled.get(), 1);
                assert_eq!(body.polls.get(), expected_polls);
            }
        });
    }

    #[test]
    fn forbidden_and_duplicate_trailers_stop_collection() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut body = TestBody::new(SizeHint::new(), [Ok(Frame::trailers(HeaderMap::new()))]);
            body.trailers = TrailerHint::None;
            assert!(matches!(
                collect(&mut body, 0).await,
                Err(CollectError::UnexpectedTrailers)
            ));
            assert_eq!(body.polls.get(), 1);

            let mut body = TestBody::new(
                SizeHint::new(),
                [
                    Ok(Frame::trailers(HeaderMap::new())),
                    Ok(Frame::trailers(HeaderMap::new())),
                    Err("must not poll"),
                ],
            );
            assert!(matches!(
                collect(&mut body, 0).await,
                Err(CollectError::InvalidSequence)
            ));
            assert_eq!(body.polls.get(), 2);
        });
    }

    #[test]
    fn ready_empty_frames_yield_and_are_reclaimed_before_cancellation() {
        use std::{
            future::Future,
            pin::pin,
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            task::{Context, Wake, Waker},
        };

        #[derive(Default)]
        struct WakeCount(AtomicUsize);
        impl Wake for WakeCount {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let mut body = TestBody::new(SizeHint::with_exact(0), []);
        for _ in 0..128 {
            body.frames.push_back(Ok(Frame::data(body.data(b""))));
        }
        let polls = Rc::clone(&body.polls);
        let recycled = Rc::clone(&body.recycled);
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let mut context = Context::from_waker(&waker);
        {
            let mut collecting = pin!(collect(&mut body, 0));
            assert!(collecting.as_mut().poll(&mut context).is_pending());
            assert!(polls.get() > 0 && polls.get() < 128);
            assert_eq!(recycled.get(), polls.get());
            assert!(wakes.0.load(Ordering::Relaxed) > 0);
        }
        assert_eq!(body.frames.len(), 128 - polls.get());
        assert_eq!(recycled.get(), polls.get());
    }

    #[test]
    fn unknown_length_collection_grows_geometrically_within_limit() {
        let mut data = Vec::new();
        let mut capacity_changes = 0;
        for length in 1..=64 {
            let before = data.capacity();
            reserve_for_append(&mut data, length, 64).unwrap();
            if data.capacity() != before {
                capacity_changes += 1;
            }
            data.push(0);
            assert!(data.capacity() >= data.len());
        }

        assert!(capacity_changes <= 7);
        assert!(data.capacity() <= 64);
    }
}
