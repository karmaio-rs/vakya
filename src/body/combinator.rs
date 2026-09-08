use super::{Body, Frame, SizeHint, TrailerHint};
use karmaio::buf::IoBuf;

/// One of two statically dispatched body types.
///
/// The variants may have different data and error types; their outputs use
/// `Either` too. This adds no allocation or dynamic dispatch. Recovered data
/// returns to its originating variant, or is dropped if that variant has been
/// replaced while the frame was outstanding.
#[derive(Clone, Copy, Debug)]
pub enum Either<A, B> {
    /// The first body variant.
    Left(A),
    /// The second body variant.
    Right(B),
}

impl<A> Either<A, A> {
    /// Removes the `Either` wrapper when both variants have the same type.
    #[inline]
    pub fn into_inner(self) -> A {
        match self {
            Self::Left(body) | Self::Right(body) => body,
        }
    }
}

impl<A, B> Body for Either<A, B>
where
    A: Body,
    B: Body,
{
    type Data = Either<A::Data, B::Data>;
    type Error = Either<A::Error, B::Error>;

    async fn next_frame(&mut self) -> Result<Option<Frame<Self::Data>>, Self::Error> {
        match self {
            Self::Left(body) => body
                .next_frame()
                .await
                .map(|frame| map_frame(frame, Either::Left))
                .map_err(Either::Left),
            Self::Right(body) => body
                .next_frame()
                .await
                .map(|frame| map_frame(frame, Either::Right))
                .map_err(Either::Right),
        }
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Left(body) => body.size_hint(),
            Self::Right(body) => body.size_hint(),
        }
    }

    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        match self {
            Self::Left(body) => body.trailer_hint(),
            Self::Right(body) => body.trailer_hint(),
        }
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        match self {
            Self::Left(body) => body.is_end_stream(),
            Self::Right(body) => body.is_end_stream(),
        }
    }

    #[inline]
    fn recycle(&mut self, data: Self::Data) {
        match (self, data) {
            (Self::Left(body), Either::Left(data)) => body.recycle(data),
            (Self::Right(body), Either::Right(data)) => body.recycle(data),
            (_, data) => drop(data),
        }
    }
}

fn map_frame<A, B>(frame: Option<Frame<A>>, map: impl FnOnce(A) -> B) -> Option<Frame<B>> {
    frame.map(|frame| frame.map_data(map))
}

impl<A: IoBuf, B: IoBuf> IoBuf for Either<A, B> {
    #[inline]
    fn as_init(&self) -> &[u8] {
        match self {
            Self::Left(data) => data.as_init(),
            Self::Right(data) => data.as_init(),
        }
    }
}

impl<A: std::fmt::Display, B: std::fmt::Display> std::fmt::Display for Either<A, B> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Left(error) => error.fmt(formatter),
            Self::Right(error) => error.fmt(formatter),
        }
    }
}

impl<A: std::error::Error + 'static, B: std::error::Error + 'static> std::error::Error for Either<A, B> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Left(error) => error,
            Self::Right(error) => error,
        })
    }
}

/// A body that transforms errors from an inner body.
///
/// Frames and recovered data pass through unchanged. The mapping function runs
/// only when the inner body returns an error.
#[derive(Clone, Copy, Debug)]
pub struct MapError<B, F> {
    body: B,
    map: F,
}

impl<B, F> MapError<B, F> {
    /// Creates an error-mapping body wrapper.
    #[inline]
    pub const fn new(body: B, map: F) -> Self {
        Self { body, map }
    }

    /// Returns a shared reference to the inner body.
    #[inline]
    pub const fn get_ref(&self) -> &B {
        &self.body
    }

    /// Returns an exclusive reference to the inner body.
    #[inline]
    pub const fn get_mut(&mut self) -> &mut B {
        &mut self.body
    }

    /// Removes this wrapper and returns the inner body.
    #[inline]
    pub fn into_inner(self) -> B {
        self.body
    }
}

impl<B, F, E> Body for MapError<B, F>
where
    B: Body,
    F: FnMut(B::Error) -> E,
{
    type Data = B::Data;
    type Error = E;

    async fn next_frame(&mut self) -> Result<Option<Frame<Self::Data>>, Self::Error> {
        self.body.next_frame().await.map_err(&mut self.map)
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }

    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        self.body.trailer_hint()
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    #[inline]
    fn recycle(&mut self, data: Self::Data) {
        self.body.recycle(data);
    }
}

#[cfg(test)]
mod tests {
    use super::{Body, Either, Frame, MapError, SizeHint};
    use karmaio::buf::IoBuf;
    use std::{
        cell::Cell,
        future::Future,
        pin::pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    fn complete<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let mut future = pin!(future);
        let mut context = Context::from_waker(Waker::noop());

        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test body unexpectedly returned pending"),
        }
    }

    struct TestBody {
        state: TestState,
        length: SizeHint,
        recycled: Rc<Cell<usize>>,
    }

    enum TestState {
        Data(Vec<u8>),
        Error(&'static str),
        Done,
    }

    impl TestBody {
        fn data(data: &'static [u8], recycled: Rc<Cell<usize>>) -> Self {
            Self {
                state: TestState::Data(data.to_vec()),
                length: SizeHint::with_exact(data.len() as u64),
                recycled,
            }
        }

        fn error(error: &'static str) -> Self {
            Self {
                state: TestState::Error(error),
                length: SizeHint::new(),
                recycled: Rc::new(Cell::new(0)),
            }
        }
    }

    impl Body for TestBody {
        type Data = Vec<u8>;
        type Error = &'static str;

        async fn next_frame(&mut self) -> Result<Option<Frame<Self::Data>>, Self::Error> {
            self.length = SizeHint::with_exact(0);
            match std::mem::replace(&mut self.state, TestState::Done) {
                TestState::Data(data) => Ok(Some(Frame::data(data))),
                TestState::Error(error) => Err(error),
                TestState::Done => Ok(None),
            }
        }

        fn size_hint(&self) -> SizeHint {
            self.length
        }

        fn recycle(&mut self, data: Self::Data) {
            assert!(!data.as_init().is_empty());
            self.recycled.set(self.recycled.get() + 1);
        }
    }

    struct OtherBody(TestBody);

    impl Body for OtherBody {
        type Data = Vec<u8>;
        type Error = &'static str;

        async fn next_frame(&mut self) -> Result<Option<Frame<Self::Data>>, Self::Error> {
            self.0.next_frame().await
        }

        fn size_hint(&self) -> SizeHint {
            self.0.size_hint()
        }

        fn recycle(&mut self, data: Self::Data) {
            self.0.recycle(data);
        }
    }

    #[test]
    fn either_supports_different_buffer_and_error_types() {
        let recycled = Rc::new(Cell::new(0));
        let mut left: Either<TestBody, crate::Full<bytes::Bytes>> =
            Either::Left(TestBody::data(b"left", Rc::clone(&recycled)));
        let data = complete(left.next_frame())
            .unwrap()
            .expect("missing frame")
            .into_data()
            .expect("missing data");
        assert_eq!(data.as_init(), b"left");
        left.recycle(data);
        assert_eq!(recycled.get(), 1);
        let mut right: Either<TestBody, crate::Full<bytes::Bytes>> =
            Either::Right(crate::Full::new(bytes::Bytes::from_static(b"right")));
        let data = complete(right.next_frame())
            .unwrap()
            .expect("missing frame")
            .into_data()
            .expect("missing data");
        assert_eq!(data.as_init(), b"right");
        right.recycle(data);
        let mut failed: Either<TestBody, crate::Empty> = Either::Left(TestBody::error("failure"));
        assert!(matches!(complete(failed.next_frame()), Err(Either::Left("failure"))));
    }

    #[test]
    fn either_left_forwards_frames_length_and_recycling() {
        let recycled = Rc::new(Cell::new(0));
        let mut body: Either<TestBody, OtherBody> = Either::Left(TestBody::data(b"left", Rc::clone(&recycled)));

        assert_eq!(body.size_hint(), SizeHint::with_exact(4));
        let data = complete(body.next_frame())
            .unwrap()
            .expect("missing frame")
            .into_data()
            .expect("missing data");
        body.recycle(data);
        assert_eq!(recycled.get(), 1);
    }

    #[test]
    fn either_right_forwards_frames_length_and_recycling() {
        let recycled = Rc::new(Cell::new(0));
        let mut body: Either<TestBody, OtherBody> =
            Either::Right(OtherBody(TestBody::data(b"right", Rc::clone(&recycled))));

        assert_eq!(body.size_hint(), SizeHint::with_exact(5));
        let data = complete(body.next_frame())
            .unwrap()
            .expect("missing frame")
            .into_data()
            .expect("missing data");
        body.recycle(data);
        assert_eq!(recycled.get(), 1);
    }

    #[test]
    fn map_error_changes_only_the_error() {
        let mut body = MapError::new(TestBody::error("body failed"), |error: &'static str| error.len());

        assert_eq!(body.size_hint(), SizeHint::new());
        assert!(matches!(
            complete(body.next_frame()),
            Err(length) if length == "body failed".len()
        ));
    }

    #[test]
    fn map_error_forwards_recycling() {
        let recycled = Rc::new(Cell::new(0));
        let mut body = MapError::new(TestBody::data(b"data", Rc::clone(&recycled)), |error: &'static str| {
            error
        });

        let data = complete(body.next_frame())
            .unwrap()
            .expect("missing frame")
            .into_data()
            .expect("missing data");
        body.recycle(data);
        assert_eq!(recycled.get(), 1);
    }

    #[test]
    fn composition_preserves_metadata_and_empty_data_is_not_end_of_stream() {
        use crate::{Empty, Full, TrailerHint};

        let body: Either<Full<Vec<u8>>, Empty> = Either::Left(Full::new(Vec::new()));
        let mut mapped = MapError::new(body, |error| error);
        let mut borrowed = &mut mapped;
        assert_eq!(borrowed.size_hint().exact(), Some(0));
        assert_eq!(borrowed.trailer_hint(), TrailerHint::None);
        assert!(!borrowed.is_end_stream());
        let frame = complete(Body::next_frame(&mut borrowed)).unwrap().unwrap();
        let data = frame.into_data().unwrap();
        assert!(data.as_init().is_empty());
        borrowed.recycle(data);
        assert!(borrowed.is_end_stream());
        assert!(complete(borrowed.next_frame()).unwrap().is_none());
    }
}
