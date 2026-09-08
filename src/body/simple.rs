use super::{Body, Frame, SizeHint, TrailerHint};
use bytes::Bytes;
use karmaio::buf::IoBuf;
use std::convert::Infallible;

/// A body that contains no frames and has an exact length of zero.
#[derive(Clone, Copy, Debug, Default)]
pub struct Empty;

impl Empty {
    /// Creates an empty body.
    #[inline]
    pub const fn new() -> Self {
        Self
    }
}

impl Body for Empty {
    type Data = Bytes;
    type Error = Infallible;

    #[inline]
    async fn next_frame(&mut self) -> Result<Option<Frame<Self::Data>>, Self::Error> {
        Ok(None)
    }

    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        TrailerHint::None
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        true
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(0)
    }
}

/// A body that yields one owned data buffer.
///
/// `Full` does not allocate beyond the supplied buffer. After yielding that
/// buffer once, all later calls to [`Body::next_frame`] return `Ok(None)`.
#[derive(Debug)]
pub struct Full<D> {
    state: FullState<D>,
}

#[derive(Debug)]
enum FullState<D> {
    Ready(D),
    Done,
}

impl<D> Full<D>
where
    D: IoBuf,
{
    /// Creates a body that yields `data` as its only frame.
    #[inline]
    pub fn new(data: D) -> Self {
        Self {
            state: FullState::Ready(data),
        }
    }
}

impl<D> Body for Full<D>
where
    D: IoBuf,
{
    type Data = D;
    type Error = Infallible;

    async fn next_frame(&mut self) -> Result<Option<Frame<Self::Data>>, Self::Error> {
        match std::mem::replace(&mut self.state, FullState::Done) {
            FullState::Ready(data) => Ok(Some(Frame::data(data))),
            FullState::Done => Ok(None),
        }
    }

    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        TrailerHint::None
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        matches!(self.state, FullState::Done)
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(match &self.state {
            FullState::Ready(data) => data.as_init().len() as u64,
            FullState::Done => 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Body, Empty, Full, SizeHint};
    use bytes::Bytes;
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
            Poll::Pending => panic!("simple body unexpectedly returned pending"),
        }
    }

    #[test]
    fn empty_is_permanently_complete() {
        let mut body = Empty::new();

        assert_eq!(body.size_hint(), SizeHint::with_exact(0));
        assert!(complete(body.next_frame()).unwrap().is_none());
        assert!(complete(body.next_frame()).unwrap().is_none());
    }

    #[test]
    fn full_bytes_transfers_one_frame() {
        let mut body = Full::new(Bytes::from_static(b"hello"));

        assert_eq!(body.size_hint(), SizeHint::with_exact(5));
        assert!(!body.is_end_stream());
        let data = complete(body.next_frame())
            .unwrap()
            .expect("missing frame")
            .into_data()
            .expect("missing data");
        assert_eq!(data.as_init(), b"hello");
        assert_eq!(body.size_hint(), SizeHint::with_exact(0));
        body.recycle(data);
        assert!(body.is_end_stream());
        assert!(complete(body.next_frame()).unwrap().is_none());
        assert!(complete(body.next_frame()).unwrap().is_none());
    }

    #[derive(Debug)]
    struct TrackedBuf {
        data: &'static [u8],
        drops: Rc<Cell<usize>>,
    }

    impl IoBuf for TrackedBuf {
        fn as_init(&self) -> &[u8] {
            self.data
        }
    }

    impl Drop for TrackedBuf {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn full_recycles_only_after_buffer_is_returned() {
        let drops = Rc::new(Cell::new(0));
        let mut body = Full::new(TrackedBuf {
            data: b"payload",
            drops: Rc::clone(&drops),
        });

        let data = complete(body.next_frame())
            .unwrap()
            .expect("missing frame")
            .into_data()
            .expect("missing data");
        assert_eq!(drops.get(), 0);

        body.recycle(data);
        assert_eq!(drops.get(), 1);
    }
}
