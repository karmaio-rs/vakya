use super::{Body, Frame, SizeHint, TrailerHint};
use karmaio::buf::IoBuf;
use std::{fmt, future::Future, pin::Pin};

type FrameFuture<'a, D, E> = Pin<Box<dyn Future<Output = Result<Option<Frame<D>>, E>> + 'a>>;

// Keep erasure private: the native Body trait retains its concrete async future.
trait ErasedBody<D, E> {
    fn next_frame(&mut self) -> FrameFuture<'_, D, E>;
    fn size_hint(&self) -> SizeHint;
    fn trailer_hint(&self) -> TrailerHint;
    fn is_end_stream(&self) -> bool;
    fn recycle(&mut self, data: D);
}

impl<B: Body> ErasedBody<B::Data, B::Error> for B {
    fn next_frame(&mut self) -> FrameFuture<'_, B::Data, B::Error> {
        Box::pin(Body::next_frame(self))
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        Body::size_hint(self)
    }
    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        Body::trailer_hint(self)
    }
    #[inline]
    fn is_end_stream(&self) -> bool {
        Body::is_end_stream(self)
    }
    #[inline]
    fn recycle(&mut self, data: B::Data) {
        Body::recycle(self, data);
    }
}

/// A locally erased body with fixed payload and error types.
///
/// The body may borrow for `'a`; neither it nor its future must be `Send` or
/// `Sync`. Construction boxes the producer and each driven `next_frame` call
/// boxes its future. Prefer concrete bodies or [`super::Either`] when the
/// alternatives are known. Metadata and recovered buffers pass through to
/// the original producer without conversion or payload copies.
///
/// Dropping a pending call drops the inner future under the native cancellation
/// contract. This does not guarantee a future recycle callback or resumability.
pub struct BoxBody<'a, D, E> {
    body: Box<dyn ErasedBody<D, E> + 'a>,
}

impl<'a, D: IoBuf, E> BoxBody<'a, D, E> {
    /// Erases a native producer without imposing thread-safety bounds.
    pub fn new<B>(body: B) -> Self
    where
        B: Body<Data = D, Error = E> + 'a,
    {
        Self { body: Box::new(body) }
    }
}

impl<D, E> fmt::Debug for BoxBody<'_, D, E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("BoxBody").finish_non_exhaustive()
    }
}

impl<D: IoBuf, E> Body for BoxBody<'_, D, E> {
    type Data = D;
    type Error = E;

    async fn next_frame(&mut self) -> Result<Option<Frame<D>>, E> {
        self.body.next_frame().await
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
    fn recycle(&mut self, data: D) {
        self.body.recycle(data);
    }
}
