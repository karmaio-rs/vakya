use super::{Body, Frame, SizeHint, TrailerHint};
use karmaio::buf::IoBuf;

/// A data-only transformation that passes trailers and source errors unchanged.
///
/// Payload length becomes unknown because the mapping may change byte count.
/// Trailer capability and source terminal state are preserved. Transformed
/// buffers are dropped by default: they cannot be safely recycled into the
/// original producer. Use inspection for ownership-preserving observation.
#[derive(Debug)]
pub struct MapData<B, F> {
    body: B,
    map: F,
    done: bool,
}
impl<B, F> MapData<B, F> {
    /// Wrap a body with a synchronous payload transformation.
    #[inline]
    pub const fn new(body: B, map: F) -> Self {
        Self { body, map, done: false }
    }
}
impl<B, F, D> Body for MapData<B, F>
where
    B: Body,
    F: FnMut(B::Data) -> D,
    D: IoBuf,
{
    type Data = D;
    type Error = B::Error;
    async fn next_frame(&mut self) -> Result<Option<Frame<D>>, Self::Error> {
        if self.done {
            return Ok(None);
        }
        match self.body.next_frame().await {
            Ok(Some(frame)) => Ok(Some(frame.map_data(&mut self.map))),
            result => {
                self.done = true;
                result.map(|_| None)
            }
        }
    }
    #[inline]
    fn size_hint(&self) -> SizeHint {
        if self.done {
            SizeHint::with_exact(0)
        } else {
            SizeHint::new()
        }
    }
    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        if self.done {
            TrailerHint::None
        } else {
            self.body.trailer_hint()
        }
    }
    #[inline]
    fn is_end_stream(&self) -> bool {
        self.done || self.body.is_end_stream()
    }
}
