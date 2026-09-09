use super::{Body, Frame, SizeHint, TrailerHint, frame::Kind};
use crate::future::WorkBudget;
use karmaio::{buf::IoBuf, io::Stream};

/// A native body backed by a Karmaio stream of fallible frames.
///
/// The stream must produce data followed by at most one trailers frame. Its
/// item-count hint is never interpreted as a byte count. Errors and stream
/// exhaustion terminate this adapter, even if the source stream can resume.
/// Recycling drops data: Karmaio streams have no producer-reclamation hook.
#[derive(Debug)]
pub struct StreamBody<S> {
    stream: S,
    done: bool,
}

impl<S> StreamBody<S> {
    /// Wraps a native stream without allocating or pinning it.
    #[inline]
    pub const fn new(stream: S) -> Self {
        Self { stream, done: false }
    }

    /// Recovers the underlying stream at its current position.
    ///
    /// This removes the adapter's terminal guard. A canceled call may already
    /// have changed the source stream's position.
    #[inline]
    pub fn into_inner(self) -> S {
        self.stream
    }
}

impl<S, D, E> Body for StreamBody<S>
where
    S: Stream<Item = Result<Frame<D>, E>>,
    D: IoBuf,
{
    type Data = D;
    type Error = E;

    async fn next_frame(&mut self) -> Result<Option<Frame<D>>, E> {
        if self.done {
            return Ok(None);
        }
        let result = self.stream.next().await.transpose();
        if !matches!(result, Ok(Some(_))) {
            self.done = true;
        }
        result
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
            TrailerHint::MayHave
        }
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.done
    }
}

/// A Karmaio stream preserving a body's data frames, trailers, and errors.
///
/// Consuming this stream transfers payload ownership to the caller without an
/// automatic recycle callback. Payload Drop remains safe. Recover the body
/// with [`Self::into_inner`] if explicit subsequent reclamation is needed.
#[derive(Debug)]
pub struct BodyStream<B> {
    body: B,
    done: bool,
}

impl<B> BodyStream<B> {
    /// Creates a native frame stream without allocating.
    #[inline]
    pub const fn new(body: B) -> Self {
        Self { body, done: false }
    }

    /// Recovers the body at its current position, removing the terminal guard.
    #[inline]
    pub fn into_inner(self) -> B {
        self.body
    }
}

impl<B: Body> Stream for BodyStream<B> {
    type Item = Result<Frame<B::Data>, B::Error>;

    async fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result = self.body.next_frame().await;
        if !matches!(result, Ok(Some(_))) {
            self.done = true;
        }
        result.transpose()
    }
}

/// A Karmaio stream of payloads that explicitly discards trailing headers.
///
/// Errors are preserved and terminate the stream. As with [`BodyStream`],
/// consuming data does not call the originating body's recycle method.
#[derive(Debug)]
pub struct BodyDataStream<B> {
    frames: BodyStream<B>,
}

impl<B> BodyDataStream<B> {
    /// Creates a native data stream without allocating.
    #[inline]
    pub const fn new(body: B) -> Self {
        Self {
            frames: BodyStream::new(body),
        }
    }

    /// Recovers the body at its current position; discarded trailers are lost.
    #[inline]
    pub fn into_inner(self) -> B {
        self.frames.into_inner()
    }
}

impl<B: Body> Stream for BodyDataStream<B> {
    type Item = Result<B::Data, B::Error>;

    async fn next(&mut self) -> Option<Self::Item> {
        let mut budget = WorkBudget::new();
        loop {
            budget.step().await;
            match self.frames.next().await? {
                Ok(frame) => match frame.kind {
                    Kind::Data(data) => return Some(Ok(data)),
                    Kind::Trailers(_) => {}
                },
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct Frames(VecDeque<Result<Frame<Vec<u8>>, &'static str>>);
    impl Stream for Frames {
        type Item = Result<Frame<Vec<u8>>, &'static str>;
        async fn next(&mut self) -> Option<Self::Item> {
            self.0.pop_front()
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.0.len(), Some(self.0.len()))
        }
    }

    #[test]
    fn item_counts_are_not_byte_hints_and_errors_fuse_the_adapter() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut body = StreamBody::new(Frames(VecDeque::from([
                Ok(Frame::data(vec![1; 8])),
                Err("source failed"),
                Ok(Frame::data(vec![2])),
            ])));
            assert_eq!(Body::size_hint(&body), SizeHint::new());
            assert_eq!(body.next_frame().await.unwrap().unwrap().into_data().unwrap().len(), 8);
            assert_eq!(body.next_frame().await.unwrap_err(), "source failed");
            assert!(body.is_end_stream());
            assert!(body.next_frame().await.unwrap().is_none());
            assert_eq!(body.into_inner().0.len(), 1);
        });
    }

    #[test]
    fn frame_stream_preserves_trailers_and_data_stream_discards_them() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let frames = || {
                Frames(VecDeque::from([
                    Ok(Frame::data(vec![])),
                    Ok(Frame::trailers(http::HeaderMap::new())),
                    Err("late error"),
                ]))
            };
            let mut stream = BodyStream::new(StreamBody::new(frames()));
            assert!(stream.next().await.unwrap().unwrap().data_ref().is_some());
            assert!(stream.next().await.unwrap().unwrap().trailers_ref().is_some());
            assert_eq!(stream.next().await.unwrap().unwrap_err(), "late error");
            assert!(stream.next().await.is_none());

            let mut stream = BodyDataStream::new(StreamBody::new(frames()));
            assert_eq!(stream.next().await.unwrap().unwrap(), Vec::<u8>::new());
            assert_eq!(stream.next().await.unwrap().unwrap_err(), "late error");
            assert!(stream.next().await.is_none());
        });
    }
}
