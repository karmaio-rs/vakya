use super::{Body, Frame, SizeHint, TrailerHint, frame::Kind};
use karmaio::buf::IoBuf;
use std::{error::Error as StdError, fmt};

/// A body that validates an exact payload length and trailer capability.
///
/// Empty data frames are retained. Exact zero is not terminal until the source
/// ends, so trailing headers and source errors remain observable. Payloads are
/// forwarded without copying and recovered data is recycled while the source
/// remains available. A contract violation reclaims its offending data frame,
/// then drops the owned source to abandon unfinished incoming consumption.
/// Wrapping a borrowed body drops only the borrow, leaving its owner responsible
/// for abandonment. Source errors are preserved and terminal.
#[derive(Debug)]
pub struct ExactLength<B> {
    body: Option<B>,
    remaining: u64,
    trailers: TrailerHint,
    saw_trailers: bool,
    done: bool,
}

impl<B> ExactLength<B> {
    /// Attach an exact payload contract and independently supplied trailer policy.
    #[inline]
    pub const fn new(body: B, bytes: u64, trailer_hint: TrailerHint) -> Self {
        Self {
            body: Some(body),
            remaining: bytes,
            trailers: trailer_hint,
            saw_trailers: false,
            done: false,
        }
    }
}

impl<B: Body> Body for ExactLength<B> {
    type Data = B::Data;
    type Error = ExactLengthError<B::Error>;

    async fn next_frame(&mut self) -> Result<Option<Frame<Self::Data>>, Self::Error> {
        if self.done {
            return Ok(None);
        }
        let next = self
            .body
            .as_mut()
            .expect("active exact-length source")
            .next_frame()
            .await;
        let frame = match next {
            Err(error) => {
                self.done = true;
                return Err(ExactLengthError::Source(error));
            }
            Ok(None) => {
                self.done = true;
                if self.remaining != 0 {
                    self.body.take();
                    return Err(ExactLengthError::TooShort {
                        remaining: self.remaining,
                    });
                }
                return Ok(None);
            }
            Ok(Some(frame)) => frame,
        };
        let violation = match &frame.kind {
            Kind::Data(_) if self.saw_trailers => Some(ExactLengthError::DataAfterTrailers),
            Kind::Data(data) => match u64::try_from(data.as_init().len()) {
                Ok(bytes) if bytes <= self.remaining => {
                    self.remaining -= bytes;
                    None
                }
                _ => Some(ExactLengthError::TooLong {
                    remaining: self.remaining,
                    frame_bytes: data.as_init().len(),
                }),
            },
            Kind::Trailers(_) if self.saw_trailers => Some(ExactLengthError::RepeatedTrailers),
            Kind::Trailers(_) if self.trailers == TrailerHint::None => Some(ExactLengthError::UnexpectedTrailers),
            Kind::Trailers(_) => {
                self.saw_trailers = true;
                None
            }
        };
        if let Some(error) = violation {
            if let Kind::Data(data) = frame.kind {
                self.recycle(data);
            }
            self.done = true;
            self.body.take();
            return Err(error);
        }
        Ok(Some(frame))
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(if self.done { 0 } else { self.remaining })
    }
    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        if self.done || self.saw_trailers {
            TrailerHint::None
        } else {
            self.trailers
        }
    }
    #[inline]
    fn is_end_stream(&self) -> bool {
        self.done
    }
    #[inline]
    fn recycle(&mut self, data: Self::Data) {
        if let Some(body) = &mut self.body {
            body.recycle(data);
        }
    }
}

/// Source failure or violation of an exact-length body contract.
#[derive(Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExactLengthError<E> {
    /// The source body's original error.
    Source(E),
    /// The source ended before supplying all promised payload bytes.
    TooShort {
        /// Payload bytes still required when the source ended.
        remaining: u64,
    },
    /// A whole data frame exceeded the remaining payload allowance.
    TooLong {
        /// Payload bytes still permitted before the offending frame.
        remaining: u64,
        /// Initialized bytes in the rejected frame.
        frame_bytes: usize,
    },
    /// The source emitted trailers despite a no-trailers contract.
    UnexpectedTrailers,
    /// The source emitted a second trailers frame.
    RepeatedTrailers,
    /// The source emitted data, including empty data, after trailers.
    DataAfterTrailers,
}
impl<E> fmt::Display for ExactLengthError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Source(_) => "exact-length body source failed",
            Self::TooShort { .. } => "body ended before its exact payload length",
            Self::TooLong { .. } => "body exceeded its exact payload length",
            Self::UnexpectedTrailers => "body produced forbidden trailers",
            Self::RepeatedTrailers => "body produced repeated trailers",
            Self::DataAfterTrailers => "body produced data after trailers",
        })
    }
}
impl<E: StdError + 'static> StdError for ExactLengthError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body::{Incoming, IncomingData};
    use bytes::Bytes;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    #[test]
    fn contract_violation_abandons_an_owned_unfinished_incoming_body() {
        let (mut producer, incoming) = Incoming::channel(SizeHint::new(), TrailerHint::None);
        let mut body = ExactLength::new(incoming, 1, TrailerHint::None);
        {
            let mut offer = pin!(producer.offer(Frame::data(IncomingData::from_bytes(Bytes::from_static(b"ab")))));
            let mut cx = Context::from_waker(Waker::noop());
            assert!(offer.as_mut().poll(&mut cx).is_pending());
            assert!(matches!(
                pin!(body.next_frame()).as_mut().poll(&mut cx),
                Poll::Ready(Err(ExactLengthError::TooLong { .. }))
            ));
        }
        assert!(producer.is_abandoned());
    }
}
