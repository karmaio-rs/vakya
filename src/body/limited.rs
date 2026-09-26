use super::{Body, Frame, SizeHint, TrailerHint};
use karmaio::buf::IoBuf;
use std::{error::Error as StdError, fmt};

/// A streaming payload-byte limit that forwards whole owned frames.
///
/// Trailer header bytes do not consume the allowance. Overflow rejects the
/// entire frame, recycles it, and drops the owned source without draining.
/// A borrowed source remains caller-owned when its borrow is dropped. Source
/// errors remain distinct from limit failures. Subsequent calls after either
/// failure are terminal. Previously yielded data can be recycled while the
/// source remains available; no payload storage or copies are added.
#[derive(Debug)]
pub struct Limited<B> {
    body: Option<B>,
    limit: u64,
    remaining: u64,
    done: bool,
}
impl<B> Limited<B> {
    /// Bound total payload consumption without charging trailers or wire framing.
    #[inline]
    pub const fn new(body: B, max_bytes: u64) -> Self {
        Self {
            body: Some(body),
            limit: max_bytes,
            remaining: max_bytes,
            done: false,
        }
    }
}
impl<B: Body> Body for Limited<B> {
    type Data = B::Data;
    type Error = LimitError<B::Error>;
    async fn next_frame(&mut self) -> Result<Option<Frame<Self::Data>>, Self::Error> {
        if self.done {
            return Ok(None);
        }
        let frame = match self.body.as_mut().expect("active limited source").next_frame().await {
            Err(error) => {
                self.done = true;
                return Err(LimitError::Source(error));
            }
            Ok(None) => {
                self.done = true;
                return Ok(None);
            }
            Ok(Some(frame)) => frame,
        };
        if let Some(data) = frame.data_ref() {
            let accepted = u64::try_from(data.as_init().len())
                .ok()
                .filter(|&bytes| bytes <= self.remaining);
            let Some(bytes) = accepted else {
                if let Ok(data) = frame.into_data() {
                    self.recycle(data);
                }
                self.done = true;
                self.body.take();
                return Err(LimitError::LimitExceeded { limit: self.limit });
            };
            self.remaining -= bytes;
        }
        Ok(Some(frame))
    }
    #[inline]
    fn size_hint(&self) -> SizeHint {
        if self.done {
            return SizeHint::with_exact(0);
        }
        let hint = self.body.as_ref().expect("active limited source").size_hint();
        // If the source cannot fit, avoid inventing an exact truncated length.
        let lower = if hint.lower() <= self.remaining {
            hint.lower()
        } else {
            0
        };
        SizeHint::with_bounds(lower, Some(hint.upper().unwrap_or(self.remaining).min(self.remaining)))
            .expect("limited hint is bounded above its lower bound")
    }
    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        if self.done {
            TrailerHint::None
        } else {
            self.body.as_ref().expect("active limited source").trailer_hint()
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

/// A source failure or configured streaming payload limit violation.
#[derive(Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LimitError<E> {
    /// The source body's original terminal error.
    Source(E),
    /// A whole frame would exceed the configured total payload allowance.
    LimitExceeded {
        /// Maximum payload bytes accepted by this wrapper.
        limit: u64,
    },
}
impl<E> fmt::Display for LimitError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(_) => f.write_str("limited body source failed"),
            Self::LimitExceeded { .. } => f.write_str("body exceeded its streaming payload limit"),
        }
    }
}
impl<E: StdError + 'static> StdError for LimitError<E> {
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
    fn overflow_abandons_an_owned_unfinished_incoming_body() {
        let (mut producer, incoming) = Incoming::channel(SizeHint::new(), TrailerHint::None);
        let mut body = Limited::new(incoming, 1);
        {
            let mut offer = pin!(producer.offer(Frame::data(IncomingData::from_bytes(Bytes::from_static(b"ab")))));
            let mut cx = Context::from_waker(Waker::noop());
            assert!(offer.as_mut().poll(&mut cx).is_pending());
            assert!(matches!(
                pin!(body.next_frame()).as_mut().poll(&mut cx),
                Poll::Ready(Err(LimitError::LimitExceeded { limit: 1 }))
            ));
        }
        assert!(producer.is_abandoned());
    }
}
