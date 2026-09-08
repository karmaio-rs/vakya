//! Completion-native streaming bodies and payload metadata.

mod collect;
mod combinator;
mod frame;
mod simple;
mod size_hint;

use karmaio::buf::IoBuf;

pub use collect::{CollectError, Collected, collect};
pub use combinator::{Either, MapError};
pub use frame::Frame;
pub use simple::{Empty, Full};
pub use size_hint::SizeHint;

/// A completion-native producer of owned data and trailing headers.
///
/// A yielded data frame transfers its buffer to the consumer. The consumer
/// retains it while any completion operation may reference it. Recovering the
/// buffer permits recycling; requesting cancellation alone does not.
///
/// Neither the body nor its generated future must be `Send`, `Sync`, or
/// `'static`. Owned payloads satisfy Karmaio's [`IoBuf`] contract.
///
/// # Implementor contract
///
/// - Trailers occur at most once and follow all data.
/// - After `Ok(None)`, later calls also return `Ok(None)`.
/// - An error is terminal: no later data or trailers may be produced.
/// - Size bounds describe remaining payload bytes, excluding trailers. Update
///   them when yielding data, not when recycling it. Equal bounds promise an
///   exact length; consumers must validate untrusted producers' promises.
/// - [`TrailerHint::None`] promises that no trailers will be produced.
/// - A true [`Body::is_end_stream`] promises that no further frame will appear.
///
/// Dropping a pending `next_frame` future does not transfer a partially produced
/// frame. Implementations must keep resources safe on cancellation; this does
/// not require a canceled call to be resumable without losing progress.
#[allow(async_fn_in_trait)]
pub trait Body {
    /// The owned data buffer yielded by this body.
    type Data: IoBuf;
    /// The error produced by this body, without additional trait bounds.
    type Error;

    /// Produces the next frame, or permanently ends the body with `Ok(None)`.
    ///
    /// Empty data is a frame, not termination. An error terminates production.
    async fn next_frame(&mut self) -> Result<Option<Frame<Self::Data>>, Self::Error>;

    /// Returns bounds on remaining payload bytes, excluding trailers.
    #[inline]
    fn size_hint(&self) -> SizeHint {
        SizeHint::new()
    }

    /// Returns whether trailers may still be produced.
    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        TrailerHint::MayHave
    }

    /// Returns true only when no further frame can be produced.
    ///
    /// False is conservative. Exact zero payload length alone does not imply
    /// termination because trailers or an empty data frame may remain.
    #[inline]
    fn is_end_stream(&self) -> bool {
        false
    }

    /// Receives a recovered data buffer for optional reuse.
    ///
    /// A consumer that continues driving completion returns each recovered
    /// buffer once while the producer remains available. Dropping the consumer,
    /// producer, or driving future does not guarantee this callback. Buffer
    /// safety must not depend on it. The default drops the buffer.
    #[inline]
    fn recycle(&mut self, data: Self::Data) {
        drop(data);
    }
}

impl<B: Body + ?Sized> Body for &mut B {
    type Data = B::Data;
    type Error = B::Error;

    #[inline]
    async fn next_frame(&mut self) -> Result<Option<Frame<Self::Data>>, Self::Error> {
        (**self).next_frame().await
    }

    #[inline]
    fn size_hint(&self) -> SizeHint {
        (**self).size_hint()
    }

    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        (**self).trailer_hint()
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        (**self).is_end_stream()
    }

    #[inline]
    fn recycle(&mut self, data: Self::Data) {
        (**self).recycle(data);
    }
}

/// Whether a body may produce trailing header fields.
///
/// Trailer capability is independent of payload size. Even an exact zero-byte
/// payload can have trailers, so it does not establish end of stream.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum TrailerHint {
    /// The body promises never to produce a trailers frame.
    None,
    /// The body may produce trailers after its data.
    #[default]
    MayHave,
}
