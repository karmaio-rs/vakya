//! Completion-native streaming bodies and payload metadata.

mod boxed;
mod collect;
mod combinator;
mod frame;
#[allow(dead_code)] // Connection constructors are introduced in subsequent phases.
pub(crate) mod incoming;
mod simple;
mod size_hint;
mod stream;

use karmaio::buf::IoBuf;

pub use boxed::BoxBody;
pub use collect::{CollectError, Collected, collect};
pub use combinator::{Either, InspectFrame, MapError, MapFrame, WithTrailers};
pub use frame::Frame;
pub use incoming::{Incoming, IncomingData};
pub use simple::{Empty, Full};
pub use size_hint::SizeHint;
pub use stream::{BodyDataStream, BodyStream, StreamBody};

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

/// Native conveniences for composing and consuming bodies.
///
/// Composition retains concrete types and futures unless [`BodyExt::boxed`]
/// explicitly erases them. Borrow with `&mut body` when the underlying producer
/// should remain available after a consuming operation.
#[allow(async_fn_in_trait)]
pub trait BodyExt: Body {
    /// Erases this body locally, preserving metadata and recycling.
    ///
    /// This boxes the producer and each driven frame future. Payload and error
    /// types remain fixed. No `Send`, `Sync`, or `'static` bound is added.
    #[inline]
    fn boxed<'a>(self) -> BoxBody<'a, Self::Data, Self::Error>
    where
        Self: Sized + 'a,
    {
        BoxBody::new(self)
    }

    /// Collects this body within an explicit payload-byte limit.
    ///
    /// See [`collect`] for errors, trailer preservation, and cancellation.
    async fn collect(mut self, max_bytes: usize) -> Result<Collected, CollectError<Self::Error>>
    where
        Self: Sized,
    {
        collect(&mut self, max_bytes).await
    }

    /// Maps errors while preserving frame ownership, metadata, and recycling.
    #[inline]
    fn map_err<F, E>(self, map: F) -> MapError<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Error) -> E,
    {
        MapError::new(self, map)
    }

    /// Observes frames without changing metadata or recycling.
    #[inline]
    fn inspect_frame<F>(self, inspect: F) -> InspectFrame<Self, F>
    where
        Self: Sized,
        F: FnMut(&Frame<Self::Data>),
    {
        InspectFrame::new(self, inspect)
    }

    /// Transforms frames, resetting size/trailer hints and dropping recycled output.
    ///
    /// The transformation must preserve valid data/trailer ordering.
    #[inline]
    fn map_frame<F, D>(self, map: F) -> MapFrame<Self, F>
    where
        Self: Sized,
        F: FnMut(Frame<Self::Data>) -> Frame<D>,
        D: IoBuf,
    {
        MapFrame::new(self, map)
    }

    /// Appends fields to existing trailers or emits them after successful EOF.
    #[inline]
    fn with_trailers(self, trailers: http::HeaderMap) -> WithTrailers<Self>
    where
        Self: Sized,
    {
        WithTrailers::new(self, trailers)
    }

    /// Converts to a Karmaio frame stream, preserving trailers and errors.
    ///
    /// Stream consumption has no automatic body-recycling callback.
    #[inline]
    fn into_frame_stream(self) -> BodyStream<Self>
    where
        Self: Sized,
    {
        BodyStream::new(self)
    }

    /// Converts to a Karmaio data stream, explicitly discarding trailers.
    ///
    /// Stream consumption has no automatic body-recycling callback.
    #[inline]
    fn into_data_stream(self) -> BodyDataStream<Self>
    where
        Self: Sized,
    {
        BodyDataStream::new(self)
    }
}

impl<B: Body + ?Sized> BodyExt for B {}

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

// The incoming-body bridge is integrated in this phase.
#[allow(dead_code)]
pub(crate) mod pipe;
