//! Native body frames and remaining-payload metadata.

mod frame;
mod size_hint;

pub use frame::Frame;
pub use size_hint::SizeHint;

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
