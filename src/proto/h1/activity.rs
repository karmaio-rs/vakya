//! Disjoint resources owned or borrowed by one retained directional activity.
use super::encode::EncodeConfig;

/// Input buffer and parser travel with the receive activity. A connection's
/// reusable head workspace is borrowed; a body's incremental decoder is owned.
/// The reader and strategy never share mutable access with transmission.
pub(super) struct ReceiveResources<'a, R, T, P> {
    pub(super) reader: &'a mut R,
    pub(super) strategy: &'a mut T,
    pub(super) buffer: crate::io::recv::RecvBuffer,
    pub(super) parser: P,
}

/// The sole writer and its concrete encoding policy belong to transmission.
pub(super) struct TransmitResources<'a, W> {
    pub(super) writer: &'a mut W,
    pub(super) encoding: EncodeConfig,
}
