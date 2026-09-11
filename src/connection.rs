//! Outcomes of a caller-driven HTTP connection.
use crate::upgrade::Upgraded;
use std::fmt;

/// Ownership returned after all retained HTTP operations have settled.
/// The caller continues driving an upgraded transport using its concrete halves.
pub enum ConnectionOutcome<R, W> {
    /// HTTP completed and the connection was closed.
    Closed,
    /// HTTP transferred the transport and preserved unread protocol bytes.
    Upgraded(Upgraded<R, W>),
}

impl<R, W> fmt::Debug for ConnectionOutcome<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("Closed"),
            Self::Upgraded(io) => f.debug_tuple("Upgraded").field(io).finish(),
        }
    }
}
