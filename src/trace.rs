//! Static lifecycle observations; message data and error sources stay private.
#[inline]
pub(crate) fn lifecycle(event: &'static str) {
    #[cfg(feature = "tracing")]
    tracing::trace!(target: "vakya::connection", event, "HTTP connection lifecycle");
    #[cfg(not(feature = "tracing"))]
    let _ = event;
}

#[inline]
pub(crate) fn failure(kind: crate::ErrorKind) {
    #[cfg(feature = "tracing")]
    tracing::debug!(target: "vakya::connection", ?kind, "HTTP connection failed");
    #[cfg(not(feature = "tracing"))]
    let _ = kind;
}
