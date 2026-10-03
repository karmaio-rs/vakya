//! Public error categories for HTTP and connection operations.

#[cfg(feature = "http1")]
use std::rc::Rc;
use std::{error::Error as StdError, fmt, io};

/// A stable, high-level category for a Vakya operation failure.
///
/// More categories may be added in future releases. Callers should retain a
/// fallback arm when matching this enum.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// A transport or operating-system I/O operation failed.
    Io,
    /// TLS configuration, authentication, or handshake failed.
    Tls,
    /// A peer sent an invalid or ambiguously framed HTTP message.
    InvalidMessage,
    /// A locally constructed HTTP message cannot be encoded safely.
    LocalMessage,
    /// Producing or consuming an HTTP body failed.
    Body,
    /// An application service failed to produce a response.
    Service,
    /// An admission or command slot is occupied by another local operation.
    Busy,
    /// A configured size, count, or work limit was exceeded.
    Limit,
    /// A configured elapsed-time limit was exceeded.
    Timeout,
    /// The requested protocol behavior is not supported.
    Unsupported,
    /// The connection closed before the requested operation completed.
    Closed,
    /// The operation was canceled by its owner or shutdown policy.
    ///
    /// Cancellation is a normal lifecycle outcome and is distinct from an I/O
    /// failure or invalid peer message.
    Canceled,
    /// A protocol upgrade failed before transport ownership was transferred.
    Upgrade,
    /// Vakya detected a violated internal invariant.
    ///
    /// This category represents a library bug when no more specific category
    /// applies.
    Internal,
}

impl ErrorKind {
    const fn description(self) -> &'static str {
        match self {
            Self::Io => "I/O failure",
            Self::Tls => "TLS failure",
            Self::InvalidMessage => "invalid peer message",
            Self::LocalMessage => "invalid local message",
            Self::Body => "body failure",
            Self::Service => "service failure",
            Self::Busy => "operation busy",
            Self::Limit => "configured limit exceeded",
            Self::Timeout => "deadline exceeded",
            Self::Unsupported => "unsupported HTTP behavior",
            Self::Closed => "connection closed",
            Self::Canceled => "operation canceled",
            Self::Upgrade => "upgrade failure",
            Self::Internal => "internal invariant failure",
        }
    }
}

/// Structured context for failures that need more than a broad error kind.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ErrorDetail {
    /// A local admission or command slot is occupied.
    Busy(BusyReason),
    /// Received input exceeded a framing, delivery, or retention budget.
    IncomingLimit(LimitKind),
    /// Generated output exceeded an encoding or informational budget.
    OutgoingLimit(LimitKind),
    /// An elapsed-time limit expired in this phase.
    Timeout(TimeoutPhase),
    /// Recoverable allocation or capacity reservation failed.
    Allocation,
    /// The driver was dropped without certifying I/O settlement.
    DriverLost,
}

/// The occupied local slot behind an [`ErrorKind::Busy`] failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum BusyReason {
    /// A request or permit already owns admission.
    RequestAdmission,
    /// Another request reservation waiter is registered.
    ReservationWaiter,
    /// An informational head write still owns its command slot.
    InformationalWrite,
}

/// The size or count budget exceeded by an HTTP operation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum LimitKind {
    /// Serialized message head bytes.
    HeadBytes,
    /// Message head field count.
    HeaderCount,
    /// Capacity of the standard message head map.
    HeadMapCapacity,
    /// One chunk-size line, including extensions.
    ChunkLineBytes,
    /// Aggregate chunk-extension bytes.
    ChunkExtensionBytes,
    /// Serialized trailing header bytes.
    TrailerBytes,
    /// Trailing field count.
    TrailerCount,
    /// Capacity of the standard trailing header map.
    TrailerMapCapacity,
    /// Retained unconsumed receive bytes.
    RetainedInput,
    /// Informational response count.
    InformationalResponses,
    /// Payload bytes discarded while draining.
    DrainPayload,
    /// Payload plus framing bytes discarded while draining.
    DrainWire,
}

/// The operation phase whose elapsed-time budget expired.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum TimeoutPhase {
    /// An underlying transport's unspecified timeout.
    Transport,
    /// Receiving a complete final message head.
    Head,
    /// Receiving demanded body progress.
    BodyProgress,
    /// Writing or flushing transport progress.
    WriteProgress,
    /// Draining the body within its total deadline.
    Drain,
    /// Graceful connection shutdown or transport shutdown settlement.
    Shutdown,
}

#[cfg(feature = "http1")]
#[derive(Debug)]
pub(crate) struct DeadlineElapsed(pub(crate) TimeoutPhase);
#[cfg(feature = "http1")]
impl fmt::Display for DeadlineElapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HTTP transport progress deadline exceeded")
    }
}
#[cfg(feature = "http1")]
impl StdError for DeadlineElapsed {}

/// An error returned by a Vakya HTTP or connection operation.
///
/// The error exposes a stable [`ErrorKind`] and may retain an underlying source.
/// Its formatted forms intentionally contain only Vakya-owned static context;
/// inspect [`StdError::source`] explicitly when source details are appropriate
/// to reveal.
pub struct Error {
    kind: ErrorKind,
    context: &'static str,
    detail: Option<ErrorDetail>,
    source: Option<Source>,
}

enum Source {
    Owned(Box<dyn StdError + 'static>),
    #[cfg(feature = "http1")]
    Shared(Rc<dyn StdError + 'static>),
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, context: &'static str) -> Self {
        Self {
            kind,
            context,
            detail: None,
            source: None,
        }
    }

    pub(crate) fn with_source<E>(kind: ErrorKind, context: &'static str, source: E) -> Self
    where
        E: StdError + 'static,
    {
        let mut error = Self::new(kind, context);
        error.source = Some(Source::Owned(Box::new(source)));
        error
    }

    pub(crate) fn with_detail(self, detail: ErrorDetail) -> Self {
        self.classified(Some(detail))
    }
    pub(crate) fn classified(mut self, detail: Option<ErrorDetail>) -> Self {
        self.detail = detail;
        self
    }

    /// Returns optional structured failure context without inspecting diagnostics.
    #[inline]
    pub const fn detail(&self) -> Option<ErrorDetail> {
        self.detail
    }

    /// Share a source only when both a body and its driver must report it.
    /// The public error remains non-Clone and source downcasts stay unchanged.
    #[cfg(feature = "http1")]
    pub(crate) fn split(self) -> (Self, Self) {
        let source = self.source.map(|source| match source {
            Source::Owned(source) => Rc::from(source),
            Source::Shared(source) => source,
        });
        let first = Self {
            kind: self.kind,
            context: self.context,
            detail: self.detail,
            source: source.clone().map(Source::Shared),
        };
        let second = Self {
            kind: self.kind,
            context: self.context,
            detail: self.detail,
            source: source.map(Source::Shared),
        };
        (first, second)
    }

    pub(crate) fn from_io(source: io::Error) -> Self {
        // Karmaio retains Rustls protocol failures inside the originating I/O
        // error. Preserve that source while keeping socket failures distinct.
        #[cfg(feature = "tls")]
        if source
            .get_ref()
            .is_some_and(|error| error.is::<karmaio::tls::rustls::Error>())
        {
            return Self::with_source(ErrorKind::Tls, "TLS protocol operation failed", source);
        }

        let kind = if karmaio::runtime::is_operation_canceled(&source) {
            ErrorKind::Canceled
        } else if source.kind() == io::ErrorKind::TimedOut {
            ErrorKind::Timeout
        } else {
            ErrorKind::Io
        };

        let detail = if kind == ErrorKind::Timeout {
            #[cfg(feature = "http1")]
            let phase = source
                .get_ref()
                .and_then(|error| error.downcast_ref::<DeadlineElapsed>())
                .map(|elapsed| elapsed.0)
                .unwrap_or(TimeoutPhase::Transport);
            #[cfg(not(feature = "http1"))]
            let phase = TimeoutPhase::Transport;
            Some(ErrorDetail::Timeout(phase))
        } else if source.kind() == io::ErrorKind::OutOfMemory {
            Some(ErrorDetail::Allocation)
        } else {
            None
        };
        Self::with_source(kind, "HTTP I/O operation failed", source).classified(detail)
    }

    /// Returns the high-level category of this error.
    #[inline]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns `true` when the operation ended because it was canceled.
    #[inline]
    pub const fn is_canceled(&self) -> bool {
        matches!(self.kind, ErrorKind::Canceled)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Error")
            .field("kind", &self.kind)
            .field("context", &self.context)
            .field("detail", &self.detail)
            .field("has_source", &self.source.is_some())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.context, self.kind.description())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_ref().map(|source| match source {
            Source::Owned(source) => source.as_ref(),
            #[cfg(feature = "http1")]
            Source::Shared(source) => source.as_ref(),
        })
    }
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Self::from_io(source)
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, ErrorDetail, ErrorKind, TimeoutPhase};
    use std::{error::Error as _, fmt, io};

    #[derive(Debug)]
    struct LocalError;

    impl fmt::Display for LocalError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("private local detail")
        }
    }

    impl std::error::Error for LocalError {}

    #[test]
    fn formatting_omits_source_text() {
        let error = Error::with_source(ErrorKind::Body, "response body failed", LocalError);

        assert_eq!(error.to_string(), "response body failed: body failure");
        assert!(!format!("{error:?}").contains("private local detail"));
    }

    #[test]
    fn io_conversion_preserves_source() {
        let error = Error::from(io::Error::other("peer supplied detail"));

        assert_eq!(error.kind(), ErrorKind::Io);
        assert_eq!(error.to_string(), "HTTP I/O operation failed: I/O failure");
        assert_eq!(
            error.source().map(ToString::to_string).as_deref(),
            Some("peer supplied detail")
        );
    }

    #[test]
    fn transport_details_preserve_originating_sources() {
        let error = Error::from(io::Error::new(io::ErrorKind::TimedOut, LocalError));
        assert_eq!(error.detail(), Some(ErrorDetail::Timeout(TimeoutPhase::Transport)));
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<io::Error>()
                .unwrap()
                .get_ref()
                .unwrap()
                .is::<LocalError>()
        );
        let error = Error::from(io::Error::from(io::ErrorKind::OutOfMemory));
        assert_eq!(error.detail(), Some(ErrorDetail::Allocation));
    }

    #[cfg(feature = "http1")]
    #[test]
    fn split_preserves_details_and_original_source_identity() {
        let error =
            Error::with_source(ErrorKind::Body, "static context", LocalError).with_detail(ErrorDetail::DriverLost);
        let (first, second) = error.split();
        assert_eq!(first.detail(), Some(ErrorDetail::DriverLost));
        assert_eq!(second.detail(), first.detail());
        assert!(std::ptr::eq(first.source().unwrap(), second.source().unwrap()));
        assert!(first.source().unwrap().is::<LocalError>());
        assert!(!format!("{first:?} {second}").contains("private local detail"));
        for phase in [
            TimeoutPhase::Head,
            TimeoutPhase::BodyProgress,
            TimeoutPhase::WriteProgress,
            TimeoutPhase::Shutdown,
        ] {
            let error = Error::from(io::Error::new(io::ErrorKind::TimedOut, super::DeadlineElapsed(phase)));
            assert_eq!(error.detail(), Some(ErrorDetail::Timeout(phase)));
            assert!(error.source().unwrap().is::<io::Error>());
        }
    }

    #[test]
    fn cancellation_is_reported_by_kind() {
        let error = Error::from(karmaio::runtime::operation_canceled());

        assert!(error.is_canceled());
        assert_eq!(error.kind(), ErrorKind::Canceled);
    }
}
