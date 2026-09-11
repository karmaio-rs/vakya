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

/// An error returned by a Vakya HTTP or connection operation.
///
/// The error exposes a stable [`ErrorKind`] and may retain an underlying source.
/// Its formatted forms intentionally contain only Vakya-owned static context;
/// inspect [`StdError::source`] explicitly when source details are appropriate
/// to reveal.
pub struct Error {
    kind: ErrorKind,
    context: &'static str,
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
            source: source.clone().map(Source::Shared),
        };
        let second = Self {
            kind: self.kind,
            context: self.context,
            source: source.map(Source::Shared),
        };
        (first, second)
    }

    pub(crate) fn from_io(source: io::Error) -> Self {
        let kind = if karmaio::runtime::is_operation_canceled(&source) {
            ErrorKind::Canceled
        } else if source.kind() == io::ErrorKind::TimedOut {
            ErrorKind::Timeout
        } else {
            ErrorKind::Io
        };
        Self::with_source(kind, "HTTP I/O operation failed", source)
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
    use super::{Error, ErrorKind};
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
    fn cancellation_is_reported_by_kind() {
        let error = Error::from(karmaio::runtime::operation_canceled());

        assert!(error.is_canceled());
        assert_eq!(error.kind(), ErrorKind::Canceled);
    }
}
