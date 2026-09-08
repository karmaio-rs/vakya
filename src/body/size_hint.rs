use crate::{Error, ErrorKind};

/// Bounds on the number of payload bytes still to be produced.
///
/// Bounds exclude trailers and wire framing. Equal bounds are an exact-length
/// promise, enforced by HTTP execution. Producers reduce the bounds when
/// yielding data, not when the data is recycled. These bounds do not establish
/// whether a body has reached its terminal state.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct SizeHint {
    lower: u64,
    upper: Option<u64>,
}

impl SizeHint {
    /// Creates an unknown size: zero lower bound and no upper bound.
    #[inline]
    pub const fn new() -> Self {
        Self { lower: 0, upper: None }
    }

    /// Creates an exact remaining-payload promise.
    #[inline]
    pub const fn with_exact(bytes: u64) -> Self {
        Self {
            lower: bytes,
            upper: Some(bytes),
        }
    }

    /// Creates checked bounds on remaining payload bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::LocalMessage`] if `upper` is less than `lower`.
    pub fn with_bounds(lower: u64, upper: Option<u64>) -> Result<Self, Error> {
        if upper.is_some_and(|upper| upper < lower) {
            return Err(Error::new(
                ErrorKind::LocalMessage,
                "body size upper bound is below its lower bound",
            ));
        }
        Ok(Self { lower, upper })
    }

    /// Returns the remaining-payload lower bound.
    #[inline]
    pub const fn lower(&self) -> u64 {
        self.lower
    }

    /// Returns the remaining-payload upper bound, if known.
    #[inline]
    pub const fn upper(&self) -> Option<u64> {
        self.upper
    }

    /// Returns an exact payload length when both bounds agree.
    #[inline]
    pub const fn exact(&self) -> Option<u64> {
        match self.upper {
            Some(upper) if upper == self.lower => Some(upper),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SizeHint;

    #[test]
    fn bounds_distinguish_unknown_exact_and_invalid_lengths() {
        for (lower, upper, exact) in [
            (0, None, None),
            (0, Some(0), Some(0)),
            (4, Some(9), None),
            (u64::MAX, Some(u64::MAX), Some(u64::MAX)),
        ] {
            assert_eq!(SizeHint::with_bounds(lower, upper).unwrap().exact(), exact);
        }
        assert!(SizeHint::with_bounds(1, Some(0)).is_err());
    }
}
