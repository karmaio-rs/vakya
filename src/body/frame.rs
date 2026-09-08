use http::HeaderMap;

/// An owned semantic body item containing payload data or trailing headers.
///
/// Frames are not transport-read boundaries or HTTP/2 wire frames. Data stays
/// owned while completion I/O can reference it. Trailers follow all data and
/// occur at most once; producers and consumers enforce that sequence.
#[derive(Debug)]
pub struct Frame<D> {
    kind: Kind<D>,
}

#[derive(Debug)]
enum Kind<D> {
    Data(D),
    Trailers(HeaderMap),
}

impl<D> Frame<D> {
    /// Creates a data frame, transferring ownership of `data` into it.
    #[inline]
    pub fn data(data: D) -> Self {
        Self { kind: Kind::Data(data) }
    }

    /// Creates a trailers frame.
    #[inline]
    pub fn trailers(trailers: HeaderMap) -> Self {
        Self {
            kind: Kind::Trailers(trailers),
        }
    }

    /// Returns a shared reference to the payload, if this is a data frame.
    #[inline]
    pub fn data_ref(&self) -> Option<&D> {
        match &self.kind {
            Kind::Data(data) => Some(data),
            Kind::Trailers(_) => None,
        }
    }

    /// Returns a shared reference to the trailing fields, if present.
    #[inline]
    pub fn trailers_ref(&self) -> Option<&HeaderMap> {
        match &self.kind {
            Kind::Data(_) => None,
            Kind::Trailers(trailers) => Some(trailers),
        }
    }

    /// Extracts the owned payload, returning the original frame on a mismatch.
    ///
    /// # Errors
    ///
    /// Returns `Err(self)` for a trailers frame, preserving its fields.
    #[inline]
    pub fn into_data(self) -> Result<D, Self> {
        match self.kind {
            Kind::Data(data) => Ok(data),
            Kind::Trailers(_) => Err(self),
        }
    }

    /// Extracts trailing fields, returning the original frame on a mismatch.
    ///
    /// # Errors
    ///
    /// Returns `Err(self)` for a data frame, preserving its payload.
    #[inline]
    pub fn into_trailers(self) -> Result<HeaderMap, Self> {
        match self.kind {
            Kind::Data(_) => Err(self),
            Kind::Trailers(trailers) => Ok(trailers),
        }
    }

    /// Transforms the payload, passing trailers through without calling `map`.
    ///
    /// The caller must update any associated size or recycling metadata when
    /// the transformation changes the payload representation or byte count.
    #[inline]
    pub fn map_data<T>(self, map: impl FnOnce(D) -> T) -> Frame<T> {
        match self.kind {
            Kind::Data(data) => Frame::data(map(data)),
            Kind::Trailers(trailers) => Frame::trailers(trailers),
        }
    }
}
