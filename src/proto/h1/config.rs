use super::{decode::DecodeLimits, encode::EncodeLimits, head::HeadLimits};

/// Protocol budgets shared by the eventual client and server drivers.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Config {
    pub(crate) head: HeadLimits,
    pub(crate) decode: DecodeLimits,
    pub(crate) encode: EncodeLimits,
    pub(crate) max_informational: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            head: HeadLimits::new(64 * 1024, 100),
            decode: DecodeLimits::default(),
            encode: EncodeLimits::new(64 * 1024, 16 * 1024),
            max_informational: 16,
        }
    }
}

impl Config {
    pub(crate) fn head_limits(&mut self, bytes: usize, headers: usize) -> Result<(), crate::Error> {
        if bytes == 0 || headers == 0 {
            return Err(crate::Error::new(
                crate::ErrorKind::LocalMessage,
                "head limits must be nonzero",
            ));
        }
        self.head = HeadLimits::new(bytes, headers);
        self.encode.max_head_bytes = bytes;
        Ok(())
    }

    pub(crate) fn body_limits(
        &mut self,
        chunk_line: usize,
        trailer_bytes: usize,
        trailers: usize,
    ) -> Result<(), crate::Error> {
        if chunk_line == 0 || trailer_bytes == 0 || trailers == 0 {
            return Err(crate::Error::new(
                crate::ErrorKind::LocalMessage,
                "body framing limits must be nonzero",
            ));
        }
        self.decode = DecodeLimits::new(chunk_line, trailer_bytes, trailers);
        self.encode.max_trailer_bytes = trailer_bytes;
        Ok(())
    }
}
