use super::{decode::DecodeLimits, encode::EncodeConfig, head::HeadLimits};
use crate::error::{Error, ErrorKind};

/// Protocol configuration shared by the client and server drivers.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Config {
    pub(crate) head: HeadLimits,
    pub(crate) decode: DecodeLimits,
    pub(crate) encode: EncodeConfig,
    pub(crate) max_informational: usize,
    pub(crate) preserve_header_case: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            head: HeadLimits::new(64 * 1024, 100),
            decode: DecodeLimits::default(),
            encode: EncodeConfig::new(64 * 1024, 16 * 1024),
            max_informational: 16,
            preserve_header_case: false,
        }
    }
}

impl Config {
    pub(crate) fn incoming_head_limits(&mut self, bytes: usize, headers: usize) -> Result<(), Error> {
        if bytes == 0 || headers == 0 {
            return Err(Error::new(ErrorKind::LocalMessage, "head limits must be nonzero"));
        }
        self.head = HeadLimits::new(bytes, headers);
        Ok(())
    }

    pub(crate) fn incoming_framing_limits(
        &mut self,
        chunk_line: usize,
        trailer_bytes: usize,
        trailers: usize,
    ) -> Result<(), Error> {
        if chunk_line == 0 || trailer_bytes == 0 || trailers == 0 {
            return Err(Error::new(
                ErrorKind::LocalMessage,
                "body framing limits must be nonzero",
            ));
        }
        self.decode = self.decode.with_framing_limits(chunk_line, trailer_bytes, trailers);
        Ok(())
    }

    pub(crate) fn incoming_chunk_extension_limit(&mut self, bytes: usize) {
        self.decode = self.decode.with_chunk_extension_limit(bytes);
    }

    pub(crate) fn outgoing_head_limit(&mut self, bytes: usize) -> Result<(), Error> {
        nonzero(bytes)?;
        self.encode.max_head_bytes = bytes;
        Ok(())
    }

    pub(crate) fn outgoing_trailer_limit(&mut self, bytes: usize) -> Result<(), Error> {
        nonzero(bytes)?;
        self.encode.max_trailer_bytes = bytes;
        Ok(())
    }

    pub(crate) fn title_case_headers(&mut self, enabled: bool) {
        self.encode.title_case_headers = enabled;
    }
}

fn nonzero(bytes: usize) -> Result<(), Error> {
    if bytes == 0 {
        return Err(Error::new(ErrorKind::LocalMessage, "output limit must be nonzero"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directional_limits_are_independent_and_rejection_is_atomic() {
        let mut config = Config::default();
        config.outgoing_head_limit(400).unwrap();
        config.outgoing_trailer_limit(200).unwrap();
        config.incoming_head_limits(40, 2).unwrap();
        config.incoming_framing_limits(10, 20, 1).unwrap();
        assert_eq!(config.encode.max_head_bytes, 400);
        assert_eq!(config.encode.max_trailer_bytes, 200);
        config.outgoing_head_limit(800).unwrap();
        config.outgoing_trailer_limit(600).unwrap();
        assert_eq!(config.head, HeadLimits::new(40, 2));
        assert_eq!(config.decode, DecodeLimits::new(10, 20, 1));
        assert!(config.incoming_head_limits(900, 0).is_err());
        assert!(config.incoming_framing_limits(1, 900, 0).is_err());
        assert!(config.outgoing_head_limit(0).is_err());
        assert!(config.outgoing_trailer_limit(0).is_err());
        assert_eq!(config.head, HeadLimits::new(40, 2));
        assert_eq!(config.decode, DecodeLimits::new(10, 20, 1));
        assert_eq!(config.encode.max_head_bytes, 800);
        assert_eq!(config.encode.max_trailer_bytes, 600);
    }

    #[test]
    fn framing_updates_preserve_independent_extension_budget_including_zero() {
        for bytes in [0, 1, usize::MAX] {
            let mut config = Config::default();
            config.incoming_chunk_extension_limit(bytes);
            config.incoming_framing_limits(50, 300, 3).unwrap();
            let expected = DecodeLimits::new(50, 300, 3).with_chunk_extension_limit(bytes);
            assert_eq!(config.decode, expected);
            for limits in [(0, 900, 8), (1, 0, 8), (1, 900, 0)] {
                assert!(config.incoming_framing_limits(limits.0, limits.1, limits.2).is_err());
                assert_eq!(config.decode, expected);
            }
            config.incoming_head_limits(50, 3).unwrap();
            config.outgoing_head_limit(50).unwrap();
            config.outgoing_trailer_limit(50).unwrap();
            assert_eq!(config.decode, expected);
        }
    }
}
