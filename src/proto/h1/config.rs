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
    pub(crate) fn head_limits(&mut self, bytes: usize, headers: usize) -> Result<(), Error> {
        self.incoming_head_limits(bytes, headers)?;
        self.encode.max_head_bytes = bytes;
        Ok(())
    }

    pub(crate) fn incoming_head_limits(&mut self, bytes: usize, headers: usize) -> Result<(), Error> {
        if bytes == 0 || headers == 0 {
            return Err(Error::new(ErrorKind::LocalMessage, "head limits must be nonzero"));
        }
        self.head = HeadLimits::new(bytes, headers);
        Ok(())
    }

    pub(crate) fn body_limits(
        &mut self,
        chunk_line: usize,
        trailer_bytes: usize,
        trailers: usize,
    ) -> Result<(), Error> {
        self.incoming_body_limits(chunk_line, trailer_bytes, trailers)?;
        self.encode.max_trailer_bytes = trailer_bytes;
        Ok(())
    }

    pub(crate) fn incoming_body_limits(
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
        self.decode = DecodeLimits::new(chunk_line, trailer_bytes, trailers);
        Ok(())
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
        config.head_limits(400, 10).unwrap();
        config.body_limits(100, 200, 5).unwrap();
        config.incoming_head_limits(40, 2).unwrap();
        config.incoming_body_limits(10, 20, 1).unwrap();
        assert_eq!(config.encode.max_head_bytes, 400);
        assert_eq!(config.encode.max_trailer_bytes, 200);
        config.outgoing_head_limit(800).unwrap();
        config.outgoing_trailer_limit(600).unwrap();
        assert_eq!(config.head, HeadLimits::new(40, 2));
        assert_eq!(config.decode, DecodeLimits::new(10, 20, 1));
        assert!(config.head_limits(900, 0).is_err());
        assert!(config.body_limits(1, 900, 0).is_err());
        assert!(config.outgoing_head_limit(0).is_err());
        assert!(config.outgoing_trailer_limit(0).is_err());
        assert_eq!(config.head, HeadLimits::new(40, 2));
        assert_eq!(config.decode, DecodeLimits::new(10, 20, 1));
        assert_eq!(config.encode.max_head_bytes, 800);
        assert_eq!(config.encode.max_trailer_bytes, 600);
        config.head_limits(500, 4).unwrap();
        config.body_limits(50, 300, 3).unwrap();
        assert_eq!(config.head, HeadLimits::new(500, 4));
        assert_eq!(config.decode, DecodeLimits::new(50, 300, 3));
        assert_eq!(config.encode.max_head_bytes, 500);
        assert_eq!(config.encode.max_trailer_bytes, 300);
    }
}
