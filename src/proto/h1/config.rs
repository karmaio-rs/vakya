use super::{decode::DecodeLimits, encode::EncodeConfig, head::HeadLimits};
use crate::body::incoming::DrainConfig;
use crate::error::{Error, ErrorKind};
use std::time::Duration;

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

/// Timeouts, drain budget, and receive bounds shared by both HTTP/1 roles.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionSettings {
    pub(crate) protocol: Config,
    pub(crate) drain: DrainConfig,
    #[cfg(feature = "tls")]
    pub(crate) tls_info: Option<crate::tls::TlsInfo>,
    pub(crate) head_timeout: Option<Duration>,
    pub(crate) body_progress_timeout: Option<Duration>,
    pub(crate) write_progress_timeout: Option<Duration>,
    pub(crate) preferred_read: usize,
    pub(crate) max_retained: usize,
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        Self {
            protocol: Config::default(),
            drain: DrainConfig::default(),
            #[cfg(feature = "tls")]
            tls_info: None,
            head_timeout: None,
            body_progress_timeout: None,
            write_progress_timeout: None,
            preferred_read: 16 * 1024,
            max_retained: 128 * 1024,
        }
    }
}

impl ConnectionSettings {
    pub(crate) fn set_head_timeout(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
        assign_deadline(&mut self.head_timeout, timeout)
    }

    pub(crate) fn set_body_progress_timeout(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
        assign_deadline(&mut self.body_progress_timeout, timeout)
    }

    pub(crate) fn set_write_progress_timeout(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
        assign_deadline(&mut self.write_progress_timeout, timeout)
    }

    pub(crate) fn set_drain_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
        crate::io::deadline::configured_after(Some(timeout))?;
        self.drain.timeout = timeout;
        Ok(())
    }

    pub(crate) fn set_read_buffer_limits(&mut self, preferred: usize, retained: usize) -> Result<(), Error> {
        if preferred == 0 || preferred > retained {
            return Err(Error::new(
                ErrorKind::LocalMessage,
                "receive limits must be nonzero and ordered",
            ));
        }
        self.preferred_read = preferred;
        self.max_retained = retained;
        Ok(())
    }
}

pub(crate) fn assign_deadline(slot: &mut Option<Duration>, timeout: Option<Duration>) -> Result<(), Error> {
    crate::io::deadline::configured_after(timeout)?;
    *slot = timeout;
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

    #[test]
    fn timeout_validation_preserves_configuration_on_rejection() {
        let mut settings = ConnectionSettings::default();
        macro_rules! check {
            ($method:ident, $field:ident) => {{
                let previous = settings.$field;
                assert_eq!(
                    settings.$method(Some(Duration::MAX)).unwrap_err().kind(),
                    ErrorKind::LocalMessage
                );
                assert_eq!(settings.$field, previous);
                for timeout in [None, Some(Duration::ZERO), Some(Duration::from_secs(1))] {
                    settings.$method(timeout).unwrap();
                    assert_eq!(settings.$field, timeout);
                }
            }};
        }
        let previous = settings.drain.timeout;
        assert_eq!(
            settings.set_drain_timeout(Duration::MAX).unwrap_err().kind(),
            ErrorKind::LocalMessage
        );
        assert_eq!(settings.drain.timeout, previous);
        settings.set_drain_timeout(Duration::ZERO).unwrap();
        assert_eq!(settings.drain.timeout, Duration::ZERO);
        check!(set_head_timeout, head_timeout);
        check!(set_body_progress_timeout, body_progress_timeout);
        check!(set_write_progress_timeout, write_progress_timeout);
    }
}
