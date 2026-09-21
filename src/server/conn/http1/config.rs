//! Private server configuration consumed by an HTTP/1 connection driver.
use crate::proto::h1::config::Config as ProtocolConfig;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) protocol: ProtocolConfig,
    pub(crate) drain: crate::body::incoming::DrainConfig,
    #[cfg(feature = "tls")]
    pub(crate) tls_info: Option<crate::tls::TlsInfo>,
    pub(crate) head_timeout: Option<std::time::Duration>,
    pub(crate) body_progress_timeout: Option<std::time::Duration>,
    pub(crate) write_progress_timeout: Option<std::time::Duration>,
    pub(crate) preferred_read: usize,
    pub(crate) max_retained: usize,
    pub(crate) auto_date: bool,
    pub(crate) auto_error_response: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            protocol: ProtocolConfig::default(),
            drain: crate::body::incoming::DrainConfig::default(),
            #[cfg(feature = "tls")]
            tls_info: None,
            head_timeout: Some(std::time::Duration::from_secs(30)),
            body_progress_timeout: None,
            write_progress_timeout: None,
            preferred_read: 16 * 1024,
            max_retained: 128 * 1024,
            auto_date: true,
            auto_error_response: true,
        }
    }
}
