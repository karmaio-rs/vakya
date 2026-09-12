//! Value-owned engine settings, independent of public builder shapes.
use crate::proto::h1::config::Config;

#[cfg(feature = "client")]
#[derive(Clone, Debug)]
pub(crate) struct ClientConfig {
    pub(crate) protocol: Config,
    pub(crate) drain: crate::body::incoming::DrainConfig,
    #[cfg(feature = "tls")]
    pub(crate) tls_info: Option<crate::tls::TlsInfo>,
    pub(crate) head_timeout: Option<std::time::Duration>,
    pub(crate) body_progress_timeout: Option<std::time::Duration>,
    pub(crate) write_progress_timeout: Option<std::time::Duration>,
    pub(crate) preferred_read: usize,
    pub(crate) max_retained: usize,
    pub(crate) continue_wait: Option<std::time::Duration>,
}

#[cfg(feature = "client")]
impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            protocol: Config::default(),
            drain: crate::body::incoming::DrainConfig::default(),
            #[cfg(feature = "tls")]
            tls_info: None,
            head_timeout: None,
            body_progress_timeout: None,
            write_progress_timeout: None,
            preferred_read: 16 * 1024,
            max_retained: 128 * 1024,
            continue_wait: None,
        }
    }
}

#[cfg(feature = "server")]
#[derive(Clone, Debug)]
pub(crate) struct ServerConfig {
    pub(crate) protocol: Config,
    pub(crate) drain: crate::body::incoming::DrainConfig,
    #[cfg(feature = "tls")]
    pub(crate) tls_info: Option<crate::tls::TlsInfo>,
    pub(crate) head_timeout: Option<std::time::Duration>,
    pub(crate) body_progress_timeout: Option<std::time::Duration>,
    pub(crate) write_progress_timeout: Option<std::time::Duration>,
    pub(crate) preferred_read: usize,
    pub(crate) max_retained: usize,
    pub(crate) auto_date: bool,
}

#[cfg(feature = "server")]
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            protocol: Config::default(),
            drain: crate::body::incoming::DrainConfig::default(),
            #[cfg(feature = "tls")]
            tls_info: None,
            head_timeout: Some(std::time::Duration::from_secs(30)),
            body_progress_timeout: None,
            write_progress_timeout: None,
            preferred_read: 16 * 1024,
            max_retained: 128 * 1024,
            auto_date: true,
        }
    }
}
