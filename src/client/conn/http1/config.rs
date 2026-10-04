//! Private client configuration consumed by an HTTP/1 connection driver.
use crate::proto::h1::config::ConnectionSettings;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) settings: ConnectionSettings,
    pub(crate) continue_wait: Option<std::time::Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            settings: ConnectionSettings::default(),
            continue_wait: Some(std::time::Duration::from_secs(1)),
        }
    }
}
