//! Private server configuration consumed by an HTTP/1 connection driver.
use crate::proto::h1::config::ConnectionSettings;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) settings: ConnectionSettings,
    pub(crate) auto_date: bool,
    pub(crate) auto_error_response: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            settings: ConnectionSettings {
                head_timeout: Some(std::time::Duration::from_secs(30)),
                ..ConnectionSettings::default()
            },
            auto_date: true,
            auto_error_response: true,
        }
    }
}
