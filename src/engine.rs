//! Private connection configuration and application/driver coordination.
#[cfg(feature = "client")]
pub(crate) mod client;
pub(crate) mod config;
#[cfg(feature = "server")]
pub(crate) mod server;
