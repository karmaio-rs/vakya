// HTTP/1 can be selected without a client or server driver.
#[allow(dead_code)]
pub(crate) mod recv;
#[allow(dead_code)]
pub(crate) mod send;
#[allow(dead_code)]
pub(crate) mod transport;

pub(crate) mod deadline;

#[cfg(target_os = "linux")]
pub(crate) mod managed;
