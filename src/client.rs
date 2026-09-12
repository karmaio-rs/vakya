//! Low-level HTTP clients over established Karmaio transports.
//!
//! Applications own dialing, driver execution, and retry policy. Accepted
//! bodies belong to the driver and continue independently of response delivery.
pub mod conn;

pub use crate::engine::client::dispatch::{RequestPermit, SendError, SendRequest, SubmitError};
pub use crate::engine::client::response::{PendingResponse, ResponseEvent, UploadControl};
