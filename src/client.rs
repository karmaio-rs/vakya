//! Low-level HTTP clients over established Karmaio transports.
//!
//! Applications own dialing, driver execution, and retry policy. Accepted
//! bodies belong to the driver and continue independently of response delivery.
pub(crate) mod admission;
pub mod conn;
pub(crate) mod response;

pub use admission::{RequestPermit, SendError, SendRequest, SubmitError};
pub use response::{PendingResponse, ResponseEvent, UploadControl};
