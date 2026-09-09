use super::{decode::DecodeLimits, encode::EncodeLimits, head::HeadLimits};

/// Protocol budgets shared by the eventual client and server drivers.
#[derive(Clone, Copy, Debug)]
pub(super) struct Config {
    pub(super) head: HeadLimits,
    pub(super) decode: DecodeLimits,
    pub(super) encode: EncodeLimits,
    pub(super) max_informational: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            head: HeadLimits::new(64 * 1024, 100),
            decode: DecodeLimits::default(),
            encode: EncodeLimits::new(64 * 1024, 16 * 1024),
            max_informational: 16,
        }
    }
}
