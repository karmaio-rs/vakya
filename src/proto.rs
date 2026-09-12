// Shared parsing/framing supports either role independently. Check for unused
// protocol code when both roles are enabled; single-role builds retain helpers
// needed by the other role and by the protocol tests.
#[cfg_attr(not(all(feature = "client", feature = "server")), allow(dead_code))]
pub(crate) mod h1;
