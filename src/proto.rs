// The private protocol core is assembled before the connection drivers.
// Remove this allowance when those drivers consume it.
#[allow(dead_code)]
pub(crate) mod h1;
