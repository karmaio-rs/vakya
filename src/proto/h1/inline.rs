use karmaio::buf::IoBuf;

/// A small owned buffer whose initialized prefix is available for I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InlineBuf<const N: usize> {
    bytes: [u8; N],
    len: usize,
}

impl<const N: usize> InlineBuf<N> {
    pub(crate) const fn empty() -> Self {
        Self { bytes: [0; N], len: 0 }
    }

    pub(crate) const fn from_parts(bytes: [u8; N], len: usize) -> Self {
        assert!(len <= N, "initialized prefix exceeds inline buffer capacity");
        Self { bytes, len }
    }
}

impl<const N: usize> IoBuf for InlineBuf<N> {
    #[inline]
    fn as_init(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}
