//! Typed transport ownership after an HTTP protocol switch or CONNECT tunnel.
use bytes::{Buf, Bytes};
use karmaio::{
    buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf},
    io::{AsyncRead, AsyncWrite, IntoOwnedSplit},
};
use std::{fmt, io};

/// The HTTP transition that transferred transport ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpgradeKind {
    /// A validated 101 protocol switch.
    Protocol,
    /// A successful CONNECT response established a tunnel.
    Tunnel,
}

/// Settled transport halves plus bytes read beyond the HTTP message boundary.
/// Reads serve retained bytes before submitting transport reads. Writes are
/// forwarded directly; no HTTP parsing, framing, or background task remains.
pub struct Upgraded<R, W> {
    reader: R,
    writer: W,
    read_ahead: Bytes,
    kind: UpgradeKind,
}

impl<R, W> Upgraded<R, W> {
    #[cfg(any(feature = "client", feature = "server"))]
    pub(crate) fn new(reader: R, writer: W, read_ahead: Bytes, kind: UpgradeKind) -> Self {
        Self {
            reader,
            writer,
            read_ahead,
            kind,
        }
    }

    /// Return the transition that created this transport.
    pub fn kind(&self) -> UpgradeKind {
        self.kind
    }

    /// Inspect bytes waiting ahead of the transport's unread input.
    pub fn read_ahead(&self) -> &[u8] {
        &self.read_ahead
    }

    /// Recover the read half, write half, remaining read-ahead, and handoff kind.
    /// Consume the returned bytes before reading further from the read half.
    pub fn into_parts(self) -> (R, W, Bytes, UpgradeKind) {
        (self.reader, self.writer, self.read_ahead, self.kind)
    }
}

impl<R, W> fmt::Debug for Upgraded<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Upgraded")
            .field("kind", &self.kind)
            .field("read_ahead_bytes", &self.read_ahead.len())
            .finish_non_exhaustive()
    }
}

/// The readable half of an upgraded transport, including unread prefetched bytes.
/// Reads exhaust the retained prefix before accessing the original reader.
/// The original write half remains independently usable after splitting.
pub struct UpgradedReadHalf<R> {
    reader: R,
    read_ahead: Bytes,
}

impl<R> UpgradedReadHalf<R> {
    /// Inspect bytes waiting ahead of the transport's unread input.
    #[inline]
    pub fn read_ahead(&self) -> &[u8] {
        &self.read_ahead
    }

    /// Recover the original reader and remaining prefix.
    /// Consume the returned bytes before reading further from the reader.
    #[inline]
    pub fn into_parts(self) -> (R, Bytes) {
        (self.reader, self.read_ahead)
    }
}

impl<R> fmt::Debug for UpgradedReadHalf<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpgradedReadHalf")
            .field("read_ahead_bytes", &self.read_ahead.len())
            .finish_non_exhaustive()
    }
}

impl<R: AsyncRead + 'static, W: AsyncWrite + 'static> IntoOwnedSplit for Upgraded<R, W> {
    type ReadHalf = UpgradedReadHalf<R>;
    type WriteHalf = W;

    #[inline]
    fn into_split(self) -> (Self::ReadHalf, W) {
        (
            UpgradedReadHalf {
                reader: self.reader,
                read_ahead: self.read_ahead,
            },
            self.writer,
        )
    }
}

impl<R: AsyncRead> AsyncRead for UpgradedReadHalf<R> {
    #[inline]
    async fn read<B: IoBufMut>(&mut self, buffer: B) -> BufResult<usize, B> {
        read_prefixed(&mut self.reader, &mut self.read_ahead, buffer).await
    }
}

impl<R: AsyncRead, W> AsyncRead for Upgraded<R, W> {
    #[inline]
    async fn read<B: IoBufMut>(&mut self, buffer: B) -> BufResult<usize, B> {
        read_prefixed(&mut self.reader, &mut self.read_ahead, buffer).await
    }
}

async fn read_prefixed<R: AsyncRead, B: IoBufMut>(
    reader: &mut R,
    prefix: &mut Bytes,
    mut buffer: B,
) -> BufResult<usize, B> {
    if prefix.is_empty() {
        return reader.read(buffer).await;
    }
    let count = buffer.as_uninit().len().min(prefix.len());
    for (slot, byte) in buffer.as_uninit()[..count].iter_mut().zip(&prefix[..count]) {
        slot.write(*byte);
    }
    // SAFETY: precisely this prefix was initialized above; no pending I/O
    // references the buffer while retained bytes are copied into it.
    unsafe {
        buffer.set_len(count);
    }
    prefix.advance(count);
    BufResult(Ok(count), buffer)
}

impl<R, W: AsyncWrite> AsyncWrite for Upgraded<R, W> {
    async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
        self.writer.write(buffer).await
    }

    async fn write_vectored<B: IoVectoredBuf>(&mut self, buffers: B) -> BufResult<usize, B> {
        self.writer.write_vectored(buffers).await
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.writer.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_transport::{Gate, ReadStep, Reader, Writer};
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Waker},
    };

    #[test]
    fn splitting_preserves_partial_prefix_and_independent_halves() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let gate = Gate::default();
            let mut upgraded = Upgraded {
                reader: Reader::new([
                    ReadStep::Wait(gate.clone()),
                    ReadStep::Data(Bytes::from_static(b"wire")),
                ]),
                writer: Writer::new([]),
                read_ahead: Bytes::from_static(b"prefix"),
                kind: UpgradeKind::Tunnel,
            };
            let (result, bytes) = upgraded.read(Vec::with_capacity(2)).await.into_parts();
            assert_eq!(result.unwrap(), 2);
            assert_eq!(bytes, b"pr");
            let (mut reader, mut writer) = upgraded.into_split();
            assert_eq!(reader.read_ahead(), b"efix");
            let (result, bytes) = reader.read(Vec::with_capacity(0)).await.into_parts();
            assert_eq!(result.unwrap(), 0);
            assert!(bytes.is_empty());
            assert_eq!(reader.read_ahead(), b"efix");
            let (result, bytes) = reader.read(Vec::with_capacity(8)).await.into_parts();
            assert_eq!(result.unwrap(), 4);
            assert_eq!(bytes, b"efix");
            assert!(reader.read_ahead().is_empty());
            {
                let mut read = pin!(reader.read(Vec::with_capacity(8)));
                assert!(read.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
                let (result, bytes) = writer.write(Bytes::from_static(b"out")).await.into_parts();
                assert_eq!(result.unwrap(), 3);
                assert_eq!(bytes, b"out"[..]);
                drop(writer);
                gate.open();
                let (result, bytes) = read.await.into_parts();
                assert_eq!(result.unwrap(), 4);
                assert_eq!(bytes, b"wire");
            }
            let (_, prefix) = reader.into_parts();
            assert!(prefix.is_empty());
        });
    }
}
