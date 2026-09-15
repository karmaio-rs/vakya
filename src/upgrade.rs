//! Typed transport ownership after an HTTP protocol switch or CONNECT tunnel.
use crate::error::{Error, ErrorKind};
use bytes::{Buf, Bytes};
use karmaio::{
    buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf},
    io::{AsyncRead, AsyncWrite, IntoOwnedSplit},
};
use std::{
    any::{Any, TypeId},
    cell::RefCell,
    fmt,
    future::Future,
    io,
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

/// The HTTP transition that transferred transport ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpgradeKind {
    /// A validated 101 protocol switch.
    Protocol,
    /// A successful CONNECT response established a tunnel.
    Tunnel,
}

/// A future for transport ownership after an HTTP upgrade or CONNECT tunnel.
///
/// The future is obtained from the request-local server context or an accepted
/// client's pending response. It resolves only after the HTTP driver has
/// settled retained operations and transferred the concrete transport halves.
/// It does not require `Send` or `Sync`, and does not erase the returned transport.
///
/// This follows Hyper's request-correlated pending/fulfill model while keeping
/// Karmaio's owned-buffer I/O types visible in the result.
#[must_use = "an upgrade future must be awaited to receive the transport"]
pub struct OnUpgrade<R, W> {
    shared: Option<Rc<RefCell<UpgradeState>>>,
    immediate: Option<Error>,
    marker: PhantomData<fn() -> (R, W)>,
}

impl<R, W> OnUpgrade<R, W> {
    pub(crate) fn failed(error: Error) -> Self {
        Self {
            shared: None,
            immediate: Some(error),
            marker: PhantomData,
        }
    }
}

impl<R, W> fmt::Debug for OnUpgrade<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnUpgrade").finish_non_exhaustive()
    }
}

impl<R: 'static, W: 'static> Future for OnUpgrade<R, W> {
    type Output = Result<Upgraded<R, W>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        if let Some(error) = this.immediate.take() {
            return Poll::Ready(Err(error));
        }

        let Some(shared) = &this.shared else {
            return Poll::Ready(Err(Error::new(
                ErrorKind::Upgrade,
                "upgrade future polled after completion",
            )));
        };
        let result = {
            let mut state = shared.borrow_mut();
            match state.result.take() {
                Some(result) => Some(result),
                None => {
                    state.waker = Some(cx.waker().clone());
                    None
                }
            }
        };
        match result {
            Some(Ok(upgraded)) => {
                this.shared = None;
                Poll::Ready(
                    upgraded
                        .downcast::<Upgraded<R, W>>()
                        .map(|upgraded| *upgraded)
                        .map_err(|_| {
                            Error::new(ErrorKind::Upgrade, "upgrade transport types do not match this future")
                        }),
                )
            }
            Some(Err(error)) => {
                this.shared = None;
                Poll::Ready(Err(error))
            }
            None => Poll::Pending,
        }
    }
}

impl<R, W> Drop for OnUpgrade<R, W> {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            let mut state = shared.borrow_mut();
            state.receiver_alive = false;
            state.waker = None;
            state.result.take();
        }
    }
}

struct UpgradeState {
    claimed: bool,
    receiver_alive: bool,
    expected: Option<(TypeId, TypeId)>,
    result: Option<Result<Box<dyn Any>, Error>>,
    waker: Option<Waker>,
}

pub(crate) struct UpgradeSlot {
    shared: Rc<RefCell<UpgradeState>>,
}

impl fmt::Debug for UpgradeSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpgradeSlot").finish_non_exhaustive()
    }
}

impl UpgradeSlot {
    pub(crate) fn claim<R: 'static, W: 'static>(&mut self) -> OnUpgrade<R, W> {
        let mut state = self.shared.borrow_mut();
        if state.claimed {
            return OnUpgrade::failed(Error::new(ErrorKind::Upgrade, "upgrade was already claimed"));
        }
        state.claimed = true;
        state.receiver_alive = true;
        state.expected = Some((TypeId::of::<R>(), TypeId::of::<W>()));
        drop(state);
        OnUpgrade {
            shared: Some(self.shared.clone()),
            immediate: None,
            marker: PhantomData,
        }
    }
}

pub(crate) struct PendingUpgrade {
    shared: Rc<RefCell<UpgradeState>>,
    settled: bool,
}

impl PendingUpgrade {
    /// Deliver a settled handoff to a claimed future, or return it so the
    /// connection outcome can retain the pre-existing ownership path.
    pub(crate) fn fulfill<R: 'static, W: 'static>(
        mut self,
        upgraded: Upgraded<R, W>,
    ) -> Result<UpgradeKind, Upgraded<R, W>> {
        let kind = upgraded.kind();
        self.settled = true;
        let mut state = self.shared.borrow_mut();
        let actual = (TypeId::of::<R>(), TypeId::of::<W>());
        if state.claimed && state.receiver_alive && state.expected == Some(actual) {
            state.result = Some(Ok(Box::new(upgraded)));
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
            Ok(kind)
        } else {
            state.result = Some(Err(Error::new(
                ErrorKind::Upgrade,
                if state.claimed && state.receiver_alive {
                    "upgrade transport types do not match this connection"
                } else {
                    "upgrade was not claimed before handoff"
                },
            )));
            Err(upgraded)
        }
    }
}

impl Drop for PendingUpgrade {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let mut state = self.shared.borrow_mut();
        if state.result.is_none() {
            state.result = Some(Err(Error::new(
                ErrorKind::Upgrade,
                "message did not complete an upgrade",
            )));
        }
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}

pub(crate) fn channel() -> (PendingUpgrade, UpgradeSlot) {
    let shared = Rc::new(RefCell::new(UpgradeState {
        claimed: false,
        receiver_alive: false,
        expected: None,
        result: None,
        waker: None,
    }));
    (
        PendingUpgrade {
            shared: shared.clone(),
            settled: false,
        },
        UpgradeSlot { shared },
    )
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
    fn mismatched_or_dropped_claims_return_transport_to_connection_ownership() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let (pending, mut slot) = channel();
            let upgrade = slot.claim::<(), ()>();
            let handed_off = Upgraded {
                reader: 1u8,
                writer: 2u8,
                read_ahead: Bytes::from_static(b"raw"),
                kind: UpgradeKind::Protocol,
            };
            let returned = pending.fulfill(handed_off).unwrap_err();
            assert_eq!(
                returned.into_parts(),
                (1, 2, Bytes::from_static(b"raw"), UpgradeKind::Protocol)
            );
            assert_eq!(upgrade.await.unwrap_err().kind(), ErrorKind::Upgrade);

            let (pending, mut slot) = channel();
            let upgrade = slot.claim::<u8, u8>();
            drop(upgrade);
            let handed_off = Upgraded {
                reader: 3u8,
                writer: 4u8,
                read_ahead: Bytes::new(),
                kind: UpgradeKind::Tunnel,
            };
            assert!(pending.fulfill(handed_off).is_err());
        });
    }

    #[test]
    fn upgrade_claim_is_one_shot_and_non_upgrade_wakes_the_future() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let (pending, mut slot) = channel();
            let upgrade = slot.claim::<(), ()>();
            let duplicate = slot.claim::<(), ()>();
            assert_eq!(duplicate.await.unwrap_err().kind(), ErrorKind::Upgrade);
            drop(pending);
            assert_eq!(upgrade.await.unwrap_err().kind(), ErrorKind::Upgrade);
        });
    }

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
