//! Local transport ownership after an HTTP protocol switch or CONNECT tunnel.
use crate::error::{Error, ErrorKind};
use bytes::{Buf, Bytes};
use karmaio::{
    buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf, IoVectoredBufMut},
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

mod boxed;
pub use boxed::{BoxedReadHalf, BoxedWriteHalf};

/// A transport-independent local upgraded connection.
pub type BoxedUpgraded = Upgraded<BoxedReadHalf, BoxedWriteHalf>;
/// A claim for transport-independent local upgraded I/O.
pub type BoxedOnUpgrade = OnUpgrade<BoxedReadHalf, BoxedWriteHalf>;

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
/// It does not require `Send` or `Sync`. Typed claims return concrete halves;
/// [`BoxedOnUpgrade`] provides transport-independent local I/O.
#[must_use = "an upgrade future must be awaited to receive the transport"]
pub struct OnUpgrade<R, W> {
    shared: Option<Rc<dyn UpgradeStorage>>,
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
            let mut state = shared.upgrade_state().borrow_mut();
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
            let result = {
                let mut state = shared.upgrade_state().borrow_mut();
                if let ClaimPhase::Claimed { alive, .. } = &mut state.phase {
                    *alive = false;
                }
                state.waker = None;
                state.result.take()
            };
            // Transport destructors must run after releasing coordination state.
            drop(result);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClaimTarget {
    Typed(TypeId, TypeId),
    Boxed,
}

#[derive(Clone, Copy, Debug, Default)]
enum ClaimPhase {
    #[default]
    Unclaimed,
    Claimed {
        target: ClaimTarget,
        alive: bool,
    },
    Closed,
}

/// Dormant storage embedded in the existing request coordination allocation.
/// Erased transport storage is allocated only for successful claimed handoff.
#[derive(Default)]
pub(crate) struct UpgradeState {
    phase: ClaimPhase,
    result: Option<Result<Box<dyn Any>, Error>>,
    waker: Option<Waker>,
}

impl fmt::Debug for UpgradeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpgradeState")
            .field("phase", &self.phase)
            .field("has_result", &self.result.is_some())
            .finish_non_exhaustive()
    }
}

// This private access trait erases coordination owners, not I/O. Rc coercion
// preserves their existing allocation; it creates no secondary shared state.
pub(crate) trait UpgradeStorage {
    fn upgrade_state(&self) -> &RefCell<UpgradeState>;
}

pub(crate) struct UpgradeSlot {
    shared: Rc<dyn UpgradeStorage>,
}

impl fmt::Debug for UpgradeSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpgradeSlot").finish_non_exhaustive()
    }
}

impl UpgradeSlot {
    pub(crate) fn claim<R: 'static, W: 'static>(&mut self) -> OnUpgrade<R, W> {
        self.claim_target(ClaimTarget::Typed(TypeId::of::<R>(), TypeId::of::<W>()))
    }

    pub(crate) fn claim_boxed(&mut self) -> BoxedOnUpgrade {
        self.claim_target(ClaimTarget::Boxed)
    }

    fn claim_target<R: 'static, W: 'static>(&mut self, target: ClaimTarget) -> OnUpgrade<R, W> {
        let mut state = self.shared.upgrade_state().borrow_mut();
        match state.phase {
            ClaimPhase::Unclaimed => state.phase = ClaimPhase::Claimed { target, alive: true },
            ClaimPhase::Claimed { .. } => {
                return OnUpgrade::failed(Error::new(ErrorKind::Upgrade, "upgrade was already claimed"));
            }
            ClaimPhase::Closed => {
                return OnUpgrade::failed(Error::new(ErrorKind::Upgrade, "upgrade claim is no longer available"));
            }
        }
        drop(state);
        OnUpgrade {
            shared: Some(self.shared.clone()),
            immediate: None,
            marker: PhantomData,
        }
    }
}

pub(crate) struct PendingUpgrade {
    shared: Rc<dyn UpgradeStorage>,
    settled: bool,
}

impl PendingUpgrade {
    /// Deliver a settled handoff to a claimed future, or return it so the
    /// connection outcome can retain the pre-existing ownership path.
    pub(crate) fn fulfill<R: AsyncRead + 'static, W: AsyncWrite + 'static>(
        mut self,
        upgraded: Upgraded<R, W>,
    ) -> Result<UpgradeKind, Upgraded<R, W>> {
        let kind = upgraded.kind();
        self.settled = true;
        let actual = ClaimTarget::Typed(TypeId::of::<R>(), TypeId::of::<W>());
        let (result, wake) = {
            let mut state = self.shared.upgrade_state().borrow_mut();
            let phase = std::mem::replace(&mut state.phase, ClaimPhase::Closed);
            let result = match phase {
                ClaimPhase::Claimed { target, alive: true } if target == actual || target == ClaimTarget::Boxed => {
                    state.result = Some(Ok(if target == ClaimTarget::Boxed {
                        Box::new(upgraded.boxed())
                    } else {
                        Box::new(upgraded)
                    }));
                    Ok(kind)
                }
                ClaimPhase::Claimed { alive: true, .. } => {
                    state.result = Some(Err(Error::new(
                        ErrorKind::Upgrade,
                        "upgrade transport types do not match this connection",
                    )));
                    Err(upgraded)
                }
                _ => Err(upgraded),
            };
            (result, state.waker.take())
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn uses_storage(&self, owner: &dyn UpgradeStorage) -> bool {
        std::ptr::eq(self.shared.upgrade_state(), owner.upgrade_state())
    }
}

impl Drop for PendingUpgrade {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let wake = {
            let mut state = self.shared.upgrade_state().borrow_mut();
            let phase = std::mem::replace(&mut state.phase, ClaimPhase::Closed);
            if matches!(phase, ClaimPhase::Claimed { alive: true, .. }) {
                state.result = Some(Err(Error::new(
                    ErrorKind::Upgrade,
                    "message did not complete an upgrade",
                )));
            }
            state.waker.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

pub(crate) fn channel(shared: Rc<dyn UpgradeStorage>) -> (PendingUpgrade, UpgradeSlot) {
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

impl<R: AsyncRead + 'static, W: AsyncWrite + 'static> Upgraded<R, W> {
    /// Erase the concrete halves for local completion I/O, preserving read-ahead.
    /// This opt-in path boxes transports, buffers, and operation futures without
    /// copying payloads or adding `Send` bounds.
    pub fn boxed(self) -> BoxedUpgraded {
        let (reader, writer, read_ahead, kind) = self.into_parts();
        Upgraded {
            reader: BoxedReadHalf::new(reader),
            writer: BoxedWriteHalf::new(writer),
            read_ahead,
            kind,
        }
    }
}

impl BoxedUpgraded {
    /// Recover concrete transport halves when both requested types match.
    /// A mismatch returns this value unchanged, including remaining read-ahead.
    /// Types must be the original halves, before read-ahead wrapping by `into_split`.
    pub fn downcast<R: 'static, W: 'static>(self) -> Result<Upgraded<R, W>, Self> {
        if !self.reader.is::<R>() || !self.writer.is::<W>() {
            return Err(self);
        }
        let (reader, writer, read_ahead, kind) = self.into_parts();
        Ok(Upgraded {
            reader: reader.into_inner(),
            writer: writer.into_inner(),
            read_ahead,
            kind,
        })
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

    #[inline]
    async fn read_vectored<V: IoVectoredBufMut>(&mut self, buffers: V) -> BufResult<usize, V> {
        read_vectored_prefixed(&mut self.reader, &mut self.read_ahead, buffers).await
    }
}

impl<R: AsyncRead, W> AsyncRead for Upgraded<R, W> {
    #[inline]
    async fn read<B: IoBufMut>(&mut self, buffer: B) -> BufResult<usize, B> {
        read_prefixed(&mut self.reader, &mut self.read_ahead, buffer).await
    }

    #[inline]
    async fn read_vectored<V: IoVectoredBufMut>(&mut self, buffers: V) -> BufResult<usize, V> {
        read_vectored_prefixed(&mut self.reader, &mut self.read_ahead, buffers).await
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

async fn read_vectored_prefixed<R: AsyncRead, V: IoVectoredBufMut>(
    reader: &mut R,
    prefix: &mut Bytes,
    mut buffers: V,
) -> BufResult<usize, V> {
    if prefix.is_empty() {
        return reader.read_vectored(buffers).await;
    }
    let mut count = 0;
    for buffer in buffers.iter_uninit_slice() {
        let n = buffer.len().min(prefix.len() - count);
        for (slot, byte) in buffer[..n].iter_mut().zip(&prefix[count..count + n]) {
            slot.write(*byte);
        }
        count += n;
        if count == prefix.len() {
            break;
        }
    }
    // SAFETY: the loop initialized exactly the aggregate prefix of `count`
    // bytes in collection order, without an outstanding transport operation.
    unsafe {
        buffers.set_len(count);
    }
    prefix.advance(count);
    BufResult(Ok(count), buffers)
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

    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct WakeCount(AtomicUsize);
    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn channel() -> (PendingUpgrade, UpgradeSlot) {
        struct Owner(RefCell<UpgradeState>);
        impl UpgradeStorage for Owner {
            fn upgrade_state(&self) -> &RefCell<UpgradeState> {
                &self.0
            }
        }
        super::channel(Rc::new(Owner(RefCell::new(UpgradeState::default()))))
    }

    #[test]
    fn boxed_downcast_preserves_remaining_prefix_and_both_halves_on_mismatch() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut upgraded = Upgraded {
                reader: Reader::new([ReadStep::Data(Bytes::from_static(b"wire"))]),
                writer: Writer::new([]),
                read_ahead: Bytes::from_static(b"prefix"),
                kind: UpgradeKind::Tunnel,
            }
            .boxed();
            let (result, bytes) = upgraded
                .read_vectored([Vec::with_capacity(1), Vec::with_capacity(2)])
                .await
                .into_parts();
            assert_eq!(result.unwrap(), 3);
            assert_eq!(bytes[0], b"p");
            assert_eq!(bytes[1], b"re");
            let upgraded = upgraded.downcast::<Reader, ()>().unwrap_err();
            let upgraded = upgraded.downcast::<(), Writer>().unwrap_err();
            assert_eq!(upgraded.read_ahead(), b"fix");
            let mut upgraded = upgraded.downcast::<Reader, Writer>().unwrap();
            assert_eq!(upgraded.kind(), UpgradeKind::Tunnel);
            assert_eq!(upgraded.read_ahead(), b"fix");
            let (result, bytes) = upgraded.read(Vec::with_capacity(8)).await.into_parts();
            assert_eq!(result.unwrap(), 3);
            assert_eq!(bytes, b"fix");
            let (result, bytes) = upgraded.read(Vec::with_capacity(8)).await.into_parts();
            assert_eq!(result.unwrap(), 4);
            assert_eq!(bytes, b"wire");
        });
    }

    #[test]
    fn boxed_claim_delivery_and_abandonment_preserve_ownership() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let (pending, mut slot) = channel();
            let claim = slot.claim_boxed();
            assert_eq!(
                slot.claim::<Reader, Writer>().await.unwrap_err().kind(),
                ErrorKind::Upgrade
            );
            assert_eq!(
                pending
                    .fulfill(Upgraded {
                        reader: Reader::new([]),
                        writer: Writer::new([]),
                        read_ahead: Bytes::from_static(b"raw"),
                        kind: UpgradeKind::Protocol,
                    })
                    .unwrap(),
                UpgradeKind::Protocol
            );
            let upgraded = claim.await.unwrap().downcast::<Reader, Writer>().unwrap();
            assert_eq!(upgraded.read_ahead(), b"raw");

            for boxed in [false, true] {
                let (pending, mut slot) = channel();
                if boxed {
                    drop(slot.claim_boxed());
                } else {
                    drop(slot.claim::<Reader, Writer>());
                }
                let returned = pending
                    .fulfill(Upgraded {
                        reader: Reader::new([]),
                        writer: Writer::new([]),
                        read_ahead: Bytes::from_static(b"fallback"),
                        kind: UpgradeKind::Tunnel,
                    })
                    .unwrap_err();
                let (reader, writer, prefix, kind) = returned.into_parts();
                assert_eq!(reader.submissions, 0);
                assert_eq!(writer.submissions, 0);
                assert_eq!(prefix, b"fallback"[..]);
                assert_eq!(kind, UpgradeKind::Tunnel);
            }
        });
    }

    #[test]
    fn claims_after_unclaimed_handoff_or_driver_retirement_are_rejected() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let (pending, mut slot) = channel();
            let returned = pending
                .fulfill(Upgraded {
                    reader: Reader::new([]),
                    writer: Writer::new([]),
                    read_ahead: Bytes::from_static(b"returned"),
                    kind: UpgradeKind::Tunnel,
                })
                .unwrap_err();
            assert_eq!(returned.read_ahead(), b"returned");
            assert_eq!(slot.claim_boxed().await.unwrap_err().kind(), ErrorKind::Upgrade);
            assert_eq!(
                slot.claim::<Reader, Writer>().await.unwrap_err().kind(),
                ErrorKind::Upgrade
            );

            let (pending, mut slot) = channel();
            drop(pending);
            assert_eq!(slot.claim_boxed().await.unwrap_err().kind(), ErrorKind::Upgrade);
        });
    }

    #[test]
    fn mismatched_claim_is_woken_when_transport_returns_to_the_driver() {
        let count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(count.clone());
        let (pending, mut slot) = channel();
        let mut claim = pin!(slot.claim::<(), ()>());
        assert!(claim.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        let returned = pending
            .fulfill(Upgraded {
                reader: Reader::new([]),
                writer: Writer::new([]),
                read_ahead: Bytes::from_static(b"raw"),
                kind: UpgradeKind::Protocol,
            })
            .unwrap_err();
        let (reader, writer, prefix, kind) = returned.into_parts();
        assert_eq!(reader.submissions, 0);
        assert_eq!(writer.submissions, 0);
        assert_eq!(prefix, b"raw"[..]);
        assert_eq!(kind, UpgradeKind::Protocol);
        assert!(count.0.load(Ordering::Relaxed) > 0);
        assert!(
            matches!(claim.as_mut().poll(&mut Context::from_waker(&waker)), Poll::Ready(Err(error)) if error.kind() == ErrorKind::Upgrade)
        );
    }

    #[test]
    fn upgrade_claim_is_one_shot_and_non_upgrade_wakes_the_future() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let (pending, mut slot) = channel();
            let count = Arc::new(WakeCount(AtomicUsize::new(0)));
            let waker = Waker::from(count.clone());
            let mut upgrade = pin!(slot.claim::<(), ()>());
            assert!(upgrade.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
            let duplicate = slot.claim::<(), ()>();
            assert_eq!(duplicate.await.unwrap_err().kind(), ErrorKind::Upgrade);
            drop(pending);
            assert!(count.0.load(Ordering::Relaxed) > 0);
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
