//! Private operation erasure for the opt-in local upgraded transport.
// The buffer allocation remains the caller's. Only its owned value, iterator,
// and operation future are boxed; no intermediate payload storage is used.
use karmaio::{
    buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf, IoVectoredBufMut, SetLen},
    io::{AsyncRead, AsyncWrite},
};
use std::{any::Any, fmt, future::Future, io, mem::MaybeUninit, pin::Pin};

type Operation<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

trait Erased: Any {
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}
impl<T: Any> Erased for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}
trait OwnedBuffer: IoBuf + Erased {}
impl<B: IoBuf> OwnedBuffer for B {}
trait OwnedBufferMut: IoBufMut + Erased {}
impl<B: IoBufMut> OwnedBufferMut for B {}

type Buffer = Box<dyn OwnedBuffer>;
type BufferMut = Box<dyn OwnedBufferMut>;

trait OwnedVector: Erased {
    fn slices(&self) -> Box<dyn Iterator<Item = &[u8]> + '_>;
}
impl<V: IoVectoredBuf> OwnedVector for V {
    fn slices(&self) -> Box<dyn Iterator<Item = &[u8]> + '_> {
        Box::new(self.iter_slice())
    }
}
trait OwnedVectorMut: OwnedVector + SetLen {
    fn uninit_slices(&mut self) -> Box<dyn Iterator<Item = &mut [MaybeUninit<u8>]> + '_>;
}
impl<V: IoVectoredBufMut> OwnedVectorMut for V {
    fn uninit_slices(&mut self) -> Box<dyn Iterator<Item = &mut [MaybeUninit<u8>]> + '_> {
        Box::new(self.iter_uninit_slice())
    }
}
struct Vector(Box<dyn OwnedVector>);
struct VectorMut(Box<dyn OwnedVectorMut>);
impl IoVectoredBuf for Vector {
    #[inline]
    fn iter_slice(&self) -> impl Iterator<Item = &[u8]> {
        self.0.slices()
    }
}
impl IoVectoredBuf for VectorMut {
    #[inline]
    fn iter_slice(&self) -> impl Iterator<Item = &[u8]> {
        self.0.slices()
    }
}
impl IoVectoredBufMut for VectorMut {
    #[inline]
    fn iter_uninit_slice(&mut self) -> impl Iterator<Item = &mut [MaybeUninit<u8>]> {
        self.0.uninit_slices()
    }
}
impl SetLen for VectorMut {
    #[inline]
    unsafe fn set_len(&mut self, len: usize) {
        // SAFETY: the caller initialized the same aggregate prefix exposed by
        // the unchanged collection. Erasure does not alter capacity or order.
        unsafe { self.0.set_len(len) }
    }
}

trait Read: Erased {
    fn read(&mut self, buffer: BufferMut) -> Operation<'_, BufResult<usize, BufferMut>>;
    fn read_vectored(&mut self, buffers: VectorMut) -> Operation<'_, BufResult<usize, VectorMut>>;
}
impl<R: AsyncRead + 'static> Read for R {
    fn read(&mut self, buffer: BufferMut) -> Operation<'_, BufResult<usize, BufferMut>> {
        Box::pin(AsyncRead::read(self, buffer))
    }
    fn read_vectored(&mut self, buffers: VectorMut) -> Operation<'_, BufResult<usize, VectorMut>> {
        Box::pin(AsyncRead::read_vectored(self, buffers))
    }
}
trait Write: Erased {
    fn write(&mut self, buffer: Buffer) -> Operation<'_, BufResult<usize, Buffer>>;
    fn write_vectored(&mut self, buffers: Vector) -> Operation<'_, BufResult<usize, Vector>>;
    fn flush(&mut self) -> Operation<'_, io::Result<()>>;
    fn shutdown(&mut self) -> Operation<'_, io::Result<()>>;
}
impl<W: AsyncWrite + 'static> Write for W {
    fn write(&mut self, buffer: Buffer) -> Operation<'_, BufResult<usize, Buffer>> {
        Box::pin(AsyncWrite::write(self, buffer))
    }
    fn write_vectored(&mut self, buffers: Vector) -> Operation<'_, BufResult<usize, Vector>> {
        Box::pin(AsyncWrite::write_vectored(self, buffers))
    }
    fn flush(&mut self) -> Operation<'_, io::Result<()>> {
        Box::pin(AsyncWrite::flush(self))
    }
    fn shutdown(&mut self) -> Operation<'_, io::Result<()>> {
        Box::pin(AsyncWrite::shutdown(self))
    }
}

/// An owned, local read half with transport-independent completion I/O.
///
/// Buffers and operation futures are boxed on this opt-in path. Scalar and
/// vectored reads return the original concrete buffers on success and failure.
/// Cancellation has the underlying transport's contract: keep awaiting an
/// operation to recover its buffer. No `Send` bound or payload copy is added.
///
/// ```compile_fail
/// use vakya::upgrade::BoxedReadHalf;
/// fn require_send<T: Send>() {}
/// require_send::<BoxedReadHalf>();
/// ```
pub struct BoxedReadHalf {
    inner: Box<dyn Read>,
}
impl BoxedReadHalf {
    /// Erase an owned concrete reader without changing its I/O semantics.
    pub fn new<R: AsyncRead + 'static>(reader: R) -> Self {
        Self {
            inner: Box::new(reader),
        }
    }
    pub(super) fn is<R: 'static>(&self) -> bool {
        self.inner.as_ref().as_any().is::<R>()
    }
    pub(super) fn into_inner<R: 'static>(self) -> R {
        recover(self.inner.into_any())
    }
}
impl fmt::Debug for BoxedReadHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxedReadHalf").finish_non_exhaustive()
    }
}
impl AsyncRead for BoxedReadHalf {
    #[inline]
    async fn read<B: IoBufMut>(&mut self, buffer: B) -> BufResult<usize, B> {
        let (result, buffer) = self.inner.read(Box::new(buffer)).await.into_parts();
        BufResult(result, recover(buffer.into_any()))
    }
    #[inline]
    async fn read_vectored<V: IoVectoredBufMut>(&mut self, buffers: V) -> BufResult<usize, V> {
        let (result, buffers) = self
            .inner
            .read_vectored(VectorMut(Box::new(buffers)))
            .await
            .into_parts();
        BufResult(result, recover(buffers.0.into_any()))
    }
}

/// An owned, local write half with transport-independent completion I/O.
///
/// Scalar and vectored writes return the original concrete buffers on every
/// result. Erasure boxes buffer values and operation futures without copying
/// payloads or imposing `Send`. Flush and shutdown delegate to the transport.
pub struct BoxedWriteHalf {
    inner: Box<dyn Write>,
}
impl BoxedWriteHalf {
    /// Erase an owned concrete writer without changing its I/O semantics.
    pub fn new<W: AsyncWrite + 'static>(writer: W) -> Self {
        Self {
            inner: Box::new(writer),
        }
    }
    pub(super) fn is<W: 'static>(&self) -> bool {
        self.inner.as_ref().as_any().is::<W>()
    }
    pub(super) fn into_inner<W: 'static>(self) -> W {
        recover(self.inner.into_any())
    }
}
impl fmt::Debug for BoxedWriteHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxedWriteHalf").finish_non_exhaustive()
    }
}
impl AsyncWrite for BoxedWriteHalf {
    #[inline]
    async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
        let (result, buffer) = self.inner.write(Box::new(buffer)).await.into_parts();
        BufResult(result, recover(buffer.into_any()))
    }
    #[inline]
    async fn write_vectored<V: IoVectoredBuf>(&mut self, buffers: V) -> BufResult<usize, V> {
        let (result, buffers) = self.inner.write_vectored(Vector(Box::new(buffers))).await.into_parts();
        BufResult(result, recover(buffers.0.into_any()))
    }
    #[inline]
    async fn flush(&mut self) -> io::Result<()> {
        self.inner.flush().await
    }
    #[inline]
    async fn shutdown(&mut self) -> io::Result<()> {
        self.inner.shutdown().await
    }
}

// Every erasure boundary creates and returns the same owned concrete type.
// For transport halves, the caller checks both types before taking either.
fn recover<T: 'static>(value: Box<dyn Any>) -> T {
    *value
        .downcast::<T>()
        .unwrap_or_else(|_| unreachable!("private I/O erasure changed an owned type"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_transport::{Gate, ReadStep, Reader, WriteStep, Writer};
    use bytes::Bytes;
    use karmaio::runtime::{CancellationSource, FutureExt};
    use std::{
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    struct LocalBuffer {
        bytes: Vec<u8>,
        identity: Rc<()>,
    }
    impl IoBuf for LocalBuffer {
        fn as_init(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl IoBufMut for LocalBuffer {
        fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
            self.bytes.as_uninit()
        }
        fn reserve(&mut self, additional: usize) -> io::Result<()> {
            IoBufMut::reserve(&mut self.bytes, additional)
        }
    }
    impl SetLen for LocalBuffer {
        unsafe fn set_len(&mut self, len: usize) {
            // SAFETY: forwarded unchanged to the same initialized allocation.
            unsafe { SetLen::set_len(&mut self.bytes, len) }
        }
    }
    fn local(bytes: Vec<u8>) -> (LocalBuffer, Rc<()>, *const u8) {
        let pointer = bytes.as_ptr();
        let identity = Rc::new(());
        (
            LocalBuffer {
                bytes,
                identity: identity.clone(),
            },
            identity,
            pointer,
        )
    }
    fn unchanged(buffer: &LocalBuffer, identity: &Rc<()>, pointer: *const u8) {
        assert!(Rc::ptr_eq(&buffer.identity, identity));
        assert_eq!(buffer.bytes.as_ptr(), pointer);
    }

    #[test]
    fn scalar_success_and_failure_return_original_local_buffers() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for failure in [false, true] {
                let step = if failure {
                    ReadStep::Error(io::Error::other("read source"))
                } else {
                    ReadStep::Data(Bytes::from_static(b"abc"))
                };
                let mut reader = BoxedReadHalf::new(Reader::new([step]));
                let (buffer, identity, pointer) = local(Vec::with_capacity(8));
                let (result, buffer) = AsyncRead::read(&mut reader, buffer).await.into_parts();
                unchanged(&buffer, &identity, pointer);
                if failure {
                    assert_eq!(result.unwrap_err().to_string(), "read source");
                } else {
                    assert_eq!(result.unwrap(), 3);
                    assert_eq!(buffer.bytes, b"abc");
                }

                let steps = if failure {
                    vec![WriteStep::Error(io::Error::other("write source"))]
                } else {
                    vec![WriteStep::Limit(2)]
                };
                let mut writer = BoxedWriteHalf::new(Writer::new(steps));
                let (buffer, identity, pointer) = local(b"abcdef".to_vec());
                let (result, buffer) = AsyncWrite::write(&mut writer, buffer).await.into_parts();
                unchanged(&buffer, &identity, pointer);
                if failure {
                    assert_eq!(result.unwrap_err().to_string(), "write source");
                } else {
                    assert_eq!(result.unwrap(), 2);
                }
                assert_eq!(buffer.bytes, b"abcdef");
            }
        });
    }

    // Panicking scalar methods prove erasure preserves native vectored dispatch.
    struct NativeVectorRead {
        failure: bool,
    }
    impl AsyncRead for NativeVectorRead {
        async fn read<B: IoBufMut>(&mut self, _: B) -> BufResult<usize, B> {
            panic!("scalar fallback")
        }
        async fn read_vectored<V: IoVectoredBufMut>(&mut self, buffers: V) -> BufResult<usize, V> {
            if self.failure {
                return BufResult(Err(io::Error::other("vector read source")), buffers);
            }
            AsyncRead::read_vectored(&mut b"abcde".as_slice(), buffers).await
        }
    }
    struct NativeVectorWrite {
        failure: bool,
    }
    impl AsyncWrite for NativeVectorWrite {
        async fn write<B: IoBuf>(&mut self, _: B) -> BufResult<usize, B> {
            panic!("scalar fallback")
        }
        async fn write_vectored<V: IoVectoredBuf>(&mut self, buffers: V) -> BufResult<usize, V> {
            if self.failure {
                return BufResult(Err(io::Error::other("vector write source")), buffers);
            }
            let length = buffers.total_len();
            BufResult(Ok(length), buffers)
        }
        async fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        async fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn vectored_success_and_failure_preserve_collections_and_native_dispatch() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for failure in [false, true] {
                let mut reader = BoxedReadHalf::new(NativeVectorRead { failure });
                let (first, first_identity, first_pointer) = local(Vec::with_capacity(2));
                let (second, second_identity, second_pointer) = local(Vec::with_capacity(8));
                let (result, buffers) = AsyncRead::read_vectored(&mut reader, [first, second])
                    .await
                    .into_parts();
                unchanged(&buffers[0], &first_identity, first_pointer);
                unchanged(&buffers[1], &second_identity, second_pointer);
                if failure {
                    assert_eq!(result.unwrap_err().to_string(), "vector read source");
                } else {
                    assert_eq!(result.unwrap(), 5);
                    assert_eq!(buffers[0].bytes, b"ab");
                    assert_eq!(buffers[1].bytes, b"cde");
                }

                let mut writer = BoxedWriteHalf::new(NativeVectorWrite { failure });
                let (first, first_identity, first_pointer) = local(b"ab".to_vec());
                let (second, second_identity, second_pointer) = local(b"cde".to_vec());
                let (result, buffers) = AsyncWrite::write_vectored(&mut writer, vec![first, second])
                    .await
                    .into_parts();
                unchanged(&buffers[0], &first_identity, first_pointer);
                unchanged(&buffers[1], &second_identity, second_pointer);
                if failure {
                    assert_eq!(result.unwrap_err().to_string(), "vector write source");
                } else {
                    assert_eq!(result.unwrap(), 5);
                }
            }
        });
    }

    #[test]
    fn split_boxed_upgrade_preserves_prefix_then_native_vectored_dispatch() {
        use crate::upgrade::{UpgradeKind, Upgraded};
        use karmaio::io::IntoOwnedSplit;
        karmaio::Runtime::new().unwrap().block_on(async {
            let upgraded = Upgraded {
                reader: NativeVectorRead { failure: false },
                writer: NativeVectorWrite { failure: false },
                read_ahead: Bytes::from_static(b"raw"),
                kind: UpgradeKind::Protocol,
            }
            .boxed();
            let (mut reader, mut writer) = upgraded.into_split();
            let (result, _) = AsyncRead::read_vectored(&mut reader, [] as [Vec<u8>; 0])
                .await
                .into_parts();
            assert_eq!(result.unwrap(), 0);
            assert_eq!(reader.read_ahead(), b"raw");
            let (result, buffers) =
                AsyncRead::read_vectored(&mut reader, [Vec::with_capacity(2), Vec::with_capacity(2)])
                    .await
                    .into_parts();
            assert_eq!(result.unwrap(), 3);
            assert_eq!(buffers[0], b"ra");
            assert_eq!(buffers[1], b"w");
            let (result, buffers) =
                AsyncRead::read_vectored(&mut reader, [Vec::with_capacity(2), Vec::with_capacity(8)])
                    .await
                    .into_parts();
            assert_eq!(result.unwrap(), 5);
            assert_eq!(buffers[0], b"ab");
            assert_eq!(buffers[1], b"cde");
            let (result, _) =
                AsyncWrite::write_vectored(&mut writer, [Bytes::from_static(b"a"), Bytes::from_static(b"bc")])
                    .await
                    .into_parts();
            assert_eq!(result.unwrap(), 3);
            AsyncWrite::flush(&mut writer).await.unwrap();
            AsyncWrite::shutdown(&mut writer).await.unwrap();
        });
    }

    #[test]
    fn boxed_read_cancellation_waits_for_buffer_return_while_write_progresses() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let source = CancellationSource::new();
            let observed = Gate::default();
            let complete = Gate::default();
            let mut reader = BoxedReadHalf::new(Reader::new([
                ReadStep::AfterCancel {
                    token: source.token(),
                    observed: observed.clone(),
                    complete: complete.clone(),
                },
                ReadStep::Error(io::Error::other("settled read failure")),
            ]));
            let mut writer = BoxedWriteHalf::new(Writer::new([]));
            let (buffer, identity, pointer) = local(Vec::with_capacity(8));
            let mut operation = std::pin::pin!(AsyncRead::read(&mut reader, buffer).with_cancellation(source.token()));
            let mut cx = Context::from_waker(Waker::noop());
            assert!(operation.as_mut().poll(&mut cx).is_pending());
            source.cancel();
            assert!(operation.as_mut().poll(&mut cx).is_pending());
            assert!(observed.is_open());
            assert_eq!(Rc::strong_count(&identity), 2);
            let (result, bytes) = AsyncWrite::write(&mut writer, Bytes::from_static(b"out"))
                .await
                .into_parts();
            assert_eq!(result.unwrap(), 3);
            assert_eq!(bytes, b"out"[..]);
            complete.open();
            let Poll::Ready(result) = operation.as_mut().poll(&mut cx) else {
                panic!("read failed to settle")
            };
            let (result, buffer) = result.into_parts();
            unchanged(&buffer, &identity, pointer);
            assert_eq!(result.unwrap_err().to_string(), "settled read failure");
        });
    }

    #[test]
    fn boxed_write_cancellation_returns_the_original_buffer_after_settlement() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let source = CancellationSource::new();
            let observed = Gate::default();
            let complete = Gate::default();
            let mut writer = BoxedWriteHalf::new(Writer::new([
                WriteStep::AfterCancel {
                    token: source.token(),
                    observed: observed.clone(),
                    complete: complete.clone(),
                },
                WriteStep::Error(io::Error::other("settled write failure")),
            ]));
            let (buffer, identity, pointer) = local(b"payload".to_vec());
            let mut operation =
                std::pin::pin!(AsyncWrite::write(&mut writer, buffer).with_cancellation(source.token()));
            let mut cx = Context::from_waker(Waker::noop());
            assert!(operation.as_mut().poll(&mut cx).is_pending());
            source.cancel();
            assert!(operation.as_mut().poll(&mut cx).is_pending());
            assert!(observed.is_open());
            assert_eq!(Rc::strong_count(&identity), 2);
            complete.open();
            let Poll::Ready(result) = operation.as_mut().poll(&mut cx) else {
                panic!("write failed to settle")
            };
            let (result, buffer) = result.into_parts();
            unchanged(&buffer, &identity, pointer);
            assert_eq!(result.unwrap_err().to_string(), "settled write failure");
        });
    }
}
