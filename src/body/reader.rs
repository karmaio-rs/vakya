use super::{Body, Frame, SizeHint, TrailerHint};
use bytes::BytesMut;
use karmaio::{
    buf::{BufResult, IoBufExt},
    io::AsyncRead,
    runtime::{CancellationSource, FutureExt},
};
use std::{future::Future, io, pin::Pin};

const DEFAULT_CHUNK: usize = 16 * 1024;

/// An owned completion reader streamed as bounded `BytesMut` payload frames.
///
/// Length is unknown and trailers are forbidden. A read owns its reader and
/// buffer in a retained local boxed future. Dropping a `next_frame` wait leaves
/// that operation available for the next wait; no bytes are silently discarded.
/// One recovered buffer may be cached. If an earlier frame remains owned by the
/// application, a new bounded chunk is allocated when another read is demanded.
/// The application remains responsible for bounding its retained frames.
/// General user-defined bodies can still borrow; this owned adapter requires
/// `R: 'static` so an outstanding operation can outlive an individual frame wait.
/// Dropping the whole body retains Karmaio buffer safety without certifying
/// settlement. Use [`Self::into_parts`] and drive it to completion to recover I/O.
/// Frame waits and pending-read decomposition require an active Karmaio runtime.
///
/// ```compile_fail
/// use vakya::body::ReaderBody;
/// fn require_send<T: Send>() {}
/// require_send::<ReaderBody<std::io::Cursor<Vec<u8>>>>();
/// ```
pub struct ReaderBody<R> {
    reads: OwnedRead<R>,
    chunk_size: usize,
    done: bool,
}

/// A reader and its buffer after cancellation and settlement of any retained read.
/// On a raced successful completion, initialized bytes are preserved in `buffer`.
pub struct ReaderParts<R> {
    /// The original reader after its operation settled.
    pub reader: R,
    /// Recovered pending-read buffer, or an empty cached buffer when idle.
    pub buffer: BytesMut,
    /// The pending operation's validated result; `None` when no read was retained.
    pub result: Option<io::Result<usize>>,
}

impl<R: AsyncRead + 'static> ReaderBody<R> {
    /// Stream an owned reader with 16 KiB chunks allocated on demand.
    #[inline]
    pub fn new(reader: R) -> Self {
        Self {
            reads: OwnedRead::new(reader),
            chunk_size: DEFAULT_CHUNK,
            done: false,
        }
    }

    /// Stream with a nonzero chunk size that fits an owned buffer allocation.
    ///
    /// # Errors
    /// Returns `InvalidInput` for zero or sizes greater than `isize::MAX`.
    pub fn with_chunk_size(reader: R, chunk_size: usize) -> io::Result<Self> {
        if chunk_size == 0 || chunk_size > isize::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid reader body chunk size",
            ));
        }
        Ok(Self {
            reads: OwnedRead::new(reader),
            chunk_size,
            done: false,
        })
    }

    /// Cancel and settle any retained read, returning the original reader and buffer.
    ///
    /// A cancellation request can race with success; `result` preserves the actual
    /// completion. Failure does not lose the buffer. Dropping this decomposition
    /// future does not certify settlement or deliver the reader to the caller.
    /// This method does not read further, drain, flush, or shut down the transport.
    pub async fn into_parts(self) -> ReaderParts<R> {
        self.reads.into_parts().await
    }

    pub(super) async fn next_chunk(&mut self, width: usize) -> io::Result<Option<BytesMut>> {
        if self.done {
            return Ok(None);
        }
        let BufResult(result, buffer) = self.reads.read(width).await;
        match result {
            Ok(0) => {
                self.done = true;
                self.reads.recycle(buffer);
                Ok(None)
            }
            Ok(_) => Ok(Some(buffer)),
            Err(error) => {
                self.done = true;
                self.reads.recycle(buffer);
                Err(error)
            }
        }
    }
}

impl<R: AsyncRead + 'static> Body for ReaderBody<R> {
    type Data = BytesMut;
    type Error = io::Error;
    async fn next_frame(&mut self) -> io::Result<Option<Frame<BytesMut>>> {
        self.next_chunk(self.chunk_size)
            .await
            .map(|buffer| buffer.map(Frame::data))
    }
    #[inline]
    fn size_hint(&self) -> SizeHint {
        if self.done {
            SizeHint::with_exact(0)
        } else {
            SizeHint::new()
        }
    }
    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        TrailerHint::None
    }
    #[inline]
    fn is_end_stream(&self) -> bool {
        self.done
    }
    #[inline]
    fn recycle(&mut self, data: BytesMut) {
        // An application may grow an owned frame. Do not retain an allocation
        // larger than this producer's configured chunk bound when it returns.
        if data.capacity() <= self.chunk_size {
            self.reads.recycle(data);
        }
    }
}

// The owned future retains only one submitted read. This is shared by the file
// adapter through its concrete positional reader, without erasing transports.
type ReadOperation<R> = Pin<Box<dyn Future<Output = (R, BufResult<usize, BytesMut>)>>>;
enum State<R> {
    Idle(R),
    Pending {
        operation: ReadOperation<R>,
        cancel: CancellationSource,
        width: usize,
    },
}
struct OwnedRead<R> {
    state: Option<State<R>>,
    spare: Option<BytesMut>,
}
impl<R: AsyncRead + 'static> OwnedRead<R> {
    fn new(reader: R) -> Self {
        Self {
            state: Some(State::Idle(reader)),
            spare: None,
        }
    }

    async fn read(&mut self, width: usize) -> BufResult<usize, BytesMut> {
        if matches!(self.state, Some(State::Idle(_))) {
            let Some(State::Idle(mut reader)) = self.state.take() else {
                unreachable!()
            };
            let mut buffer = self.spare.take().unwrap_or_else(|| BytesMut::with_capacity(width));
            buffer.clear();
            buffer.reserve(width);
            let cancel = CancellationSource::new();
            let token = cancel.token();
            let operation = Box::pin(async move {
                let (result, view) = reader
                    .read(buffer.slice(..width))
                    .with_cancellation(token)
                    .await
                    .into_parts();
                (reader, BufResult(result, view.into_inner()))
            });
            self.state = Some(State::Pending {
                operation,
                cancel,
                width,
            });
        }
        let Some(State::Pending { operation, width, .. }) = self.state.as_mut() else {
            unreachable!()
        };
        let width = *width;
        let (reader, BufResult(result, buffer)) = operation.as_mut().await;
        self.state = Some(State::Idle(reader));
        BufResult(validate(result, &buffer, width), buffer)
    }

    fn recycle(&mut self, mut buffer: BytesMut) {
        buffer.clear();
        // Only one unsubmitted allocation is cached, including during a pending
        // read. Previously application-owned frames never alias its allocation.
        if self.spare.is_none() {
            self.spare = Some(buffer);
        }
    }

    async fn into_parts(mut self) -> ReaderParts<R> {
        match self.state.take().expect("owned reader state") {
            State::Idle(reader) => ReaderParts {
                reader,
                buffer: self.spare.take().unwrap_or_default(),
                result: None,
            },
            State::Pending {
                mut operation,
                cancel,
                width,
            } => {
                cancel.cancel();
                let (reader, BufResult(result, buffer)) = operation.as_mut().await;
                ReaderParts {
                    reader,
                    result: Some(validate(result, &buffer, width)),
                    buffer,
                }
            }
        }
    }
}
fn validate(result: io::Result<usize>, buffer: &BytesMut, width: usize) -> io::Result<usize> {
    if buffer.len() > width
        || result
            .as_ref()
            .is_ok_and(|&bytes| bytes > width || bytes != buffer.len())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "reader returned inconsistent initialized progress",
        ));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_transport::Gate;
    use karmaio::buf::IoBufMut;
    use std::{
        cell::RefCell,
        collections::VecDeque,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    struct Reader {
        input: VecDeque<u8>,
        gate: Gate,
        pointers: Rc<RefCell<Vec<usize>>>,
        widths: Rc<RefCell<Vec<usize>>>,
        failure: bool,
        report: Option<usize>,
    }
    impl AsyncRead for Reader {
        async fn read<B: IoBufMut>(&mut self, mut buffer: B) -> BufResult<usize, B> {
            let width = buffer.as_uninit().len();
            self.widths.borrow_mut().push(width);
            self.pointers.borrow_mut().push(buffer.as_uninit().as_ptr() as usize);
            self.gate.wait().await;
            let count = width.min(self.input.len());
            for slot in &mut buffer.as_uninit()[..count] {
                slot.write(self.input.pop_front().unwrap());
            }
            // SAFETY: exactly this prefix was initialized above within the view.
            unsafe { buffer.set_len(count) };
            let result = if self.failure {
                Err(io::Error::new(io::ErrorKind::ConnectionReset, "original read failure"))
            } else {
                Ok(self.report.unwrap_or(count))
            };
            BufResult(result, buffer)
        }
    }
    fn reader(input: &[u8], open: bool) -> Reader {
        Reader {
            input: input.iter().copied().collect(),
            gate: {
                let gate = Gate::default();
                if open {
                    gate.open();
                }
                gate
            },
            pointers: Default::default(),
            widths: Default::default(),
            failure: false,
            report: None,
        }
    }
    fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn bounded_chunks_recycle_and_application_owned_frames_keep_their_allocations() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let reader = reader(b"abcdef", true);
            let pointers = reader.pointers.clone();
            let widths = reader.widths.clone();
            let mut body = ReaderBody::with_chunk_size(reader, 2).unwrap();
            assert_eq!(body.size_hint(), SizeHint::new());
            assert_eq!(body.trailer_hint(), TrailerHint::None);
            let first = body.next_frame().await.unwrap().unwrap().into_data().unwrap();
            let address = first.as_ptr();
            let second = body.next_frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(&first[..], b"ab");
            assert_eq!(&second[..], b"cd");
            assert_ne!(address, second.as_ptr());
            body.recycle(first);
            body.recycle(second);
            let third = body.next_frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(third.as_ptr(), address);
            assert_eq!(&third[..], b"ef");
            body.recycle(third);
            assert!(body.next_frame().await.unwrap().is_none());
            assert!(body.is_end_stream());
            assert!(body.next_frame().await.unwrap().is_none());
            assert_eq!(&*widths.borrow(), &[2, 2, 2, 2]);
            assert_eq!(pointers.borrow()[0], pointers.borrow()[2]);
            let parts = body.into_parts().await;
            assert!(parts.result.is_none());
            assert!(parts.buffer.is_empty());
        });
    }

    #[test]
    fn dropping_a_frame_wait_retains_the_same_read_until_resumed() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let reader = reader(b"payload", false);
            let gate = reader.gate.clone();
            let pointers = reader.pointers.clone();
            let mut body = ReaderBody::new(reader);
            {
                let mut next = std::pin::pin!(body.next_frame());
                assert!(poll(next.as_mut()).is_pending());
            }
            assert_eq!(pointers.borrow().len(), 1);
            gate.open();
            let frame = body.next_frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(&frame[..], b"payload");
            assert_eq!(frame.as_ptr() as usize, pointers.borrow()[0]);
            assert_eq!(pointers.borrow().len(), 1);
            body.recycle(frame);
            assert!(body.next_frame().await.unwrap().is_none());
        });
    }

    #[test]
    fn decomposition_cancels_then_settles_success_and_failure_without_losing_buffer() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for failure in [false, true] {
                let mut reader = reader(b"x", false);
                reader.failure = failure;
                let gate = reader.gate.clone();
                let pointers = reader.pointers.clone();
                let mut body = ReaderBody::new(reader);
                {
                    let mut next = std::pin::pin!(body.next_frame());
                    assert!(poll(next.as_mut()).is_pending());
                }
                let token = match body.reads.state.as_ref().unwrap() {
                    State::Pending { cancel, .. } => cancel.token(),
                    _ => panic!("missing retained read"),
                };
                let mut parts = std::pin::pin!(body.into_parts());
                assert!(poll(parts.as_mut()).is_pending());
                assert!(token.is_cancel_requested());
                gate.open();
                let parts = parts.await;
                assert_eq!(&parts.buffer[..], b"x");
                assert_eq!(parts.buffer.as_ptr() as usize, pointers.borrow()[0]);
                if failure {
                    let error = parts.result.unwrap().unwrap_err();
                    assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
                    assert_eq!(error.to_string(), "original read failure");
                } else {
                    assert_eq!(parts.result.unwrap().unwrap(), 1);
                }
                assert_eq!(parts.reader.widths.borrow().len(), 1);
            }
        });
    }

    #[test]
    fn invalid_sizes_and_inconsistent_progress_are_rejected_and_errors_fuse() {
        assert!(ReaderBody::with_chunk_size(reader(b"", true), 0).is_err());
        assert!(ReaderBody::with_chunk_size(reader(b"", true), usize::MAX).is_err());
        karmaio::Runtime::new().unwrap().block_on(async {
            for report in [0, 2, 3] {
                let mut reader = reader(b"x", true);
                reader.report = Some(report);
                let mut body = ReaderBody::with_chunk_size(reader, 2).unwrap();
                assert_eq!(body.next_frame().await.unwrap_err().kind(), io::ErrorKind::InvalidData);
                assert!(body.next_frame().await.unwrap().is_none());
                assert!(body.is_end_stream());
                assert_eq!(body.into_parts().await.reader.widths.borrow().len(), 1);
            }
            let mut reader = reader(b"", true);
            reader.failure = true;
            let mut body = ReaderBody::new(reader);
            assert_eq!(
                body.next_frame().await.unwrap_err().kind(),
                io::ErrorKind::ConnectionReset
            );
            assert!(body.next_frame().await.unwrap().is_none());
        });
    }
}
