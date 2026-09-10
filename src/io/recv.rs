use crate::{Error, ErrorKind};
use bytes::{Buf, Bytes, BytesMut};
use karmaio::{
    buf::IoBufExt,
    io::AsyncRead,
    runtime::{CancellationToken, FutureExt, operation_canceled},
};
use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadStatus {
    Data(usize),
    Eof,
}

/// Retained, initialized input for incremental parsing. Payload extraction
/// shares the allocation; subsequent reads cannot overwrite retained payloads.
/// The logical bound applies to input retained here, not application-held data
/// or allocator rounding. Body demand applies the separate delivery bound.
#[derive(Debug)]
pub(crate) struct RecvBuffer {
    bytes: BytesMut,
    preferred_read: usize,
    max_retained: usize,
}

impl RecvBuffer {
    pub(crate) fn new(preferred_read: usize, max_retained: usize) -> Result<Self, Error> {
        if preferred_read == 0 || preferred_read > max_retained {
            return Err(Error::new(
                ErrorKind::Limit,
                "receive limits must be nonzero and ordered",
            ));
        }
        Ok(Self {
            bytes: BytesMut::with_capacity(preferred_read),
            preferred_read,
            max_retained,
        })
    }

    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn consume(&mut self, count: usize) -> Result<(), Error> {
        if count > self.bytes.len() {
            return Err(Error::new(
                ErrorKind::Internal,
                "receive buffer consumed beyond initialized bytes",
            ));
        }
        self.bytes.advance(count);
        self.compact_if_empty();
        Ok(())
    }

    /// Transfer payload ownership, discard its framing, and retain read-ahead.
    pub(crate) fn take_payload(&mut self, payload: Range<usize>, consumed: usize) -> Result<Bytes, Error> {
        if payload.start > payload.end || payload.end > consumed || consumed > self.bytes.len() {
            return Err(Error::new(
                ErrorKind::Internal,
                "decoder returned an invalid receive-buffer range",
            ));
        }
        let mut bytes = self.bytes.split_to(consumed).freeze();
        bytes.truncate(payload.end);
        bytes.advance(payload.start);
        self.compact_if_empty();
        Ok(bytes)
    }

    /// Transfer all read-ahead without copying, including upgraded protocol data.
    pub(crate) fn take_all(&mut self) -> Bytes {
        std::mem::take(&mut self.bytes).freeze()
    }

    /// Append one bounded completion read. Taking `self` prevents accidental
    /// reuse of an emptied receive buffer if this future is dropped while I/O
    /// owns its allocation. Only awaiting completion recovers the buffer.
    pub(crate) async fn read<R: AsyncRead>(
        mut self,
        reader: &mut R,
        cancellation: Option<CancellationToken>,
    ) -> (Result<ReadStatus, Error>, Self) {
        if cancellation.is_some_and(|token| token.is_cancel_requested()) {
            return (Err(Error::from(operation_canceled())), self);
        }
        let available = self.max_retained - self.bytes.len();
        if available == 0 {
            return (Err(Error::new(ErrorKind::Limit, "receive buffer limit reached")), self);
        }
        let target = available.min(self.preferred_read);
        let start = self.bytes.len();
        // target <= max_retained - start, so this addition cannot overflow.
        let end = start + target;
        self.bytes.reserve(target);
        let slice = self.bytes.slice(start..end);
        let (result, slice) = match cancellation {
            Some(token) => reader.read(slice).with_cancellation(token).await.into_parts(),
            None => reader.read(slice).await.into_parts(),
        };
        self.bytes = slice.into_inner();
        let status = match result {
            Ok(count) if count > target || self.bytes.len() != start + count => Err(Error::new(
                ErrorKind::Io,
                "reader reported an inconsistent initialized length",
            )),
            Ok(0) => Ok(ReadStatus::Eof),
            Ok(count) => Ok(ReadStatus::Data(count)),
            Err(error) => Err(Error::from(error)),
        };
        (status, self)
    }

    fn compact_if_empty(&mut self) {
        let threshold = self.preferred_read.saturating_mul(4).min(self.max_retained);
        if self.bytes.is_empty() && self.bytes.capacity() > threshold {
            self.bytes = BytesMut::with_capacity(self.preferred_read);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        io::transport::{Portable, Receive},
        test_transport::{Gate, ReadStep, Reader},
    };
    use karmaio::runtime::CancellationSource;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    #[test]
    fn short_reads_obey_retained_bound_and_eof_preserves_read_ahead() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut reader = Reader::new([
                ReadStep::Data(Bytes::from_static(b"ab")),
                ReadStep::Data(Bytes::from_static(b"cdefgh")),
            ]);
            let buffer = RecvBuffer::new(3, 7).unwrap();
            let (result, buffer) = Portable.read(&mut reader, buffer, None).await;
            assert_eq!(result.unwrap(), ReadStatus::Data(2));
            let (result, buffer) = buffer.read(&mut reader, None).await;
            assert_eq!(result.unwrap(), ReadStatus::Data(3));
            let (result, buffer) = buffer.read(&mut reader, None).await;
            assert_eq!(result.unwrap(), ReadStatus::Data(2));
            assert_eq!(buffer.bytes(), b"abcdefg");
            let (result, mut buffer) = buffer.read(&mut reader, None).await;
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Limit);
            assert_eq!(reader.submissions, 3);
            assert_eq!(reader.capacities, [3, 3, 2]);
            buffer.consume(4).unwrap();
            let (result, buffer) = buffer.read(&mut reader, None).await;
            assert_eq!(result.unwrap(), ReadStatus::Data(1));
            let (result, buffer) = buffer.read(&mut reader, None).await;
            assert_eq!(result.unwrap(), ReadStatus::Eof);
            assert_eq!(buffer.bytes(), b"efgh");
        });
    }

    #[test]
    fn payload_extraction_shares_storage_and_preserves_framing_boundaries() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut reader = Reader::new([
                ReadStep::Data(Bytes::from_static(b"HEAD4\r\nbody\r\nNEXT")),
                ReadStep::Data(Bytes::from_static(b"later")),
            ]);
            let buffer = RecvBuffer::new(32, 32).unwrap();
            let (result, mut buffer) = buffer.read(&mut reader, None).await;
            result.unwrap();
            buffer.consume(4).unwrap();
            let pointer = buffer.bytes()[3..].as_ptr();
            let body = buffer.take_payload(3..7, 9).unwrap();
            assert_eq!(body.as_ptr(), pointer);
            assert_eq!(body, "body");
            assert_eq!(buffer.bytes(), b"NEXT");
            let (result, mut buffer) = buffer.read(&mut reader, None).await;
            result.unwrap();
            assert_eq!(body, "body");
            let pointer = buffer.bytes().as_ptr();
            let read_ahead = buffer.take_all();
            assert_eq!(read_ahead, "NEXTlater");
            assert_eq!(read_ahead.as_ptr(), pointer);
        });
    }

    #[test]
    fn invalid_decoder_ranges_do_not_mutate_retained_input() {
        let mut buffer = RecvBuffer::new(8, 16).unwrap();
        buffer.bytes.extend_from_slice(b"bytes");
        assert_eq!(buffer.take_payload(1..5, 4).unwrap_err().kind(), ErrorKind::Internal);
        assert_eq!(buffer.consume(6).unwrap_err().kind(), ErrorKind::Internal);
        assert_eq!(buffer.bytes(), b"bytes");
    }

    #[test]
    fn pending_cancellation_recovers_prefix_only_after_actual_completion() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for success in [false, true] {
                let source = CancellationSource::new();
                let observed = Gate::default();
                let complete = Gate::default();
                let mut reader = Reader::new([
                    ReadStep::Data(Bytes::from_static(b"prefix")),
                    ReadStep::AfterCancel {
                        token: source.token(),
                        observed: observed.clone(),
                        complete: complete.clone(),
                    },
                    if success {
                        ReadStep::Data(Bytes::from_static(b"x"))
                    } else {
                        ReadStep::Error(operation_canceled())
                    },
                ]);
                let buffer = RecvBuffer::new(8, 16).unwrap();
                let (_, buffer) = buffer.read(&mut reader, None).await;
                let mut future = pin!(buffer.read(&mut reader, Some(source.token())));
                let mut cx = Context::from_waker(Waker::noop());
                assert!(future.as_mut().poll(&mut cx).is_pending());
                source.cancel();
                assert!(future.as_mut().poll(&mut cx).is_pending());
                assert!(observed.is_open());
                complete.open();
                let Poll::Ready((result, buffer)) = future.as_mut().poll(&mut cx) else {
                    panic!("completion still pending")
                };
                if success {
                    assert_eq!(result.unwrap(), ReadStatus::Data(1));
                    assert_eq!(buffer.bytes(), b"prefixx");
                } else {
                    let error = result.unwrap_err();
                    assert_eq!(error.kind(), ErrorKind::Canceled);
                    assert!(std::error::Error::source(&error).is_some());
                    assert_eq!(buffer.bytes(), b"prefix");
                }
            }
        });
    }

    #[test]
    fn pre_submission_cancel_and_io_error_preserve_existing_input() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let source = CancellationSource::new();
            source.cancel();
            let mut reader = Reader::new([ReadStep::Error(std::io::Error::other("failed"))]);
            let mut buffer = RecvBuffer::new(8, 16).unwrap();
            buffer.bytes.extend_from_slice(b"prefix");
            let (result, buffer) = buffer.read(&mut reader, Some(source.token())).await;
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Canceled);
            assert_eq!(reader.submissions, 0);
            assert_eq!(buffer.bytes(), b"prefix");
            let (result, buffer) = buffer.read(&mut reader, None).await;
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Io);
            assert_eq!(buffer.bytes(), b"prefix");
        });
    }
}
