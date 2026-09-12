#[cfg(target_os = "linux")]
use crate::body::incoming::ManagedLeasePermit;
use crate::{Error, ErrorKind, IncomingData};
use bytes::{Buf, Bytes, BytesMut};
use karmaio::{
    buf::IoBufExt,
    io::AsyncRead,
    runtime::{CancellationToken, FutureExt, operation_canceled},
};
#[cfg(target_os = "linux")]
use karmaio::{
    buf::PooledBuf,
    net::{split::OwnedReadHalf, tcp::TcpStream},
};
use std::mem;
use std::ops::Range;
#[cfg(target_os = "linux")]
use std::rc::Rc;

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
    storage: ReceiveStorage,
    #[cfg(target_os = "linux")]
    managed_permit: Option<Rc<ManagedLeasePermit>>,
    preferred_read: usize,
    max_retained: usize,
}

#[derive(Debug)]
enum ReceiveStorage {
    Portable(BytesMut),
    #[cfg(target_os = "linux")]
    Managed {
        buffer: PooledBuf,
        range: Range<usize>,
    },
}

impl ReceiveStorage {
    #[inline]
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Portable(bytes) => bytes,
            #[cfg(target_os = "linux")]
            Self::Managed { buffer, range } => &buffer[range.clone()],
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.bytes().len()
    }
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
            storage: ReceiveStorage::Portable(BytesMut::with_capacity(preferred_read)),
            #[cfg(target_os = "linux")]
            managed_permit: None,
            preferred_read,
            max_retained,
        })
    }

    /// Returns the initialized bytes available to a synchronous parser.
    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        self.storage.bytes()
    }

    /// Returns the initialized byte count.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.storage.len()
    }

    /// Removes a parsed control-data prefix while preserving later bytes.
    pub(crate) fn consume(&mut self, count: usize) -> Result<(), Error> {
        if count > self.len() {
            return Err(Error::new(
                ErrorKind::Internal,
                "receive buffer consumed beyond initialized bytes",
            ));
        }

        match &mut self.storage {
            ReceiveStorage::Portable(bytes) => bytes.advance(count),
            #[cfg(target_os = "linux")]
            ReceiveStorage::Managed { range, .. } => range.start += count,
        }

        self.compact_if_empty();

        Ok(())
    }

    /// Transfers a decoded payload range while discarding framing it consumed.
    pub(crate) fn take_payload(&mut self, payload: Range<usize>, consumed: usize) -> Result<IncomingData, Error> {
        if payload.start > payload.end || payload.end > consumed || consumed > self.len() {
            return Err(Error::new(
                ErrorKind::Internal,
                "decoder returned an invalid receive-buffer range",
            ));
        }

        let storage = mem::replace(&mut self.storage, ReceiveStorage::Portable(BytesMut::new()));
        match storage {
            ReceiveStorage::Portable(mut bytes) => {
                let mut consumed_bytes = bytes.split_to(consumed).freeze();
                consumed_bytes.truncate(payload.end);
                consumed_bytes.advance(payload.start);
                let payload = IncomingData::from_bytes(consumed_bytes);
                self.storage = ReceiveStorage::Portable(bytes);
                self.compact_if_empty();
                Ok(payload)
            }
            #[cfg(target_os = "linux")]
            ReceiveStorage::Managed { buffer, range } => {
                let absolute_payload = range.start + payload.start..range.start + payload.end;
                let suffix = range.start + consumed..range.end;
                if !suffix.is_empty() {
                    self.storage = ReceiveStorage::Portable(BytesMut::from(&buffer[suffix]));
                }
                IncomingData::from_managed(
                    buffer,
                    absolute_payload,
                    Rc::clone(
                        self.managed_permit
                            .as_ref()
                            .expect("managed storage has a lease permit"),
                    ),
                )
            }
        }
    }

    /// Transfers portable read-ahead; managed read-ahead is copied into `Bytes`.
    pub(crate) fn take_all(&mut self) -> Bytes {
        let replacement = ReceiveStorage::Portable(BytesMut::new());

        match mem::replace(&mut self.storage, replacement) {
            ReceiveStorage::Portable(bytes) => bytes.freeze(),
            #[cfg(target_os = "linux")]
            ReceiveStorage::Managed { buffer, range } => Bytes::copy_from_slice(&buffer[range]),
        }
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

        self.make_portable();
        let available = self.max_retained - self.len();

        if available == 0 {
            return (Err(Error::new(ErrorKind::Limit, "receive buffer limit reached")), self);
        }

        let target = available.min(self.preferred_read);
        let start = self.len();
        // target <= max_retained - start, so this addition cannot overflow.
        let end = start + target;

        #[allow(clippy::infallible_destructuring_match)] // Linux also has managed storage.
        let mut bytes = match mem::replace(&mut self.storage, ReceiveStorage::Portable(BytesMut::new())) {
            ReceiveStorage::Portable(bytes) => bytes,
            #[cfg(target_os = "linux")]
            ReceiveStorage::Managed { .. } => unreachable!("converted above"),
        };

        bytes.reserve(target);
        let slice = bytes.slice(start..end);

        let (result, slice) = match cancellation {
            Some(token) => reader.read(slice).with_cancellation(token).await.into_parts(),
            None => reader.read(slice).await.into_parts(),
        };

        self.storage = ReceiveStorage::Portable(slice.into_inner());

        let status = match result {
            Ok(count) if count > target || self.len() != start + count => Err(Error::new(
                ErrorKind::Io,
                "reader reported an inconsistent initialized length",
            )),
            Ok(0) => Ok(ReadStatus::Eof),
            Ok(count) => {
                crate::trace::progress("read", count);
                Ok(ReadStatus::Data(count))
            }
            Err(error) => Err(Error::from(error)),
        };

        (status, self)
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn read_managed(
        mut self,
        reader: &mut OwnedReadHalf<TcpStream>,
        cancellation: Option<CancellationToken>,
    ) -> (Result<ReadStatus, Error>, Self) {
        if cancellation.is_some_and(|token| token.is_cancel_requested()) {
            return (Err(Error::from(operation_canceled())), self);
        }

        if self.managed_permit.as_ref().is_some_and(|permit| permit.is_held()) {
            return self.read(reader, cancellation).await;
        }

        // A partial head or framing boundary must be contiguous for the parser.
        // Reading portably into that prefix avoids receiving only to copy again.
        if !self.bytes().is_empty() {
            return self.read(reader, cancellation).await;
        }

        let target = self.preferred_read.min(self.max_retained);

        match super::managed::read(reader, target, cancellation).await {
            Ok(Some(buffer)) => {
                let count = buffer.len();
                crate::trace::progress("read_managed", count);
                self.managed_permit
                    .get_or_insert_with(|| Rc::new(ManagedLeasePermit::new()));
                self.storage = ReceiveStorage::Managed {
                    buffer,
                    range: 0..count,
                };
                (Ok(ReadStatus::Data(count)), self)
            }
            Ok(None) => (Ok(ReadStatus::Eof), self),
            Err(error) => (Err(error), self),
        }
    }

    #[cfg(test)]
    fn portable_mut(&mut self) -> &mut BytesMut {
        match &mut self.storage {
            ReceiveStorage::Portable(bytes) => bytes,
            #[cfg(target_os = "linux")]
            ReceiveStorage::Managed { .. } => panic!("test expected portable storage"),
        }
    }

    fn compact_if_empty(&mut self) {
        let release_managed = match &mut self.storage {
            ReceiveStorage::Portable(bytes) => {
                let replacement_threshold = self.preferred_read.saturating_mul(4).min(self.max_retained);
                if bytes.as_ref().is_empty() && bytes.capacity() > replacement_threshold {
                    *bytes = BytesMut::with_capacity(self.preferred_read);
                }
                false
            }
            #[cfg(target_os = "linux")]
            ReceiveStorage::Managed { range, .. } => range.start == range.end,
        };

        if release_managed {
            self.storage = ReceiveStorage::Portable(BytesMut::new());
        }
    }

    fn make_portable(&mut self) {
        #[cfg(target_os = "linux")]
        if matches!(&self.storage, ReceiveStorage::Managed { .. }) {
            let ReceiveStorage::Managed { buffer, range } =
                mem::replace(&mut self.storage, ReceiveStorage::Portable(BytesMut::new()))
            else {
                unreachable!("managed receive storage changed during conversion");
            };

            let bytes = BytesMut::from(&buffer[range]);

            self.storage = ReceiveStorage::Portable(bytes);
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
            assert_eq!(body.as_ref().as_ptr(), pointer);
            assert_eq!(body.as_ref(), b"body");
            assert_eq!(buffer.bytes(), b"NEXT");
            let (result, mut buffer) = buffer.read(&mut reader, None).await;
            result.unwrap();
            assert_eq!(body.as_ref(), b"body");
            let pointer = buffer.bytes().as_ptr();
            let read_ahead = buffer.take_all();
            assert_eq!(read_ahead, "NEXTlater");
            assert_eq!(read_ahead.as_ptr(), pointer);
        });
    }

    #[test]
    fn invalid_decoder_ranges_do_not_mutate_retained_input() {
        let mut buffer = RecvBuffer::new(8, 16).unwrap();
        buffer.portable_mut().extend_from_slice(b"bytes");
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
            buffer.portable_mut().extend_from_slice(b"prefix");
            let (result, buffer) = buffer.read(&mut reader, Some(source.token())).await;
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Canceled);
            assert_eq!(reader.submissions, 0);
            assert_eq!(buffer.bytes(), b"prefix");
            let (result, buffer) = buffer.read(&mut reader, None).await;
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Io);
            assert_eq!(buffer.bytes(), b"prefix");
        });
    }
    #[cfg(target_os = "linux")]
    use karmaio::{RuntimeBuilder, buf::IoBuf, net::tcp::TcpListener};
    #[cfg(target_os = "linux")]
    use std::{io::Write, net::SocketAddr, thread};

    #[cfg(target_os = "linux")]
    async fn managed_read(buffer: &mut RecvBuffer, reader: &mut OwnedReadHalf<TcpStream>) -> Result<ReadStatus, Error> {
        let owned = mem::replace(buffer, RecvBuffer::new(1, 1).unwrap());
        let (result, returned) = crate::io::transport::Tcp.read(reader, owned, None).await;
        *buffer = returned;
        result
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn managed_payload_moves_without_copy_and_retains_only_explicit_slices() {
        RuntimeBuilder::new()
            .buffer_pool_size(1)
            .buffer_pool_buffer_len(16)
            .build()
            .unwrap()
            .block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).unwrap();
                let address = listener.local_addr().unwrap();
                let client = thread::spawn(move || {
                    let mut stream = std::net::TcpStream::connect(address).unwrap();
                    stream.write_all(b"HEADbodyNEXT").unwrap();
                });

                let (stream, _) = listener.accept().await.unwrap();
                client.join().unwrap();
                let (mut reader, writer) = stream.into_split();
                let mut buffer = RecvBuffer::new(16, 16).unwrap();
                assert_eq!(
                    managed_read(&mut buffer, &mut reader).await.unwrap(),
                    ReadStatus::Data(12)
                );
                assert_eq!(buffer.bytes(), b"HEADbodyNEXT");

                buffer.consume(4).unwrap();
                let payload_pointer = buffer.bytes().as_ptr();
                let payload = buffer.take_payload(0..4, 4).unwrap();
                assert_eq!(payload.as_ref(), b"body");
                assert_eq!(payload.as_init(), b"body");
                assert_eq!(payload.as_ref().as_ptr(), payload_pointer);
                assert_eq!(payload.managed_strong_count(), Some(1));
                assert_eq!(buffer.bytes(), b"NEXT");

                let slice = IncomingData::slice(&payload, 1..3);
                assert_eq!(slice.as_ref(), b"od");
                assert_eq!(payload.managed_strong_count(), Some(2));
                drop(slice);
                assert_eq!(payload.managed_strong_count(), Some(1));
                drop(payload);

                buffer.consume(4).unwrap();
                assert_eq!(managed_read(&mut buffer, &mut reader).await.unwrap(), ReadStatus::Eof);
                drop(reader);
                drop(writer);

                let client = thread::spawn(move || {
                    let mut stream = std::net::TcpStream::connect(address).unwrap();
                    stream.write_all(b"x").unwrap();
                });
                let (stream, _) = listener.accept().await.unwrap();
                client.join().unwrap();
                let (mut reader, writer) = stream.into_split();
                let mut buffer = RecvBuffer::new(1, 1).unwrap();
                assert_eq!(
                    managed_read(&mut buffer, &mut reader).await.unwrap(),
                    ReadStatus::Data(1)
                );
                assert_eq!(buffer.bytes(), b"x");
                let payload_pointer = buffer.bytes().as_ptr();
                let payload = buffer.take_payload(0..1, 1).unwrap();
                assert_eq!(payload.as_ref(), b"x");
                assert_eq!(payload.as_ref().as_ptr(), payload_pointer);
                assert!(buffer.bytes().is_empty());
                drop(payload);
                drop(reader);
                drop(writer);

                let client = thread::spawn(move || {
                    let mut stream = std::net::TcpStream::connect(address).unwrap();
                    stream.write_all(b"UPGRADE").unwrap();
                });
                let (stream, _) = listener.accept().await.unwrap();
                client.join().unwrap();
                let (mut reader, writer) = stream.into_split();
                let mut buffer = RecvBuffer::new(8, 8).unwrap();
                assert_eq!(
                    managed_read(&mut buffer, &mut reader).await.unwrap(),
                    ReadStatus::Data(7)
                );
                assert_eq!(buffer.take_all(), b"UPGRADE"[..]);
                assert!(buffer.bytes().is_empty());
                drop(reader);
                drop(writer);
            });
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn retained_managed_frame_forces_bounded_portable_fallback() {
        RuntimeBuilder::new()
            .buffer_pool_size(1)
            .buffer_pool_buffer_len(1)
            .build()
            .unwrap()
            .block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).unwrap();
                let address = listener.local_addr().unwrap();
                let client = thread::spawn(move || {
                    let mut stream = std::net::TcpStream::connect(address).unwrap();
                    stream.write_all(b"abcd").unwrap();
                });

                let (stream, _) = listener.accept().await.unwrap();
                client.join().unwrap();
                let (mut reader, writer) = stream.into_split();
                let mut buffer = RecvBuffer::new(1, 1).unwrap();

                assert_eq!(
                    managed_read(&mut buffer, &mut reader).await.unwrap(),
                    ReadStatus::Data(1)
                );
                let first = buffer.take_payload(0..1, 1).unwrap();
                assert!(first.is_managed());
                assert!(buffer.managed_permit.as_ref().unwrap().is_held());

                assert_eq!(
                    managed_read(&mut buffer, &mut reader).await.unwrap(),
                    ReadStatus::Data(1)
                );
                let second = buffer.take_payload(0..1, 1).unwrap();
                assert!(!second.is_managed());
                drop(second);

                assert_eq!(
                    managed_read(&mut buffer, &mut reader).await.unwrap(),
                    ReadStatus::Data(1)
                );
                let third = buffer.take_payload(0..1, 1).unwrap();
                assert!(!third.is_managed());
                drop(third);

                let retained = first.clone();
                drop(first);
                assert!(buffer.managed_permit.as_ref().unwrap().is_held());
                drop(retained);
                assert!(!buffer.managed_permit.as_ref().unwrap().is_held());
                assert_eq!(
                    managed_read(&mut buffer, &mut reader).await.unwrap(),
                    ReadStatus::Data(1)
                );
                let fourth = buffer.take_payload(0..1, 1).unwrap();
                assert!(fourth.is_managed());

                drop(fourth);
                drop(reader);
                drop(writer);
            });
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn managed_views_remain_valid_after_connection_and_runtime_teardown() {
        let data = {
            let mut runtime = RuntimeBuilder::new()
                .buffer_pool_size(1)
                .buffer_pool_buffer_len(16)
                .build()
                .unwrap();
            runtime.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).unwrap();
                let mut peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
                peer.write_all(b"payload").unwrap();
                let (stream, _) = listener.accept().await.unwrap();
                let (mut reader, _writer) = stream.into_split();
                let mut buffer = RecvBuffer::new(16, 16).unwrap();
                assert_eq!(
                    managed_read(&mut buffer, &mut reader).await.unwrap(),
                    ReadStatus::Data(7)
                );
                let data = buffer.take_payload(0..7, 7).unwrap();
                assert!(data.is_managed());
                data
            })
        };
        let mut clone = data.clone();
        let slice = IncomingData::slice(&data, 1..4);
        bytes::Buf::advance(&mut clone, 3);
        drop(data);
        assert_eq!(clone.as_ref(), b"load");
        assert_eq!(slice.as_ref(), b"ayl");
        drop(clone);
        assert_eq!(slice.as_ref(), b"ayl");
    }
}
