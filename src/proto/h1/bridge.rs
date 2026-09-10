use super::{
    BodyMode,
    decode::{BodyDecoder, DecodeOutcome},
    encode::{BodyEncoder, EncodeError},
};
use crate::{
    Body, Error, ErrorKind, Frame, Incoming, IncomingData, SizeHint, TrailerHint,
    body::{incoming::IncomingProducer, pipe::OfferError},
    future::{Race, WorkBudget, cancellable_wait, race},
    io::{
        recv::{ReadStatus, RecvBuffer},
        send::{write_all, write_framed},
        transport::Receive,
    },
};
use karmaio::{
    buf::IoBuf,
    io::AsyncWrite,
    runtime::{CancellationSource, CancellationToken, FutureExt, operation_canceled},
};
use std::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};

/// Derive body metadata only from validated incoming wire framing. A head-only
/// or zero-length fixed message needs no rendezvous. Chunked framing always
/// retains demand for its terminator and possible trailers.
pub(super) fn incoming(mode: BodyMode) -> (Option<IncomingProducer>, Incoming) {
    match mode {
        BodyMode::None | BodyMode::Fixed(0) => (None, Incoming::empty()),
        BodyMode::Fixed(length) => {
            let (producer, body) = Incoming::channel(SizeHint::with_exact(length), TrailerHint::None);
            (Some(producer), body)
        }
        BodyMode::Chunked | BodyMode::UntilEof => {
            let trailers = if mode == BodyMode::Chunked {
                TrailerHint::MayHave
            } else {
                TrailerHint::None
            };
            let (producer, body) = Incoming::channel(SizeHint::new(), trailers);
            (Some(producer), body)
        }
    }
}

/// Drive one receive activity inside its connection's scope. No spawned task
/// or request-context lifetime is attached to the body. Awaiting this future
/// recovers retained input even after abandonment cancels pending transport I/O.
pub(super) async fn receive_body<R, S: Receive<R>>(
    reader: &mut R,
    strategy: &mut S,
    mut buffer: RecvBuffer,
    mut decoder: BodyDecoder,
    mut producer: IncomingProducer,
    cancellation: Option<CancellationToken>,
) -> (ReceiveEnd, RecvBuffer) {
    let abandonment = CancellationSource::new();
    let mut eof = false;
    let mut work = WorkBudget::new();
    loop {
        work.step().await;
        if decoder.is_complete() {
            producer.close();
            return (ReceiveEnd::Complete, buffer);
        }
        match cancellable_wait(producer.demand(), cancellation).await {
            Some(Ok(())) => {}
            Some(Err(_)) => return (ReceiveEnd::Abandoned, buffer),
            None => return (ReceiveEnd::fail(producer, Error::from(operation_canceled())), buffer),
        }
        let outcome = match decoder.decode(buffer.bytes(), eof) {
            Ok(outcome) => outcome,
            Err(error) => return (ReceiveEnd::fail(producer, error.into()), buffer),
        };
        let frame = match outcome {
            DecodeOutcome::NeedMore { consumed } => {
                if let Err(error) = buffer.consume(consumed) {
                    return (ReceiveEnd::fail(producer, error), buffer);
                }
                let (result, returned) =
                    read_or_abandoned(reader, strategy, buffer, &mut producer, &abandonment, cancellation).await;
                buffer = returned;
                if producer.is_abandoned() {
                    return (ReceiveEnd::Abandoned, buffer);
                }
                match result {
                    Ok(ReadStatus::Eof) => eof = true,
                    Ok(ReadStatus::Data(_)) => {}
                    Err(error) => return (ReceiveEnd::fail(producer, error), buffer),
                }
                continue;
            }
            DecodeOutcome::Data { payload, consumed } => {
                let data = match buffer.take_payload(payload, consumed) {
                    Ok(data) => IncomingData::from_bytes(data),
                    Err(error) => return (ReceiveEnd::fail(producer, error), buffer),
                };
                Frame::data(data)
            }
            DecodeOutcome::Trailers { trailers, consumed } => {
                if let Err(error) = buffer.consume(consumed) {
                    return (ReceiveEnd::fail(producer, error), buffer);
                }
                Frame::trailers(trailers)
            }
            DecodeOutcome::End { consumed } => {
                if let Err(error) = buffer.consume(consumed) {
                    return (ReceiveEnd::fail(producer, error), buffer);
                }
                producer.close();
                return (ReceiveEnd::Complete, buffer);
            }
        };
        match cancellable_wait(producer.offer(frame), cancellation).await {
            Some(Ok(())) => {}
            Some(Err(OfferError::Abandoned(_))) => return (ReceiveEnd::Abandoned, buffer),
            Some(Err(OfferError::Closed(_))) => {
                return (
                    ReceiveEnd::Failed(Error::new(ErrorKind::Closed, "body handoff closed")),
                    buffer,
                );
            }
            None => return (ReceiveEnd::fail(producer, Error::from(operation_canceled())), buffer),
        }
    }
}

async fn read_or_abandoned<R, S: Receive<R>>(
    reader: &mut R,
    strategy: &mut S,
    buffer: RecvBuffer,
    producer: &mut IncomingProducer,
    abandonment: &CancellationSource,
    cancellation: Option<CancellationToken>,
) -> (Result<ReadStatus, Error>, RecvBuffer) {
    // Both scopes must be installed before the first poll/submission. Retain
    // the read even when the abandonment wait wins; cancellation is fail-slow.
    let read = async {
        let read = strategy.read(reader, buffer, Some(abandonment.token()));
        match cancellation {
            Some(token) => read.with_cancellation(token).await,
            None => read.await,
        }
    };
    let mut read = pin!(read);
    let mut abandoned = pin!(producer.abandoned());
    match race(read.as_mut(), abandoned.as_mut()).await {
        Race::First(result) => result,
        Race::Second(()) => {
            abandonment.cancel();
            read.await
        }
    }
}

#[derive(Debug)]
pub(super) enum ReceiveEnd {
    Complete,
    Abandoned,
    /// Shares the original source with the error delivered to Incoming.
    Failed(Error),
}

impl ReceiveEnd {
    pub(super) fn fail(producer: IncomingProducer, error: Error) -> Self {
        let (incoming, driver) = error.split();
        producer.fail(incoming);
        Self::Failed(driver)
    }
}

/// Drive a producer and its writes in the connection's scope. Recycle each
/// recovered payload while the producer is still available, including failure
/// completions. A dropped future does not promise a later recycler callback.
pub(super) async fn send_body<W: AsyncWrite, B: Body>(
    writer: &mut W,
    body: &mut B,
    encoder: &mut BodyEncoder,
    cancellation: Option<CancellationToken>,
) -> Result<SendStats, SendBodyError<B::Error>> {
    let mut trailers_sent = false;
    let mut stats = SendStats::default();
    let mut work = WorkBudget::new();
    // The caller may have queued a head before entering the body driver.
    let mut pending_output = true;
    loop {
        let next = {
            // Retain this exact producer future across flushing. Include the
            // fairness wait so ready producers cannot strand buffered output.
            let mut next = pin!(cancellable_wait(
                async {
                    work.step().await;
                    body.next_frame().await
                },
                cancellation
            ));
            match poll_fn(|cx| Poll::Ready(next.as_mut().poll(cx))).await {
                Poll::Ready(next) => next,
                Poll::Pending => {
                    if pending_output {
                        let flush = writer.flush();
                        match cancellation {
                            Some(token) => flush.with_cancellation(token).await,
                            None => flush.await,
                        }
                        .map_err(SendBodyError::Io)?;
                        pending_output = false;
                    }
                    // An immediately ready flush must not consume the work
                    // budget's yield by repolling `next` in the same turn.
                    let mut yielded = false;
                    poll_fn(|cx| {
                        if yielded {
                            next.as_mut().poll(cx)
                        } else {
                            yielded = true;
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    })
                    .await
                }
            }
        }
        .ok_or(SendBodyError::Canceled)?
        .map_err(SendBodyError::Body)?;
        match next {
            Some(frame) => match frame.into_data() {
                Ok(data) => {
                    let framing = match encoder.data(data.as_init().len()) {
                        Ok(framing) => framing,
                        Err(error) => {
                            body.recycle(data);
                            return Err(SendBodyError::Encode(error));
                        }
                    };
                    let (result, data) = write_framed(writer, framing.prefix, data, framing.suffix, cancellation)
                        .await
                        .into_parts();
                    body.recycle(data);
                    let written = result.map_err(SendBodyError::Io)?;
                    pending_output |= written != 0;
                    stats.wire_bytes = stats.wire_bytes.saturating_add(written as u64);
                    stats.data_frames = stats.data_frames.saturating_add(1);
                }
                Err(frame) => {
                    let trailers = frame.into_trailers().ok().expect("non-data frame contains trailers");
                    let bytes = encoder.trailers(&trailers).map_err(SendBodyError::Encode)?;
                    let (result, _) = write_all(writer, bytes, cancellation).await.into_parts();
                    let written = result.map_err(SendBodyError::Io)?;
                    pending_output |= written != 0;
                    stats.wire_bytes = stats.wire_bytes.saturating_add(written as u64);
                    stats.trailer_frames = stats.trailer_frames.saturating_add(1);
                    trailers_sent = true;
                }
            },
            None if trailers_sent => return Ok(stats),
            None => {
                let bytes = encoder.finish().map_err(SendBodyError::Encode)?;
                let (result, _) = write_all(writer, bytes, cancellation).await.into_parts();
                stats.wire_bytes = stats
                    .wire_bytes
                    .saturating_add(result.map_err(SendBodyError::Io)? as u64);
                return Ok(stats);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct SendStats {
    pub(super) wire_bytes: u64,
    pub(super) data_frames: u64,
    pub(super) trailer_frames: u64,
}

#[derive(Debug)]
pub(super) enum SendBodyError<E> {
    Body(E),
    Encode(EncodeError),
    /// Includes confirmed cancellation and retains the original I/O error.
    Io(std::io::Error),
    /// Cancellation before the producer yielded a frame.
    Canceled,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BodyExt, Full, Service,
        body::collect,
        io::transport::Portable,
        proto::h1::encode::{BodyMetadata, EncodeLimits},
        test_transport::{Gate, ReadStep, Reader, WriteStep, Writer},
    };
    use bytes::Bytes;
    use karmaio::{
        buf::{BufResult, IoBufMut},
        io::AsyncRead,
    };
    use std::{
        cell::Cell,
        collections::VecDeque,
        future::Future,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    const LIMITS: EncodeLimits = EncodeLimits::new(4096, 4096);

    fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    async fn pair<A: Future, B: Future>(first: A, second: B) -> (A::Output, B::Output) {
        let mut first = pin!(first);
        let mut second = pin!(second);
        match race(first.as_mut(), second.as_mut()).await {
            Race::First(value) => (value, second.await),
            Race::Second(value) => (first.await, value),
        }
    }

    struct CountedReader {
        reader: Reader,
        calls: Rc<Cell<usize>>,
    }
    impl AsyncRead for CountedReader {
        async fn read<B: IoBufMut>(&mut self, buffer: B) -> BufResult<usize, B> {
            self.calls.set(self.calls.get() + 1);
            self.reader.read(buffer).await
        }
    }

    #[test]
    fn fixed_chunked_and_eof_bodies_preserve_payload_trailers_and_read_ahead() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for (mode, wire, expected, remainder) in [
                (
                    BodyMode::Fixed(5),
                    b"helloNEXT".as_slice(),
                    b"hello".as_slice(),
                    b"NEXT".as_slice(),
                ),
                (
                    BodyMode::Chunked,
                    b"4\r\nWiki\r\n5\r\npedia\r\n0\r\nx-end: yes\r\n\r\nNEXT".as_slice(),
                    b"Wikipedia".as_slice(),
                    b"NEXT".as_slice(),
                ),
                (
                    BodyMode::UntilEof,
                    b"hello".as_slice(),
                    b"hello".as_slice(),
                    b"".as_slice(),
                ),
            ] {
                // Reuse the framing fixtures at fragmented and single-read sizes.
                for width in [1, 3, wire.len()] {
                    let mut reader = Reader::new(
                        wire.chunks(width)
                            .map(|chunk| ReadStep::Data(Bytes::copy_from_slice(chunk))),
                    );
                    let mut strategy = Portable;
                    let (producer, mut body) = incoming(mode);
                    let driver = receive_body(
                        &mut reader,
                        &mut strategy,
                        RecvBuffer::new(128, 256).unwrap(),
                        BodyDecoder::new(mode),
                        producer.unwrap(),
                        None,
                    );
                    let ((end, mut buffer), collected) = pair(driver, collect(&mut body, 16)).await;
                    let collected = collected.unwrap();
                    assert!(matches!(end, ReceiveEnd::Complete));
                    assert_eq!(collected.as_ref(), expected);
                    if mode == BodyMode::Chunked {
                        assert_eq!(collected.trailers().unwrap()["x-end"], "yes");
                    }
                    // Some read-ahead may still be in the transport when fragmented.
                    let mut next = buffer.take_all().to_vec();
                    while next.len() < remainder.len() {
                        let (result, mut returned) = buffer.read(&mut reader, None).await;
                        assert!(matches!(result.unwrap(), ReadStatus::Data(_)));
                        next.extend_from_slice(&returned.take_all());
                        buffer = returned;
                    }
                    assert_eq!(next, remainder);
                    assert!(body.is_end_stream());
                }
            }
        });
    }

    #[test]
    fn demand_bounds_reads_and_fixed_completion_needs_no_extra_poll() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let calls = Rc::new(Cell::new(0));
            let mut reader = CountedReader {
                calls: calls.clone(),
                reader: Reader::new([
                    ReadStep::Data(Bytes::from_static(b"abc")),
                    ReadStep::Data(Bytes::from_static(b"def")),
                ]),
            };
            let mut strategy = Portable;
            let (producer, mut body) = incoming(BodyMode::Fixed(6));
            let mut driver = pin!(receive_body(
                &mut reader,
                &mut strategy,
                RecvBuffer::new(3, 6).unwrap(),
                BodyDecoder::new(BodyMode::Fixed(6)),
                producer.unwrap(),
                None
            ));
            assert!(poll(driver.as_mut()).is_pending());
            assert_eq!(calls.get(), 0);
            // Canceling the wait retains one demand; it does not abandon the body.
            assert!(poll(pin!(body.next_frame())).is_pending());
            assert!(poll(driver.as_mut()).is_pending());
            assert_eq!(calls.get(), 1);
            assert!(poll(driver.as_mut()).is_pending());
            assert_eq!(calls.get(), 1);
            let Poll::Ready(Ok(Some(first))) = poll(pin!(body.next_frame())) else {
                panic!("missing first frame")
            };
            let first = first.into_data().unwrap();
            assert!(poll(driver.as_mut()).is_pending());
            assert_eq!(calls.get(), 1);
            assert!(poll(pin!(body.next_frame())).is_pending());
            assert!(poll(driver.as_mut()).is_pending());
            assert_eq!(calls.get(), 2);
            let Poll::Ready(Ok(Some(second))) = poll(pin!(body.next_frame())) else {
                panic!("missing second frame")
            };
            assert_eq!(second.into_data().unwrap().as_ref(), b"def");
            // Drop immediately after the final data, without asking for None.
            drop(body);
            let Poll::Ready((end, _)) = poll(driver.as_mut()) else {
                panic!("fixed completion required extra demand")
            };
            assert!(matches!(end, ReceiveEnd::Complete));
            assert_eq!(first.as_ref(), b"abc");
        });
    }

    #[test]
    fn abandoning_pending_receive_waits_for_completion_and_returns_storage() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for success in [false, true] {
                let gate = Gate::default();
                let calls = Rc::new(Cell::new(0));
                let mut reader = CountedReader {
                    calls: calls.clone(),
                    reader: Reader::new([
                        ReadStep::Wait(gate.clone()),
                        if success {
                            ReadStep::Data(Bytes::from_static(b"x"))
                        } else {
                            ReadStep::Error(operation_canceled())
                        },
                    ]),
                };
                let mut strategy = Portable;
                let (producer, mut body) = incoming(BodyMode::Fixed(1));
                let mut driver = pin!(receive_body(
                    &mut reader,
                    &mut strategy,
                    RecvBuffer::new(8, 16).unwrap(),
                    BodyDecoder::new(BodyMode::Fixed(1)),
                    producer.unwrap(),
                    None
                ));
                assert!(poll(pin!(body.next_frame())).is_pending());
                assert!(poll(driver.as_mut()).is_pending());
                assert_eq!(calls.get(), 1);
                drop(body);
                assert!(poll(driver.as_mut()).is_pending());
                gate.open();
                let Poll::Ready((end, buffer)) = poll(driver.as_mut()) else {
                    panic!("read not settled")
                };
                assert!(matches!(end, ReceiveEnd::Abandoned));
                assert_eq!(buffer.bytes(), if success { b"x".as_slice() } else { b"".as_slice() });
                assert_eq!(calls.get(), 1);
            }
        });
    }

    #[test]
    fn canceling_an_offer_or_dropping_the_activity_never_reports_clean_eof() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for explicit in [false, true] {
                let source = CancellationSource::new();
                let mut reader = Reader::new([ReadStep::Data(Bytes::from_static(b"payload"))]);
                let mut strategy = Portable;
                let (producer, mut body) = incoming(BodyMode::Fixed(7));
                {
                    let mut driver = pin!(receive_body(
                        &mut reader,
                        &mut strategy,
                        RecvBuffer::new(8, 16).unwrap(),
                        BodyDecoder::new(BodyMode::Fixed(7)),
                        producer.unwrap(),
                        Some(source.token())
                    ));
                    assert!(poll(pin!(body.next_frame())).is_pending());
                    assert!(poll(driver.as_mut()).is_pending());
                    if explicit {
                        source.cancel();
                        let Poll::Ready((end, _)) = poll(driver.as_mut()) else {
                            panic!("offer cancellation did not settle")
                        };
                        assert!(matches!(end, ReceiveEnd::Failed(ref error) if error.kind() == ErrorKind::Canceled));
                    }
                }
                let Poll::Ready(Err(error)) = poll(pin!(body.next_frame())) else {
                    panic!("activity loss was hidden")
                };
                assert_eq!(
                    error.kind(),
                    if explicit {
                        ErrorKind::Canceled
                    } else {
                        ErrorKind::Closed
                    }
                );
                assert!(matches!(poll(pin!(body.next_frame())), Poll::Ready(Ok(None))));
            }
        });
    }

    #[test]
    fn framing_failure_reaches_incoming_with_its_original_source() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for (mode, wire) in [
                (BodyMode::Fixed(3), b"x".as_slice()),
                (BodyMode::Chunked, b"oops\r\n".as_slice()),
            ] {
                let mut reader = Reader::new([ReadStep::Data(Bytes::copy_from_slice(wire))]);
                let mut strategy = Portable;
                let (producer, mut body) = incoming(mode);
                let driver = receive_body(
                    &mut reader,
                    &mut strategy,
                    RecvBuffer::new(16, 32).unwrap(),
                    BodyDecoder::new(mode),
                    producer.unwrap(),
                    None,
                );
                let ((end, _), result) = pair(driver, collect(&mut body, 16)).await;
                assert!(matches!(end, ReceiveEnd::Failed(ref error) if error.kind() == ErrorKind::InvalidMessage));
                let crate::CollectError::Body(error) = result.unwrap_err() else {
                    panic!("wrong failure")
                };
                assert!(
                    std::error::Error::source(&error)
                        .unwrap()
                        .is::<super::super::decode::DecodeError>()
                );
                assert!(body.is_end_stream());
            }
        });
    }

    #[test]
    fn returned_request_body_is_driven_after_service_future_finishes() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mode = BodyMode::Chunked;
            let (producer, body) = incoming(mode);
            let service = crate::service_fn(async |body: Incoming| -> Result<_, std::convert::Infallible> {
                Ok(http::Response::new(body))
            });
            let mut body = service.call(body).await.unwrap().into_body();
            let metadata = BodyMetadata {
                size: body.size_hint(),
                trailers: body.trailer_hint(),
            };
            let mut encoder = BodyEncoder::new(mode, metadata, LIMITS).unwrap();
            let mut writer = Writer::limited(1);
            let mut reader = Reader::new([ReadStep::Data(Bytes::from_static(
                b"4\r\nbody\r\n0\r\nx-end: yes\r\n\r\nNEXT",
            ))]);
            let mut strategy = Portable;
            let receive = receive_body(
                &mut reader,
                &mut strategy,
                RecvBuffer::new(64, 128).unwrap(),
                BodyDecoder::new(mode),
                producer.unwrap(),
                None,
            );
            let ((end, buffer), sent) = pair(receive, send_body(&mut writer, &mut body, &mut encoder, None)).await;
            assert!(matches!(end, ReceiveEnd::Complete));
            assert_eq!(sent.unwrap().trailer_frames, 1);
            assert_eq!(writer.output, b"4\r\nbody\r\n0\r\nx-end: yes\r\n\r\n");
            assert_eq!(buffer.bytes(), b"NEXT");
        });
    }

    #[test]
    fn exact_length_with_attached_trailers_is_written_without_losing_terminal_checks() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut trailers = http::HeaderMap::new();
            trailers.insert("x-end", http::HeaderValue::from_static("yes"));
            let mut body = Full::new(Bytes::from_static(b"body")).with_trailers(trailers);
            let metadata = BodyMetadata {
                size: body.size_hint(),
                trailers: body.trailer_hint(),
            };
            let mut encoder = BodyEncoder::new(BodyMode::Chunked, metadata, LIMITS).unwrap();
            let mut writer = Writer::limited(2);
            let stats = send_body(&mut writer, &mut body, &mut encoder, None).await.unwrap();
            assert_eq!(stats.data_frames, 1);
            assert_eq!(stats.trailer_frames, 1);
            assert_eq!(stats.wire_bytes as usize, writer.output.len());
            assert_eq!(writer.output, b"4\r\nbody\r\n0\r\nx-end: yes\r\n\r\n");
        });
    }

    #[derive(Default)]
    struct BufferedOutput {
        staged: Vec<u8>,
        visible: Vec<u8>,
        flushes: usize,
    }

    struct BufferedWriter(Rc<std::cell::RefCell<BufferedOutput>>);
    impl AsyncWrite for BufferedWriter {
        async fn write<B: IoBuf>(&mut self, data: B) -> BufResult<usize, B> {
            let n = data.as_init().len();
            self.0.borrow_mut().staged.extend_from_slice(data.as_init());
            BufResult(Ok(n), data)
        }
        async fn write_vectored<B: karmaio::buf::IoVectoredBuf>(&mut self, data: B) -> BufResult<usize, B> {
            let mut n = 0;
            for bytes in data.iter_slice() {
                self.0.borrow_mut().staged.extend_from_slice(bytes);
                n += bytes.len();
            }
            BufResult(Ok(n), data)
        }
        async fn flush(&mut self) -> std::io::Result<()> {
            let mut output = self.0.borrow_mut();
            output.flushes += 1;
            let bytes = std::mem::take(&mut output.staged);
            output.visible.extend(bytes);
            Ok(())
        }
        async fn shutdown(&mut self) -> std::io::Result<()> {
            self.flush().await
        }
    }

    #[test]
    fn pending_production_flushes_head_and_data_without_restarting_the_frame_future() {
        struct Waiting {
            calls: Rc<Cell<usize>>,
            first: Gate,
            end: Gate,
        }
        impl Body for Waiting {
            type Data = Bytes;
            type Error = std::convert::Infallible;
            async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, Self::Error> {
                let call = self.calls.get();
                self.calls.set(call + 1);
                if call == 0 {
                    self.first.wait().await;
                    Ok(Some(Frame::data(Bytes::from_static(b"abc"))))
                } else {
                    self.end.wait().await;
                    Ok(None)
                }
            }
        }
        karmaio::Runtime::new().unwrap().block_on(async {
            let output = Rc::new(std::cell::RefCell::new(BufferedOutput {
                staged: b"head".to_vec(),
                ..Default::default()
            }));
            let calls = Rc::new(Cell::new(0));
            let first = Gate::default();
            let end = Gate::default();
            let mut body = Waiting {
                calls: calls.clone(),
                first: first.clone(),
                end: end.clone(),
            };
            let mut writer = BufferedWriter(output.clone());
            let metadata = BodyMetadata {
                size: SizeHint::with_exact(3),
                trailers: TrailerHint::None,
            };
            let mut encoder = BodyEncoder::new(BodyMode::Fixed(3), metadata, LIMITS).unwrap();
            let mut send = pin!(send_body(&mut writer, &mut body, &mut encoder, None));
            assert!(poll(send.as_mut()).is_pending());
            assert_eq!(output.borrow().visible, b"head");
            assert_eq!(calls.get(), 1);
            assert!(poll(send.as_mut()).is_pending());
            assert_eq!(calls.get(), 1);
            assert_eq!(output.borrow().flushes, 1);
            first.open();
            assert!(poll(send.as_mut()).is_pending());
            assert_eq!(output.borrow().visible, b"headabc");
            assert_eq!(output.borrow().flushes, 2);
            assert_eq!(calls.get(), 2);
            end.open();
            send.await.unwrap();
            assert_eq!(calls.get(), 2);
        });
    }

    #[test]
    fn ready_frames_batch_but_fairness_suspension_flushes_output() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for count in [1, 17] {
                let output = Rc::new(std::cell::RefCell::new(BufferedOutput::default()));
                let mut writer = BufferedWriter(output.clone());
                let mut body = TestBody {
                    frames: (0..count).map(|_| Ok(Frame::data(Bytes::from_static(b"x")))).collect(),
                    recycled: Rc::new(Cell::new(0)),
                };
                let metadata = BodyMetadata {
                    size: SizeHint::with_exact(count),
                    trailers: TrailerHint::None,
                };
                let mut encoder = BodyEncoder::new(BodyMode::Fixed(count), metadata, LIMITS).unwrap();
                let mut send = pin!(send_body(&mut writer, &mut body, &mut encoder, None));
                let first = poll(send.as_mut());
                if count == 1 {
                    assert!(matches!(first, Poll::Ready(Ok(_))));
                    assert_eq!(output.borrow().flushes, 0);
                } else {
                    assert!(first.is_pending());
                    assert_eq!(output.borrow().visible, vec![b'x'; 16]);
                    assert_eq!(output.borrow().flushes, 1);
                    send.await.unwrap();
                }
            }
        });
    }

    #[test]
    fn cancellation_settles_pending_flush_and_preserves_its_failure() {
        struct FlushGate {
            token: CancellationToken,
            observed: Gate,
            complete: Gate,
            settled: Rc<Cell<bool>>,
        }
        impl AsyncWrite for FlushGate {
            async fn write<B: IoBuf>(&mut self, data: B) -> BufResult<usize, B> {
                BufResult(Ok(data.as_init().len()), data)
            }
            async fn flush(&mut self) -> std::io::Result<()> {
                self.token.cancelled().await;
                self.observed.open();
                self.complete.wait().await;
                self.settled.set(true);
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "original flush failure",
                ))
            }
            async fn shutdown(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        struct Pending;
        impl Body for Pending {
            type Data = Bytes;
            type Error = std::convert::Infallible;
            async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, Self::Error> {
                std::future::pending().await
            }
        }
        karmaio::Runtime::new().unwrap().block_on(async {
            let source = CancellationSource::new();
            let observed = Gate::default();
            let complete = Gate::default();
            let settled = Rc::new(Cell::new(false));
            let mut writer = FlushGate {
                token: source.token(),
                observed: observed.clone(),
                complete: complete.clone(),
                settled: settled.clone(),
            };
            let mut encoder = BodyEncoder::new(
                BodyMode::Chunked,
                BodyMetadata {
                    size: SizeHint::new(),
                    trailers: TrailerHint::MayHave,
                },
                LIMITS,
            )
            .unwrap();
            let mut body = Pending;
            let mut send = pin!(send_body(&mut writer, &mut body, &mut encoder, Some(source.token())));
            assert!(poll(send.as_mut()).is_pending());
            source.cancel();
            assert!(poll(send.as_mut()).is_pending());
            assert!(observed.is_open());
            assert!(!settled.get());
            complete.open();
            let Err(SendBodyError::Io(error)) = send.await else {
                panic!("lost flush failure")
            };
            assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
            assert!(settled.get());
        });
    }

    struct TestBody {
        frames: VecDeque<Result<Frame<Bytes>, &'static str>>,
        recycled: Rc<Cell<usize>>,
    }
    impl Body for TestBody {
        type Data = Bytes;
        type Error = &'static str;
        async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, &'static str> {
            self.frames.pop_front().transpose()
        }
        fn recycle(&mut self, _: Bytes) {
            self.recycled.set(self.recycled.get() + 1);
        }
    }

    #[test]
    fn late_producer_errors_and_post_trailer_frames_prevent_successful_send() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for late_error in [false, true] {
                let recycled = Rc::new(Cell::new(0));
                let mut body = TestBody {
                    recycled: recycled.clone(),
                    frames: VecDeque::from([
                        Ok(Frame::trailers(http::HeaderMap::new())),
                        if late_error {
                            Err("late failure")
                        } else {
                            Ok(Frame::data(Bytes::new()))
                        },
                    ]),
                };
                let metadata = BodyMetadata {
                    size: SizeHint::with_exact(0),
                    trailers: TrailerHint::MayHave,
                };
                let mut encoder = BodyEncoder::new(BodyMode::Chunked, metadata, LIMITS).unwrap();
                let mut writer = Writer::new([]);
                let result = send_body(&mut writer, &mut body, &mut encoder, None).await;
                if late_error {
                    assert!(matches!(result, Err(SendBodyError::Body("late failure"))));
                } else {
                    assert!(matches!(result, Err(SendBodyError::Encode(EncodeError::FrameAfterEnd))));
                }
                assert_eq!(recycled.get(), usize::from(!late_error));
                assert_eq!(writer.output, b"0\r\n\r\n");
            }
        });
    }

    #[test]
    fn recycling_waits_for_write_completion_and_is_not_promised_on_drop() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for drop_activity in [false, true] {
                let recycled = Rc::new(Cell::new(0));
                let mut body = TestBody {
                    recycled: recycled.clone(),
                    frames: VecDeque::from([Ok(Frame::data(Bytes::from_static(b"body")))]),
                };
                let source = CancellationSource::new();
                let observed = Gate::default();
                let complete = Gate::default();
                let mut writer = Writer::new([
                    WriteStep::AfterCancel {
                        token: source.token(),
                        observed: observed.clone(),
                        complete: complete.clone(),
                    },
                    WriteStep::Error(operation_canceled()),
                ]);
                let metadata = BodyMetadata {
                    size: SizeHint::with_exact(4),
                    trailers: TrailerHint::None,
                };
                let mut encoder = BodyEncoder::new(BodyMode::Fixed(4), metadata, LIMITS).unwrap();
                {
                    let mut send = pin!(send_body(&mut writer, &mut body, &mut encoder, Some(source.token())));
                    assert!(poll(send.as_mut()).is_pending());
                    source.cancel();
                    assert!(poll(send.as_mut()).is_pending());
                    assert!(observed.is_open());
                    assert_eq!(recycled.get(), 0);
                    if !drop_activity {
                        complete.open();
                        let Poll::Ready(Err(SendBodyError::Io(error))) = poll(send.as_mut()) else {
                            panic!("write cancellation lost")
                        };
                        assert!(karmaio::runtime::is_operation_canceled(&error));
                    }
                }
                assert_eq!(recycled.get(), usize::from(!drop_activity));
            }
        });
    }
    #[test]
    fn ready_empty_frames_yield_and_cancel_without_transport_submissions() {
        struct EmptyFrames {
            recycled: usize,
        }
        impl Body for EmptyFrames {
            type Data = Bytes;
            type Error = std::convert::Infallible;
            async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, Self::Error> {
                Ok(Some(Frame::data(Bytes::new())))
            }
            fn recycle(&mut self, _: Bytes) {
                self.recycled += 1;
            }
        }
        karmaio::Runtime::new().unwrap().block_on(async {
            let source = CancellationSource::new();
            let mut body = EmptyFrames { recycled: 0 };
            let mut writer = Writer::new([]);
            let metadata = BodyMetadata {
                size: SizeHint::new(),
                trailers: TrailerHint::MayHave,
            };
            let mut encoder = BodyEncoder::new(BodyMode::Chunked, metadata, LIMITS).unwrap();
            {
                let mut send = pin!(send_body(&mut writer, &mut body, &mut encoder, Some(source.token())));
                assert!(poll(send.as_mut()).is_pending());
                source.cancel();
                assert!(matches!(poll(send.as_mut()), Poll::Ready(Err(SendBodyError::Canceled))));
            }
            assert_eq!(body.recycled, 16);
            assert_eq!(writer.submissions, 0);
        });
    }
    #[test]
    fn dropping_body_cancels_and_settles_its_scoped_tcp_read() {
        use crate::io::transport::Tcp;
        use karmaio::net::tcp::TcpListener;
        karmaio::Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
            let _peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, _writer) = stream.into_split();
            let mut tcp = Tcp;
            let (producer, mut body) = incoming(BodyMode::Fixed(1));
            let mut driver = pin!(receive_body(
                &mut reader,
                &mut tcp,
                RecvBuffer::new(8, 16).unwrap(),
                BodyDecoder::new(BodyMode::Fixed(1)),
                producer.unwrap(),
                None
            ));
            assert!(poll(pin!(body.next_frame())).is_pending());
            assert!(poll(driver.as_mut()).is_pending());
            drop(body);
            let (end, buffer) = driver.await;
            assert!(matches!(end, ReceiveEnd::Abandoned));
            assert!(buffer.bytes().is_empty());
        });
    }
}
