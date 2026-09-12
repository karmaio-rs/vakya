use crate::future::WorkBudget;
use karmaio::{
    buf::{BufResult, IoBuf, IoBufExt, IoVectoredBuf},
    io::AsyncWrite,
    runtime::{CancellationToken, FutureExt, operation_canceled},
};
use std::io;

/// Writes an owned scalar buffer completely and returns it on completion.
/// Cancellation requests remain attached until the operation returns its buffer.
/// Dropping this future cannot promise a return to the producer or its recycler.
pub(crate) async fn write_all<W, B>(
    writer: &mut W,
    buffer: B,
    cancellation: Option<CancellationToken>,
) -> BufResult<usize, B>
where
    W: AsyncWrite,
    B: IoBuf,
{
    let future = write_scalar(writer, buffer, cancellation);
    match cancellation {
        Some(token) => future.with_cancellation(token).await,
        None => future.await,
    }
}

/// Writes an owned vectored collection and returns every component on completion.
/// The cursor borrows no payload storage and never flattens the collection.
pub(crate) async fn write_vectored_all<W, V>(
    writer: &mut W,
    buffers: V,
    cancellation: Option<CancellationToken>,
) -> BufResult<usize, V>
where
    W: AsyncWrite,
    V: IoVectoredBuf,
{
    let future = write_vectors(writer, buffers, cancellation);
    match cancellation {
        Some(token) => future.with_cancellation(token).await,
        None => future.await,
    }
}

// Adapted from Karmaio's owned write-all loops: keep its slice types, add a
// cooperative budget and pre-submission cancellation checks, and use the same
// zero/over-report validation for both scalar and vectored writes.
async fn write_scalar<W: AsyncWrite, B: IoBuf>(
    writer: &mut W,
    mut buffer: B,
    cancellation: Option<CancellationToken>,
) -> BufResult<usize, B> {
    let length = buffer.as_init().len();
    let mut written = 0;
    let mut budget = WorkBudget::new();
    while written < length {
        budget.step().await;
        if cancellation.is_some_and(|token| token.is_cancel_requested()) {
            return BufResult(Err(operation_canceled()), buffer);
        }
        let view = buffer.slice(written..length);
        let (result, view) = writer.write(view).await.into_parts();
        buffer = view.into_inner();
        match checked_progress(result, length - written) {
            Ok(count) => {
                crate::trace::progress("write", count);
                written += count;
            }
            Err(error) => return BufResult(Err(error), buffer),
        }
    }
    BufResult(Ok(written), buffer)
}

async fn write_vectors<W: AsyncWrite, V: IoVectoredBuf>(
    writer: &mut W,
    mut buffers: V,
    cancellation: Option<CancellationToken>,
) -> BufResult<usize, V> {
    let length = buffers
        .iter_slice()
        .try_fold(0usize, |total, part| total.checked_add(part.len()));
    let Some(length) = length else {
        return BufResult(
            Err(io::Error::new(io::ErrorKind::InvalidInput, "buffer length overflow")),
            buffers,
        );
    };
    let mut written = 0;
    let mut budget = WorkBudget::new();
    while written < length {
        budget.step().await;
        if cancellation.is_some_and(|token| token.is_cancel_requested()) {
            return BufResult(Err(operation_canceled()), buffers);
        }
        let view = IoVectoredBuf::slice(buffers, written);
        let (result, view) = writer.write_vectored(view).await.into_parts();
        buffers = view.into_inner();
        match checked_progress(result, length - written) {
            Ok(count) => {
                crate::trace::progress("write_vectored", count);
                written += count;
            }
            Err(error) => return BufResult(Err(error), buffers),
        }
    }
    BufResult(Ok(written), buffers)
}

fn checked_progress(result: io::Result<usize>, remaining: usize) -> io::Result<usize> {
    match result {
        Ok(0) => Err(io::ErrorKind::WriteZero.into()),
        Ok(count) if count > remaining => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "writer over-reported completion",
        )),
        // Return zero progress to the budgeted loop for a retry. Explicit
        // Karmaio cancellation is a distinct error, never Interrupted.
        Err(error) if error.kind() == io::ErrorKind::Interrupted => Ok(0),
        result => result,
    }
}

/// Writes optional protocol framing around one owned payload buffer.
///
/// Karmaio's owned vectored slice retains component offsets across
/// partial completions. The returned result strips the bounded framing buffers
/// and restores the original payload for body recycling.
pub(crate) async fn write_framed<W, P, D, S>(
    writer: &mut W,
    prefix: P,
    data: D,
    suffix: S,
    cancellation: Option<CancellationToken>,
) -> BufResult<usize, D>
where
    W: AsyncWrite,
    P: IoBuf,
    D: IoBuf,
    S: IoBuf,
{
    let buffers = (prefix, (data, (suffix,)));
    let (result, (_, (data, (_,)))) = write_vectored_all(writer, buffers, cancellation).await.into_parts();
    BufResult(result, data)
}

#[cfg(test)]
mod tests {
    use super::{write_all, write_framed, write_vectored_all};
    use bytes::Bytes;
    use karmaio::{
        buf::IoBuf,
        runtime::{is_operation_canceled, operation_canceled},
    };
    use std::{cell::Cell, io, rc::Rc};

    use crate::test_transport::{Gate, WriteStep, Writer};
    use karmaio::runtime::CancellationSource;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    struct TrackedData {
        bytes: &'static [u8],
        drops: Rc<Cell<usize>>,
    }

    impl IoBuf for TrackedData {
        fn as_init(&self) -> &[u8] {
            self.bytes
        }
    }

    impl Drop for TrackedData {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn scalar_write_completes_through_every_partial_width() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for width in 1..=6 {
                let mut writer = Writer::limited(width);
                let buffer = Bytes::from_static(b"abcdef");
                let (result, returned) = write_all(&mut writer, buffer, None).await.into_parts();

                assert_eq!(result.unwrap(), 6);
                assert_eq!(returned, b"abcdef"[..]);
                assert_eq!(writer.output, b"abcdef");
            }
        });
    }

    #[test]
    fn vectored_cursor_crosses_every_component_boundary() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for width in 1..=9 {
                let mut writer = Writer::limited(width);
                let buffers = (
                    Bytes::from_static(b"abc"),
                    (
                        Bytes::new(),
                        (Bytes::from_static(b"de"), (Bytes::from_static(b"fghi"),)),
                    ),
                );
                let (result, returned) = write_vectored_all(&mut writer, buffers, None).await.into_parts();

                assert_eq!(result.unwrap(), 9);
                assert_eq!(returned.0, b"abc"[..]);
                assert_eq!(returned.1.1.0, b"de"[..]);
                assert_eq!(returned.1.1.1.0, b"fghi"[..]);
                assert_eq!(writer.output, b"abcdefghi");
            }
        });
    }

    #[test]
    fn framed_write_supports_chunk_and_zero_length_framing() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut chunked = Writer::limited(1);
            let (result, data) = write_framed(
                &mut chunked,
                Bytes::from_static(b"4\r\n"),
                Bytes::from_static(b"body"),
                Bytes::from_static(b"\r\n"),
                None,
            )
            .await
            .into_parts();
            assert_eq!(result.unwrap(), 9);
            assert_eq!(data, b"body"[..]);
            assert_eq!(chunked.output, b"4\r\nbody\r\n");

            let mut fixed = Writer::limited(2);
            let (result, data) = write_framed(
                &mut fixed,
                Bytes::new(),
                Bytes::from_static(b"body"),
                Bytes::new(),
                None,
            )
            .await
            .into_parts();
            assert_eq!(result.unwrap(), 4);
            assert_eq!(data, b"body"[..]);
            assert_eq!(fixed.output, b"body");
        });
    }

    #[test]
    fn io_failure_after_progress_returns_payload_once() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let drops = Rc::new(Cell::new(0));
            let data = TrackedData {
                bytes: b"payload",
                drops: Rc::clone(&drops),
            };
            let mut writer = Writer::new([
                WriteStep::Limit(2),
                WriteStep::Limit(2),
                WriteStep::Error(io::Error::other("failed")),
            ]);

            let (result, data) = write_framed(
                &mut writer,
                Bytes::from_static(b"prefix"),
                data,
                Bytes::from_static(b"suffix"),
                None,
            )
            .await
            .into_parts();

            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
            assert_eq!(drops.get(), 0);
            drop(data);
            assert_eq!(drops.get(), 1);
        });
    }

    #[test]
    fn confirmed_cancellation_returns_payload_once() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for canceled_on_call in [1, 2] {
                let drops = Rc::new(Cell::new(0));
                let data = TrackedData {
                    bytes: b"payload",
                    drops: Rc::clone(&drops),
                };
                let mut steps: Vec<_> = (1..canceled_on_call).map(|_| WriteStep::Limit(3)).collect();
                steps.push(WriteStep::Error(operation_canceled()));
                let mut writer = Writer::new(steps);

                let (result, data) = write_framed(
                    &mut writer,
                    Bytes::from_static(b"prefix"),
                    data,
                    Bytes::from_static(b"suffix"),
                    None,
                )
                .await
                .into_parts();

                assert!(is_operation_canceled(result.as_ref().unwrap_err()));
                assert_eq!(drops.get(), 0);
                drop(data);
                assert_eq!(drops.get(), 1);
            }
        });
    }

    #[test]
    fn control_buffers_cover_heads_final_chunks_and_trailers() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut writer = Writer::limited(2);
            for control in [
                Bytes::from_static(b"HTTP/1.1 200 OK\r\n\r\n"),
                Bytes::from_static(b"0\r\n\r\n"),
                Bytes::from_static(b"0\r\nX-End: yes\r\n\r\n"),
            ] {
                let (result, _) = write_all(&mut writer, control, None).await.into_parts();
                result.unwrap();
            }
            assert_eq!(
                writer.output,
                b"HTTP/1.1 200 OK\r\n\r\n0\r\n\r\n0\r\nX-End: yes\r\n\r\n"
            );
        });
    }
    #[test]
    fn zero_and_invalid_completions_preserve_scalar_and_vectored_buffers() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for (count, kind) in [(0, io::ErrorKind::WriteZero), (100, io::ErrorKind::InvalidData)] {
                let bytes = Bytes::from_static(b"body");
                let pointer = bytes.as_ptr();
                let mut writer = Writer::new([WriteStep::Report(count)]);
                let (result, returned) = write_all(&mut writer, bytes, None).await.into_parts();
                assert_eq!(result.unwrap_err().kind(), kind);
                assert_eq!(returned.as_ptr(), pointer);
                let mut writer = Writer::new([WriteStep::Report(count)]);
                let (result, (returned,)) = write_vectored_all(&mut writer, (returned,), None).await.into_parts();
                assert_eq!(result.unwrap_err().kind(), kind);
                assert_eq!(returned.as_ptr(), pointer);
            }
        });
    }

    #[test]
    fn partial_writes_use_original_payload_addresses_and_retry_interrupted() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let bytes = Bytes::from_static(b"body");
            let pointer = bytes.as_ptr() as usize;
            let steps = || {
                [
                    WriteStep::Limit(2),
                    WriteStep::Error(io::ErrorKind::Interrupted.into()),
                    WriteStep::Limit(2),
                ]
            };
            let mut writer = Writer::new(steps());
            let (result, bytes) = write_all(&mut writer, bytes, None).await.into_parts();
            assert_eq!(result.unwrap(), 4);
            assert_eq!(writer.output, b"body");
            assert_eq!(writer.pointers, [pointer, pointer + 2]);
            let mut writer = Writer::new(steps());
            let (result, (bytes,)) = write_vectored_all(&mut writer, (bytes,), None).await.into_parts();
            assert_eq!(result.unwrap(), 4);
            assert_eq!(writer.output, b"body");
            assert_eq!(writer.pointers, [pointer, pointer + 2]);
            assert_eq!(bytes.as_ptr() as usize, pointer);
        });
    }

    #[test]
    fn ready_writes_yield_and_observe_cancel_before_another_submission() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for vectored in [false, true] {
                let source = CancellationSource::new();
                let mut writer = Writer::limited(1);
                let bytes = Bytes::from(vec![0; 100]);
                {
                    let mut future = pin!(async {
                        if vectored {
                            write_framed(&mut writer, Bytes::new(), bytes, Bytes::new(), Some(source.token())).await
                        } else {
                            write_all(&mut writer, bytes, Some(source.token())).await
                        }
                    });
                    let mut cx = Context::from_waker(Waker::noop());
                    assert!(future.as_mut().poll(&mut cx).is_pending());
                    source.cancel();
                    let Poll::Ready(result) = future.as_mut().poll(&mut cx) else {
                        panic!("canceled retry did not complete")
                    };
                    assert!(is_operation_canceled(result.0.as_ref().unwrap_err()));
                    assert_eq!(result.1.len(), 100);
                }
                assert_eq!(writer.submissions, 16);
                assert_eq!(writer.output.len(), 16);
            }
        });
    }

    #[test]
    fn pending_cancel_and_success_races_return_payload_only_after_completion() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for success in [false, true] {
                let source = CancellationSource::new();
                let observed = Gate::default();
                let complete = Gate::default();
                let drops = Rc::new(Cell::new(0));
                let data = TrackedData {
                    bytes: b"body",
                    drops: drops.clone(),
                };
                let mut writer = Writer::new([
                    WriteStep::AfterCancel {
                        token: source.token(),
                        observed: observed.clone(),
                        complete: complete.clone(),
                    },
                    if success {
                        WriteStep::Limit(4)
                    } else {
                        WriteStep::Error(operation_canceled())
                    },
                ]);
                let mut future = pin!(write_framed(
                    &mut writer,
                    Bytes::new(),
                    data,
                    Bytes::new(),
                    Some(source.token())
                ));
                let mut cx = Context::from_waker(Waker::noop());
                assert!(future.as_mut().poll(&mut cx).is_pending());
                source.cancel();
                assert!(future.as_mut().poll(&mut cx).is_pending());
                assert!(observed.is_open());
                assert_eq!(drops.get(), 0);
                complete.open();
                let Poll::Ready(result) = future.as_mut().poll(&mut cx) else {
                    panic!("completion not observed")
                };
                assert_eq!(drops.get(), 0);
                if success {
                    assert_eq!(result.0.unwrap(), 4);
                } else {
                    assert!(is_operation_canceled(result.0.as_ref().unwrap_err()));
                }
                drop(result.1);
                assert_eq!(drops.get(), 1);
            }
        });
    }
}
