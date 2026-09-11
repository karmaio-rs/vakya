//! Deadline scopes retain submitted operations until their buffers return.
use crate::future::{Race, race};
use karmaio::{
    buf::{BufResult, IoBuf, IoVectoredBuf},
    io::AsyncWrite,
    runtime::{CancellationSource, FutureExt, is_operation_canceled},
};
use std::{
    future::Future,
    io,
    pin::pin,
    time::{Duration, Instant},
};

/// Returns the operation's original output even after timeout. Callers replace
/// only successful/cancellation results, preserving independent failures.
pub(crate) async fn settle<F: Future>(deadline: Option<Instant>, future: std::pin::Pin<&mut F>) -> (F::Output, bool) {
    let Some(deadline) = deadline else {
        return (future.await, false);
    };
    let source = CancellationSource::new();
    let mut operation = pin!(future.with_cancellation(source.token()));
    let mut timer = pin!(karmaio::time::sleep_until(deadline));
    // A completed operation wins a simultaneous deadline, retaining real errors.
    match race(operation.as_mut(), timer.as_mut()).await {
        Race::First(output) => (output, false),
        Race::Second(()) => {
            source.cancel();
            (operation.await, true)
        }
    }
}

#[allow(dead_code)] // Role builders use this; HTTP/1 alone has no driver.
pub(crate) fn after(timeout: Option<Duration>) -> io::Result<Option<Instant>> {
    timeout
        .map(|duration| {
            Instant::now().checked_add(duration).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "timeout cannot be represented as a deadline",
                )
            })
        })
        .transpose()
}

/// Validate local configuration without converting overflow into no deadline.
#[allow(dead_code)]
pub(crate) fn configured_after(timeout: Option<Duration>) -> Result<Option<Instant>, crate::Error> {
    after(timeout)
        .map_err(|error| crate::Error::with_source(crate::ErrorKind::LocalMessage, "invalid timeout duration", error))
}

#[allow(dead_code)]
fn timed<T>(result: io::Result<T>, elapsed: bool) -> io::Result<T> {
    if elapsed && (result.is_ok() || result.as_ref().is_err_and(is_operation_canceled)) {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "HTTP transport progress deadline exceeded",
        ))
    } else {
        result
    }
}

/// Static, private policy over the concrete write half. Each completion resets
/// the progress budget; time spent waiting for application frames is excluded.
#[allow(dead_code)]
pub(crate) struct Write<W> {
    pub(crate) inner: W,
    pub(crate) timeout: Option<Duration>,
}

impl<W: AsyncWrite> AsyncWrite for Write<W> {
    async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
        let deadline = match after(self.timeout) {
            Ok(deadline) => deadline,
            Err(error) => return BufResult(Err(error), buffer),
        };
        let (BufResult(result, buffer), elapsed) = settle(deadline, pin!(self.inner.write(buffer))).await;
        BufResult(timed(result, elapsed), buffer)
    }
    async fn write_vectored<B: IoVectoredBuf>(&mut self, buffers: B) -> BufResult<usize, B> {
        let deadline = match after(self.timeout) {
            Ok(deadline) => deadline,
            Err(error) => return BufResult(Err(error), buffers),
        };
        let (BufResult(result, buffers), elapsed) = settle(deadline, pin!(self.inner.write_vectored(buffers))).await;
        BufResult(timed(result, elapsed), buffers)
    }
    async fn flush(&mut self) -> io::Result<()> {
        let (result, elapsed) = settle(after(self.timeout)?, pin!(self.inner.flush())).await;
        timed(result, elapsed)
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        let (result, elapsed) = settle(after(self.timeout)?, pin!(self.inner.shutdown())).await;
        timed(result, elapsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_transport::Gate;
    use std::{cell::Cell, future::poll_fn, task::Poll};

    #[test]
    fn deadline_overflow_is_an_error_and_never_disables_the_budget() {
        assert!(after(None).unwrap().is_none());
        let before = Instant::now();
        let deadline = after(Some(Duration::ZERO)).unwrap().unwrap();
        assert!(deadline >= before && deadline <= Instant::now());
        assert!(after(Some(Duration::from_secs(1))).unwrap().is_some());
        assert_eq!(
            after(Some(Duration::MAX)).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            configured_after(Some(Duration::MAX)).unwrap_err().kind(),
            crate::ErrorKind::LocalMessage
        );
    }

    #[test]
    fn invalid_write_deadline_returns_owned_buffers_without_submitting() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let mut writer = Write {
                inner: crate::test_transport::Writer::new([]),
                timeout: Some(Duration::MAX),
            };
            let buffer = b"payload".to_vec();
            let address = buffer.as_ptr();
            let (result, returned) = writer.write(buffer).await.into_parts();
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
            assert_eq!(returned.as_ptr(), address);
            assert_eq!(returned, b"payload");
            let buffers = vec![b"a".to_vec(), b"b".to_vec()];
            let addresses = [buffers[0].as_ptr(), buffers[1].as_ptr()];
            let (result, returned) = writer.write_vectored(buffers).await.into_parts();
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
            assert_eq!([returned[0].as_ptr(), returned[1].as_ptr()], addresses);
            assert_eq!(writer.flush().await.unwrap_err().kind(), io::ErrorKind::InvalidInput);
            assert_eq!(writer.shutdown().await.unwrap_err().kind(), io::ErrorKind::InvalidInput);
            assert_eq!(writer.inner.submissions, 0);
        });
    }

    #[test]
    fn elapsed_deadline_waits_for_completion_and_preserves_original_failure() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let complete = Gate::default();
            let returned = Cell::new(false);
            let operation = async {
                complete.wait().await;
                returned.set(true);
                Err::<(), _>(io::Error::new(io::ErrorKind::ConnectionReset, "original"))
            };
            let mut operation = pin!(operation);
            let mut work = pin!(settle(Some(Instant::now()), operation.as_mut()));
            assert!(poll_fn(|cx| Poll::Ready(work.as_mut().poll(cx))).await.is_pending());
            karmaio::time::sleep(Duration::from_millis(2)).await;
            assert!(poll_fn(|cx| Poll::Ready(work.as_mut().poll(cx))).await.is_pending());
            assert!(!returned.get());
            complete.open();
            let (result, elapsed) = work.await;
            assert!(elapsed);
            assert!(returned.get());
            assert_eq!(
                timed(result, elapsed).unwrap_err().kind(),
                io::ErrorKind::ConnectionReset
            );
        });
    }
}
