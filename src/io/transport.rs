use super::recv::{ReadStatus, RecvBuffer};
use crate::Error;
use karmaio::{
    io::AsyncRead,
    net::{split::OwnedReadHalf, tcp::TcpStream},
    runtime::CancellationToken,
};

/// Private, statically selected receive policy. Supplied transports use
/// `Portable`; explicit TCP constructors select `Tcp`. These are Karmaio I/O
/// policies, not a runtime abstraction or an erased transport registry.
pub(crate) trait Receive<R> {
    /// Apply an absolute deadline without dropping the retained read.
    async fn read_until(
        &mut self,
        reader: &mut R,
        buffer: RecvBuffer,
        cancellation: Option<CancellationToken>,
        deadline: Option<std::time::Instant>,
    ) -> (Result<ReadStatus, Error>, RecvBuffer) {
        let ((result, buffer), elapsed) =
            super::deadline::settle(deadline, std::pin::pin!(self.read(reader, buffer, cancellation))).await;
        let result = if elapsed && (result.is_ok() || result.as_ref().is_err_and(Error::is_canceled)) {
            Err(Error::new(crate::ErrorKind::Timeout, "HTTP receive deadline exceeded"))
        } else {
            result
        };
        (result, buffer)
    }

    async fn read(
        &mut self,
        reader: &mut R,
        buffer: RecvBuffer,
        cancellation: Option<CancellationToken>,
    ) -> (Result<ReadStatus, Error>, RecvBuffer);
}

#[derive(Debug, Default)]
pub(crate) struct Portable;

impl<R: AsyncRead> Receive<R> for Portable {
    #[inline]
    async fn read(
        &mut self,
        reader: &mut R,
        buffer: RecvBuffer,
        cancellation: Option<CancellationToken>,
    ) -> (Result<ReadStatus, Error>, RecvBuffer) {
        buffer.read(reader, cancellation).await
    }
}

/// Explicit TCP policy: Linux uses demand-driven managed leases when possible;
/// other targets and retained-lease/partial-prefix reads use portable I/O.
#[derive(Debug, Default)]
pub(crate) struct Tcp;

impl Receive<OwnedReadHalf<TcpStream>> for Tcp {
    #[inline]
    async fn read(
        &mut self,
        reader: &mut OwnedReadHalf<TcpStream>,
        buffer: RecvBuffer,
        cancellation: Option<CancellationToken>,
    ) -> (Result<ReadStatus, Error>, RecvBuffer) {
        #[cfg(target_os = "linux")]
        {
            buffer.read_managed(reader, cancellation).await
        }

        #[cfg(not(target_os = "linux"))]
        {
            buffer.read(reader, cancellation).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        future::{Race, race},
        io::send::write_all,
        test_transport::{Gate, ReadStep, Reader, Transport, WriteStep, Writer},
    };
    use bytes::Bytes;
    use karmaio::io::IntoOwnedSplit;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Waker},
    };

    #[test]
    fn supplied_halves_progress_independently_and_race_preserves_pending_io() {
        karmaio::Runtime::new().unwrap().block_on(async {
            for block_read in [false, true] {
                let read_gate = Gate::default();
                let write_gate = Gate::default();
                if block_read {
                    write_gate.open();
                } else {
                    read_gate.open();
                }
                let transport = Transport {
                    reader: Reader::new([
                        ReadStep::Wait(read_gate.clone()),
                        ReadStep::Data(Bytes::from_static(b"input")),
                    ]),
                    writer: Writer::new([WriteStep::Wait(write_gate.clone())]),
                };
                let (mut reader, mut writer) = transport.into_split();
                let mut portable = Portable;
                let mut read = pin!(portable.read(&mut reader, RecvBuffer::new(8, 16).unwrap(), None));
                let mut write = pin!(write_all(&mut writer, Bytes::from_static(b"output"), None));
                match race(read.as_mut(), write.as_mut()).await {
                    Race::First((result, buffer)) => {
                        assert!(!block_read);
                        assert_eq!(result.unwrap(), ReadStatus::Data(5));
                        assert_eq!(buffer.bytes(), b"input");
                        write_gate.open();
                        assert_eq!(write.await.0.unwrap(), 6);
                    }
                    Race::Second(result) => {
                        assert!(block_read);
                        assert_eq!(result.0.unwrap(), 6);
                        read_gate.open();
                        let (result, buffer) = read.await;
                        assert_eq!(result.unwrap(), ReadStatus::Data(5));
                        assert_eq!(buffer.bytes(), b"input");
                    }
                }
            }
        });
    }

    #[test]
    fn tcp_strategy_recovers_buffer_after_kernel_read_cancellation() {
        use karmaio::{net::tcp::TcpListener, runtime::CancellationSource};
        use std::io::Write;
        karmaio::Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
            let mut peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, _writer) = stream.into_split();
            peer.write_all(b"prefix").unwrap();
            let mut tcp = Tcp;
            let mut buffer = RecvBuffer::new(8, 16).unwrap();
            while buffer.bytes().len() < 6 {
                let (result, returned) = tcp.read(&mut reader, buffer, None).await;
                assert!(matches!(result.unwrap(), ReadStatus::Data(count) if count > 0));
                buffer = returned;
            }
            assert_eq!(buffer.bytes(), b"prefix");
            let source = CancellationSource::new();
            let mut read = pin!(tcp.read(&mut reader, buffer, Some(source.token())));
            assert!(read.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
            source.cancel();
            let (result, buffer) = read.await;
            assert_eq!(result.unwrap_err().kind(), crate::ErrorKind::Canceled);
            assert_eq!(buffer.bytes(), b"prefix");
        });
    }
}
