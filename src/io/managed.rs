use crate::{Error, ErrorKind};
use karmaio::{
    buf::{IoBuf, PooledBuf},
    io::AsyncReadManaged,
    net::{split::OwnedReadHalf, tcp::TcpStream},
    runtime::{CancellationToken, FutureExt},
};

/// Performs one demand-driven managed receive and returns its pool lease.
pub(crate) async fn read(
    reader: &mut OwnedReadHalf<TcpStream>,
    requested: usize,
    cancellation: Option<CancellationToken>,
) -> Result<Option<PooledBuf>, Error> {
    let result = match cancellation {
        Some(token) => reader.read_managed(requested).with_cancellation(token).await,
        None => reader.read_managed(requested).await,
    };

    match result {
        Ok(Some(buffer)) if buffer.as_init().is_empty() => Err(Error::new(
            ErrorKind::Internal,
            "managed stream receive returned an empty lease",
        )),
        Ok(Some(buffer)) if requested != 0 && buffer.as_init().len() > requested => Err(Error::new(
            ErrorKind::Internal,
            "managed stream receive exceeded its requested range",
        )),
        Ok(buffer) => Ok(buffer),
        Err(error) => Err(map_error(error)),
    }
}

fn map_error(error: std::io::Error) -> Error {
    Error::from(error)
}

#[cfg(test)]
mod tests {
    use super::{map_error, read};
    use crate::ErrorKind;
    use karmaio::{
        net::tcp::TcpListener,
        runtime::{CancellationSource, spawn_local},
        time::sleep,
    };
    use std::{io::Write, net::SocketAddr, thread, time::Duration};

    #[test]
    fn managed_read_reports_exact_ranges_and_eof() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).unwrap();
            let address = listener.local_addr().unwrap();
            let client = thread::spawn(move || {
                let mut stream = std::net::TcpStream::connect(address).unwrap();
                stream.write_all(b"hello").unwrap();
            });

            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, writer) = stream.into_split();
            let first = read(&mut reader, 3, None).await.unwrap().unwrap();
            assert_eq!(&first[..], b"hel");
            first.release();

            let second = read(&mut reader, 3, None).await.unwrap().unwrap();
            assert_eq!(&second[..], b"lo");
            second.release();
            assert!(read(&mut reader, 3, None).await.unwrap().is_none());

            drop(reader);
            drop(writer);

            client.join().unwrap();
        });
    }

    #[test]
    fn managed_read_maps_cancellation() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).unwrap();
            let address = listener.local_addr().unwrap();
            let client = thread::spawn(move || std::net::TcpStream::connect(address).unwrap());

            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, writer) = stream.into_split();
            let source = CancellationSource::new();
            let token = source.token();
            spawn_local(async move {
                sleep(Duration::from_millis(20)).await;
                source.cancel();
            });

            let error = read(&mut reader, 8, Some(token)).await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Canceled);

            drop(reader);
            drop(writer);
            drop(client.join().unwrap());
        });
    }

    #[test]
    fn managed_read_preserves_io_errors() {
        let error = map_error(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        assert_eq!(error.kind(), ErrorKind::Io);
    }
}
