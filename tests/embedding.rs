#![cfg(any(feature = "client", feature = "server"))]
mod support;
use bytes::Bytes;
use std::pin::pin;
use support::future::poll;
use support::transport::{ReadStep, Reader};
#[cfg(feature = "server")]
use vakya::body::Empty;
#[cfg(feature = "client")]
use vakya::body::Full;
use vakya::{Request, connection::ConnectionOutcome};

struct Flags {
    flush: support::transport::Gate,
    flushing: std::cell::Cell<bool>,
    shutdowns: std::cell::Cell<usize>,
    output: std::cell::RefCell<Vec<u8>>,
}
struct CountWriter(std::rc::Rc<Flags>);
impl karmaio::io::AsyncWrite for CountWriter {
    async fn write<B: karmaio::buf::IoBuf>(&mut self, buffer: B) -> karmaio::buf::BufResult<usize, B> {
        self.0.output.borrow_mut().extend_from_slice(buffer.as_init());
        karmaio::buf::BufResult(Ok(buffer.as_init().len()), buffer)
    }
    async fn flush(&mut self) -> std::io::Result<()> {
        self.0.flushing.set(true);
        self.0.flush.wait().await;
        Ok(())
    }
    async fn shutdown(&mut self) -> std::io::Result<()> {
        self.0.shutdowns.set(self.0.shutdowns.get() + 1);
        Ok(())
    }
}
fn flags() -> std::rc::Rc<Flags> {
    std::rc::Rc::new(Flags {
        flush: Default::default(),
        flushing: Default::default(),
        shutdowns: Default::default(),
        output: Default::default(),
    })
}

#[cfg(feature = "client")]
#[test]
fn client_recovery_waits_for_flush_preserves_read_ahead_and_avoids_shutdown() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let flags = flags();
        let mut body = Full::new(Bytes::from_static(b"upload"));
        let (sender, connection) = vakya::client::conn::http1::Builder::new().handshake((
            Reader::new([ReadStep::Data(Bytes::from_static(
                b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\nprefix",
            ))]),
            CountWriter(flags.clone()),
        ));
        let connection: vakya::client::conn::http1::BoxedConnection<'_, Reader, CountWriter> = connection.boxed();
        let pending = sender
            .start_request(
                Request::builder()
                    .uri("/")
                    .header("host", "test")
                    .body(&mut body)
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut run = pin!(connection.run_without_shutdown());
        assert!(poll(run.as_mut()).is_pending());
        pending.response().await.unwrap();
        assert!(poll(run.as_mut()).is_pending());
        assert!(flags.flushing.get());
        assert_eq!(flags.shutdowns.get(), 0);
        flags.flush.open();
        let ConnectionOutcome::Recovered(parts) = run.await.unwrap() else {
            panic!("missing recovered parts")
        };
        assert_eq!(parts.read_ahead, "prefix");
        assert!(flags.output.borrow().ends_with(b"upload"));
        assert_eq!(parts.reader.submissions, 1);
        assert!(std::rc::Rc::ptr_eq(&parts.writer.0, &flags));
        assert_eq!(flags.shutdowns.get(), 0);
    });
}

#[cfg(feature = "server")]
#[test]
fn server_recovery_settles_idle_read_and_preserves_bytes_returned_after_cancel() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let complete = support::transport::Gate::default();
        let flags = flags();
        flags.flush.open();
        let calls = std::cell::Cell::new(0);
        let service = vakya::server::request_service_fn(async |_: Request<vakya::body::Incoming>| {
            calls.set(calls.get() + 1);
            panic!("unadmitted idle input must be returned");
            #[allow(unreachable_code)]
            Ok::<_, std::convert::Infallible>(vakya::Response::new(Empty::new()))
        });
        let connection = vakya::server::conn::http1::Builder::new().serve_connection(
            (
                Reader::new([
                    ReadStep::Wait(complete.clone()),
                    ReadStep::Data(Bytes::from_static(b"GET /pending HTTP/1.1\r\n")),
                ]),
                CountWriter(flags.clone()),
            ),
            service,
        );
        let control = connection.control();
        let connection: vakya::server::conn::http1::BoxedConnection<'_, Reader, CountWriter> = connection.boxed();
        let mut run = pin!(connection.run_without_shutdown());
        assert!(poll(run.as_mut()).is_pending());
        control.graceful_shutdown();
        assert!(poll(run.as_mut()).is_pending());
        assert_eq!(flags.shutdowns.get(), 0);
        complete.open();
        let ConnectionOutcome::Recovered(parts) = run.await.unwrap() else {
            panic!("missing recovered parts")
        };
        assert_eq!(parts.read_ahead, "GET /pending HTTP/1.1\r\n");
        assert_eq!(calls.get(), 0);
        assert_eq!(parts.reader.submissions, 1);
        assert_eq!(flags.shutdowns.get(), 0);
    });
}

#[cfg(feature = "server")]
#[test]
fn server_recovery_returns_buffered_input_after_clean_message_closure() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let flags = flags();
        flags.flush.open();
        let service = vakya::server::request_service_fn(async |_: Request<vakya::body::Incoming>| {
            Ok::<_, std::convert::Infallible>(vakya::Response::new(Empty::new()))
        });
        let connection = vakya::server::conn::http1::Builder::new().serve_connection(
            (
                Reader::new([ReadStep::Data(Bytes::from_static(
                    b"GET / HTTP/1.1\r\nhost: test\r\nconnection: close\r\n\r\nnext",
                ))]),
                CountWriter(flags.clone()),
            ),
            service,
        );
        let ConnectionOutcome::Recovered(parts) = connection.run_without_shutdown().await.unwrap() else {
            panic!("missing recovered parts")
        };
        assert_eq!(parts.read_ahead, "next");
        assert_eq!(flags.shutdowns.get(), 0);
    });
}

#[cfg(feature = "server")]
#[test]
fn recovery_mode_retires_abandonment_and_rejects_abort_or_invalid_protocol() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for mode in ["abandon", "abort", "invalid"] {
            let flags = flags();
            flags.flush.open();
            let service = vakya::server::request_service_fn(async |_: Request<vakya::body::Incoming>| {
                Ok::<_, std::convert::Infallible>(vakya::Response::new(Empty::new()))
            });
            let wire = if mode == "invalid" {
                b"bad\r\n\r\n".as_slice()
            } else {
                b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 1\r\n\r\n".as_slice()
            };
            let connection = vakya::server::conn::http1::Builder::new().serve_connection(
                (
                    Reader::new([ReadStep::Data(Bytes::from_static(wire))]),
                    CountWriter(flags.clone()),
                ),
                service,
            );
            if mode == "abort" {
                connection.control().abort();
            }
            let result = connection.run_without_shutdown().await;
            if mode == "abandon" {
                assert!(matches!(result.unwrap(), ConnectionOutcome::Closed));
                assert_eq!(flags.shutdowns.get(), 1);
            } else {
                assert!(result.is_err());
            }
        }
    });
}

#[cfg(feature = "client")]
#[test]
fn client_recovery_mode_still_retires_upload_abort_and_body_abandonment() {
    struct WaitingBody;
    impl vakya::body::Body for WaitingBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        async fn next_frame(&mut self) -> Result<Option<vakya::body::Frame<Bytes>>, Self::Error> {
            std::future::pending().await
        }
        fn size_hint(&self) -> vakya::body::SizeHint {
            vakya::body::SizeHint::with_exact(1)
        }
        fn trailer_hint(&self) -> vakya::body::TrailerHint {
            vakya::body::TrailerHint::None
        }
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        for abort in [false, true] {
            let flags = flags();
            flags.flush.open();
            let wire = if abort {
                b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n".as_slice()
            } else {
                b"HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\n".as_slice()
            };
            let (sender, connection) = vakya::client::conn::http1::Builder::new().handshake((
                Reader::new([ReadStep::Data(Bytes::from_static(wire))]),
                CountWriter(flags.clone()),
            ));
            let pending = sender
                .start_request(
                    Request::builder()
                        .uri("/")
                        .header("host", "test")
                        .body(WaitingBody)
                        .unwrap(),
                )
                .await
                .unwrap();
            let upload = pending.control();
            let mut run = pin!(connection.run_without_shutdown());
            assert!(poll(run.as_mut()).is_pending());
            let response = pending.response().await.unwrap();
            if abort {
                upload.abort();
            }
            drop(response);
            assert!(matches!(run.await.unwrap(), ConnectionOutcome::Closed));
            assert_eq!(flags.shutdowns.get(), 1);
        }
    });
}
