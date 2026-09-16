#![cfg(feature = "server")]
mod support;
use bytes::Bytes;
use karmaio::{
    buf::{BufResult, IoBuf, IoVectoredBuf},
    io::AsyncWrite,
};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    future::{Future, poll_fn},
    io,
    pin::{Pin, pin},
    rc::Rc,
    task::{Context, Poll, Waker},
};
use support::transport::{ReadStep, Reader, Writer};
use vakya::{
    Request, Response,
    body::{Body, BodyExt, Empty, Frame, Full, Incoming, SizeHint, TrailerHint},
    error::{Error, ErrorKind},
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
};

struct ObservedWriter {
    writer: Writer,
    output: Rc<RefCell<Vec<u8>>>,
}
impl ObservedWriter {
    fn new() -> (Self, Rc<RefCell<Vec<u8>>>) {
        let output = Rc::new(RefCell::new(Vec::new()));
        (
            Self {
                writer: Writer::limited(3),
                output: output.clone(),
            },
            output,
        )
    }
    fn record(&self) {
        let mut output = self.output.borrow_mut();
        let start = output.len();
        output.extend_from_slice(&self.writer.output[start..]);
    }
}
impl AsyncWrite for ObservedWriter {
    async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
        let result = self.writer.write(buffer).await;
        self.record();
        result
    }
    async fn write_vectored<V: IoVectoredBuf>(&mut self, buffers: V) -> BufResult<usize, V> {
        let result = self.writer.write_vectored(buffers).await;
        self.record();
        result
    }
    async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        self.writer.shutdown().await
    }
}
fn reader(bytes: &'static [u8]) -> Reader {
    Reader::new([ReadStep::Data(Bytes::from_static(bytes))])
}
fn output(bytes: &Rc<RefCell<Vec<u8>>>) -> String {
    String::from_utf8(bytes.borrow().clone()).unwrap()
}
fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
async fn pair<A: Future, B: Future>(first: A, second: B) -> (A::Output, B::Output) {
    let mut first = pin!(first);
    let mut second = pin!(second);
    let mut a = None;
    let mut b = None;
    poll_fn(|cx| {
        if a.is_none()
            && let Poll::Ready(value) = first.as_mut().poll(cx)
        {
            a = Some(value);
        }
        if b.is_none()
            && let Poll::Ready(value) = second.as_mut().poll(cx)
        {
            b = Some(value);
        }
        if a.is_some() && b.is_some() {
            Poll::Ready((a.take().unwrap(), b.take().unwrap()))
        } else {
            Poll::Pending
        }
    })
    .await
}

#[test]
fn http10_expectation_and_upgrade_reach_service_without_informational_or_handoff() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for fields in [
            "Expect: 100-continue\r\n",
            "Connection: upgrade\r\nUpgrade: websocket\r\n",
            "Expect: 100-continue\r\nConnection: upgrade\r\nUpgrade: websocket\r\n",
        ] {
            let called = Cell::new(false);
            let service = service_fn(async |(request, _): (Request<Incoming>, RequestContext)| {
                called.set(true);
                assert_eq!(request.version(), vakya::Version::HTTP_10);
                assert_eq!(request.into_body().collect(1).await.unwrap().bytes().as_ref(), b"x");
                Ok::<_, Infallible>(Response::new(Empty::new()))
            });
            let wire = Bytes::from(format!("POST / HTTP/1.0\r\nContent-Length: 1\r\n{fields}\r\nx"));
            let (writer, bytes) = ObservedWriter::new();
            let result = Builder::new()
                .auto_date(false)
                .serve_connection((Reader::new([ReadStep::Data(wire)]), writer), service)
                .run()
                .await
                .unwrap();
            assert!(matches!(result, vakya::connection::ConnectionOutcome::Closed));
            assert!(called.get());
            let wire = output(&bytes);
            assert!(wire.starts_with("HTTP/1.0 200 OK\r\n"));
            assert_eq!(wire.matches("HTTP/").count(), 1);
        }
    });
}

#[test]
fn keep_alive_preserves_buffered_requests_and_borrowed_local_service() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let calls = Cell::new(0);
        let local = Rc::new(String::from("reply"));
        let service = service_fn(async |(request, context): (Request<Incoming>, RequestContext)| {
            calls.set(calls.get() + 1);
            if request.uri().path() == "/two" { context.close_connection(); }
            Ok::<_, Infallible>(Response::new(Full::new(Bytes::copy_from_slice(local.as_bytes()))))
        });
        let (writer, bytes) = ObservedWriter::new();
        Builder::new().auto_date(false).serve_connection((reader(b"GET /one HTTP/1.1\r\nHost: test\r\n\r\nGET /two HTTP/1.1\r\nHost: test\r\n\r\nGET /ignored HTTP/1.1\r\nHost: test\r\n\r\n"), writer), service).run().await.unwrap();
        assert_eq!(calls.get(), 2);
        let wire = output(&bytes);
        assert_eq!(wire.matches("HTTP/1.1 200 OK").count(), 2);
        assert_eq!(wire.matches("reply").count(), 2);
        assert!(wire.contains("connection: close\r\n"));
        assert!(!wire.contains("date:"));
    });
}

#[test]
fn preserved_request_header_case_is_reused_when_forwarded() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let service = service_fn(async |(request, _): (Request<Incoming>, RequestContext)| {
            let (parts, _) = request.into_parts();
            let mut response = Response::new(Empty::new());
            *response.headers_mut() = parts.headers;
            *response.extensions_mut() = parts.extensions;
            Ok::<_, Infallible>(response)
        });
        let (writer, bytes) = ObservedWriter::new();
        Builder::new()
            .preserve_header_case(true)
            .title_case_headers(true)
            .auto_date(false)
            .serve_connection(
                (
                    reader(b"GET / HTTP/1.1\r\nhOsT: test\r\nx-WeIrD: one\r\nX-WEIRD: two\r\n\r\n"),
                    writer,
                ),
                service,
            )
            .run()
            .await
            .unwrap();

        let wire = output(&bytes);
        assert!(wire.contains("\r\nhOsT: test\r\n"));
        assert!(wire.contains("\r\nx-WeIrD: one\r\n"));
        assert!(wire.contains("\r\nX-WEIRD: two\r\n"));
        assert!(wire.contains("\r\nContent-Length: 0\r\n"));
    });
}

#[test]
fn streaming_echo_preserves_trailers_and_applies_title_case() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let service = service_fn(async |(request, context): (Request<Incoming>, RequestContext)| {
            drop(context);
            Ok::<_, Infallible>(Response::new(request.into_body()))
        });
        let (writer, bytes) = ObservedWriter::new();
        let input = Reader::new([
            ReadStep::Data(Bytes::from_static(
                b"POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            )),
            ReadStep::Data(Bytes::from_static(b"4\r\nbody\r\n0\r\nx-end: yes\r\n\r\n")),
        ]);
        Builder::new()
            .title_case_headers(true)
            .auto_date(false)
            .serve_connection((input, writer), service)
            .run()
            .await
            .unwrap();
        let wire = output(&bytes);
        assert!(wire.contains("Transfer-Encoding: chunked\r\n"));
        assert!(wire.ends_with("\r\n\r\n4\r\nbody\r\n0\r\nX-End: yes\r\n\r\n"));
    });
}

#[test]
fn early_final_does_not_cancel_an_externally_retained_upload() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for status in [200, 400] {
            let retained = Rc::new(RefCell::new(None));
            let service = service_fn(async |(request, _context): (Request<Incoming>, RequestContext)| {
                *retained.borrow_mut() = Some(request.into_body());
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(status)
                        .body(Full::new(Bytes::from_static(b"early")))
                        .unwrap(),
                )
            });
            let (writer, bytes) = ObservedWriter::new();
            let input = Reader::new([
                ReadStep::Data(Bytes::from_static(
                    b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 6\r\nConnection: close\r\n\r\n",
                )),
                ReadStep::Data(Bytes::from_static(b"upload")),
            ]);
            let connection = Builder::new()
                .auto_date(false)
                .serve_connection((input, writer), service);
            let mut driver = pin!(connection.run());
            // Partial writes may spend a work budget before the final body is sent.
            for _ in 0..10 {
                assert!(poll(driver.as_mut()).is_pending());
                if output(&bytes).ends_with("early") {
                    break;
                }
            }
            assert!(output(&bytes).ends_with("early"));
            let body = retained.borrow_mut().take().unwrap();
            let (result, body) = pair(driver, body.collect(6)).await;
            result.unwrap();
            assert_eq!(body.unwrap().as_ref(), b"upload");
        }
    });
}

#[test]
fn abandoning_request_body_finishes_response_and_retires_connection() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let calls = Cell::new(0);
        let service = service_fn(async |(_request, _context): (Request<Incoming>, RequestContext)| {
            calls.set(calls.get() + 1);
            Ok::<_, Infallible>(Response::builder().status(400).body(Full::new(Bytes::from_static(b"rejected"))).unwrap())
        });
        let (writer, bytes) = ObservedWriter::new();
        Builder::new().serve_connection((reader(b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\nbodyGET / HTTP/1.1\r\nHost: test\r\n\r\n"), writer), service).run().await.unwrap();
        assert_eq!(calls.get(), 1);
        let wire = output(&bytes);
        assert!(wire.contains("400 Bad Request"));
        assert!(wire.contains("connection: close\r\n"));
        assert!(wire.ends_with("rejected"));
    });
}

struct CountedBody {
    polls: Rc<Cell<usize>>,
}
impl Body for CountedBody {
    type Data = Bytes;
    type Error = Infallible;
    async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, Infallible> {
        self.polls.set(self.polls.get() + 1);
        Ok(Some(Frame::data(Bytes::from_static(b"body"))))
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(4)
    }
    fn trailer_hint(&self) -> TrailerHint {
        TrailerHint::None
    }
}

#[test]
fn head_suppresses_body_production_and_preserves_representation_length() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let polls = Rc::new(Cell::new(0));
        let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
            Ok::<_, Infallible>(Response::new(CountedBody { polls: polls.clone() }))
        });
        let (writer, bytes) = ObservedWriter::new();
        Builder::new()
            .auto_date(false)
            .serve_connection(
                (
                    reader(b"HEAD / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"),
                    writer,
                ),
                service,
            )
            .run()
            .await
            .unwrap();
        assert_eq!(polls.get(), 0);
        assert!(output(&bytes).contains("content-length: 4\r\n"));
        assert!(output(&bytes).ends_with("\r\n\r\n"));
    });
}

#[test]
fn date_insertion_preserves_supplied_values_and_can_be_disabled() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for (automatic, supplied) in [(true, false), (true, true), (false, false)] {
            let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
                let mut response = Response::new(Empty::new());
                if supplied {
                    response
                        .headers_mut()
                        .insert("date", "Sun, 06 Nov 1994 08:49:37 GMT".parse().unwrap());
                }
                Ok::<_, Infallible>(response)
            });
            let (writer, bytes) = ObservedWriter::new();
            Builder::new()
                .auto_date(automatic)
                .serve_connection((reader(b"GET / HTTP/1.0\r\n\r\n"), writer), service)
                .run()
                .await
                .unwrap();
            let wire = output(&bytes);
            assert!(wire.starts_with("HTTP/1.0 200 OK"));
            let date = wire.lines().find_map(|line| line.strip_prefix("date: "));
            if supplied {
                assert_eq!(date, Some("Sun, 06 Nov 1994 08:49:37 GMT"));
            } else if automatic {
                assert!(httpdate::parse_http_date(date.unwrap()).is_ok());
            } else {
                assert!(date.is_none());
            }
        }
    });
}

#[derive(Debug)]
struct LocalError(Rc<()>);
impl std::fmt::Display for LocalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("local service failure")
    }
}
impl std::error::Error for LocalError {}

#[test]
fn service_and_body_errors_retain_local_sources() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let identity = Rc::new(());
        let service = service_fn(
            async |_: (Request<Incoming>, RequestContext)| -> Result<Response<Empty>, LocalError> {
                Err(LocalError(identity.clone()))
            },
        );
        let error = Builder::new()
            .serve_connection(
                (reader(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n"), Writer::new([])),
                service,
            )
            .run()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Service);
        let source = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<LocalError>()
            .unwrap();
        assert!(Rc::ptr_eq(&source.0, &identity));

        struct FailedBody(Rc<()>);
        impl Body for FailedBody {
            type Data = Bytes;
            type Error = LocalError;
            async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, LocalError> {
                Err(LocalError(self.0.clone()))
            }
        }
        let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
            Ok::<_, Infallible>(Response::new(FailedBody(identity.clone())))
        });
        let error = Builder::new()
            .serve_connection(
                (reader(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n"), Writer::new([])),
                service,
            )
            .run()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Body);
        let source = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<LocalError>()
            .unwrap();
        assert!(Rc::ptr_eq(&source.0, &identity));
    });
}

#[test]
fn receive_failure_preserves_source_when_service_is_still_waiting() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let service = service_fn(
            async |(request, _): (Request<Incoming>, RequestContext)| -> Result<Response<Empty>, Error> {
                let _ = request.into_body().collect(8).await;
                std::future::pending().await
            },
        );
        let input = Reader::new([
            ReadStep::Data(Bytes::from_static(
                b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\n",
            )),
            ReadStep::Error(io::Error::other("original read failure")),
        ]);
        let error = Builder::new()
            .serve_connection((input, Writer::new([])), service)
            .run()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Io);
        let source = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap();
        assert_eq!(source.to_string(), "original read failure");
    });
}

#[test]
fn invalid_heads_and_local_framing_fail_before_response_bytes_are_written() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
            Ok::<_, Infallible>(
                Response::builder()
                    .header("content-length", "9")
                    .body(Full::new(Bytes::from_static(b"body")))
                    .unwrap(),
            )
        });
        let (writer, bytes) = ObservedWriter::new();
        let error = Builder::new()
            .serve_connection((reader(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n"), writer), service)
            .run()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::LocalMessage);
        assert!(bytes.borrow().is_empty());
        let calls = Cell::new(0);
        let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
            calls.set(1);
            Ok::<_, Infallible>(Response::new(Empty::new()))
        });
        let error = Builder::new()
            .serve_connection(
                (
                    reader(b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\n"),
                    Writer::new([]),
                ),
                service,
            )
            .run()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidMessage);
        assert_eq!(calls.get(), 0);
    });
}

#[test]
fn supplied_tcp_connection_serves_a_complete_exchange() {
    use std::io::{Read, Write};
    karmaio::Runtime::new().unwrap().block_on(async {
        let listener =
            karmaio::net::tcp::TcpListener::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
        let address = listener.local_addr().unwrap();
        let peer = std::thread::spawn(move || {
            let mut socket = std::net::TcpStream::connect(address).unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(3)))
                .unwrap();
            socket
                .write_all(b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody")
                .unwrap();
            let mut wire = String::new();
            socket.read_to_string(&mut wire).unwrap();
            wire
        });
        let (socket, _) = listener.accept().await.unwrap();
        let service = service_fn(async |(request, _): (Request<Incoming>, RequestContext)| {
            Ok::<_, Infallible>(Response::new(request.into_body()))
        });
        Builder::new()
            .auto_date(false)
            .serve_tcp(socket, service)
            .run()
            .await
            .unwrap();
        let wire = peer.join().unwrap();
        assert!(wire.starts_with("HTTP/1.1 200 OK"));
        assert!(wire.ends_with("\r\n\r\nbody"));
    });
}

#[test]
fn response_write_failure_settles_an_already_submitted_request_read() {
    use karmaio::{buf::IoBufMut, io::AsyncRead};
    use support::transport::{Gate, WriteStep};
    struct Counted {
        inner: Reader,
        calls: Rc<Cell<usize>>,
    }
    impl AsyncRead for Counted {
        async fn read<B: IoBufMut>(&mut self, buffer: B) -> BufResult<usize, B> {
            self.calls.set(self.calls.get() + 1);
            self.inner.read(buffer).await
        }
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        let retained = Rc::new(RefCell::new(None));
        let service = service_fn(
            async |(request, _): (Request<Incoming>, RequestContext)| -> Result<Response<Empty>, Error> {
                let mut body = request.into_body();
                assert_eq!(body.next_frame().await?.unwrap().into_data().unwrap().as_ref(), b"a");
                assert!(
                    poll_fn(|cx| {
                        let mut next = pin!(body.next_frame());
                        Poll::Ready(next.as_mut().poll(cx))
                    })
                    .await
                    .is_pending()
                );
                *retained.borrow_mut() = Some(body);
                Ok(Response::new(Empty::new()))
            },
        );
        let complete = Gate::default();
        let calls = Rc::new(Cell::new(0));
        let input = Counted {
            calls: calls.clone(),
            inner: Reader::new([
                ReadStep::Data(Bytes::from_static(
                    b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 2\r\n\r\n",
                )),
                ReadStep::Data(Bytes::from_static(b"a")),
                ReadStep::Wait(complete.clone()),
                ReadStep::Error(karmaio::runtime::operation_canceled()),
            ]),
        };
        let writer = Writer::new([WriteStep::Error(io::Error::other("original write failure"))]);
        let connection = Builder::new().serve_connection((input, writer), service);
        let mut driver = pin!(connection.run());
        for _ in 0..10 {
            assert!(poll(driver.as_mut()).is_pending());
            if calls.get() == 3 {
                break;
            }
        }
        assert_eq!(calls.get(), 3);
        assert!(poll(driver.as_mut()).is_pending());
        complete.open();
        let error = driver.await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Io);
        assert_eq!(
            std::error::Error::source(&error).unwrap().to_string(),
            "original write failure"
        );
        let mut body = retained.borrow_mut().take().unwrap();
        assert_eq!(body.next_frame().await.unwrap_err().kind(), ErrorKind::Canceled);
    });
}

#[test]
fn request_limits_and_partial_head_eof_fail_before_service_admission() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for (wire, expected) in [
            (
                b"GET / HTTP/1.1\r\nHost: test\r\nX: one\r\n\r\n".as_slice(),
                ErrorKind::Limit,
            ),
            (b"GET / HTTP/1.1\r\nHost: test".as_slice(), ErrorKind::InvalidMessage),
        ] {
            let calls = Cell::new(0);
            let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
                calls.set(calls.get() + 1);
                Ok::<_, Infallible>(Response::new(Empty::new()))
            });
            let mut builder = Builder::new();
            builder.head_limits(128, 1).unwrap();
            let error = builder
                .serve_connection((reader(wire), Writer::new([])), service)
                .run()
                .await
                .unwrap_err();
            assert_eq!(error.kind(), expected);
            assert_eq!(calls.get(), 0);
        }
    });
}
