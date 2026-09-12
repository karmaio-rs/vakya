#![cfg(feature = "client")]
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
use support::transport::{Gate, ReadStep, Reader, Writer};
use vakya::{
    Body, BodyExt, Empty, ErrorKind, Frame, Full, Request, SizeHint, TrailerHint, client::conn::http1::Builder,
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

fn request<B>(body: B) -> Request<B> {
    Request::builder()
        .method("POST")
        .uri("/upload")
        .header("host", "test")
        .body(body)
        .unwrap()
}

#[test]
fn unsolicited_read_ahead_never_becomes_a_later_response() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for framing in [
            "Content-Length: 0\r\n\r\n",
            "Content-Length: 1\r\n\r\nx",
            "Transfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n",
        ] {
            let wire = Bytes::from(format!(
                "HTTP/1.1 200 OK\r\n{framing}HTTP/1.1 201 Created\r\nContent-Length: 6\r\n\r\nPOISON"
            ));
            let (writer, output) = ObservedWriter::new();
            let (sender, driver) = Builder::new().handshake((Reader::new([ReadStep::Data(wire)]), writer));
            let app = async {
                let response = sender.send_request(request(Empty::new())).await.unwrap();
                assert_eq!(response.status(), 200);
                response.into_body().collect(1).await.unwrap();
                assert!(sender.send_request(request(Empty::new())).await.is_err());
            };
            let (result, ()) = pair(driver.run(), app).await;
            assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidMessage);
            assert_eq!(
                String::from_utf8(output.borrow().clone())
                    .unwrap()
                    .matches("POST /upload")
                    .count(),
                1
            );
        }
    });
}

#[test]
fn reservations_are_bounded_cancel_safe_and_recover_unsubmitted_requests() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (sender, driver) = Builder::new().handshake::<_, Full<Bytes>>((reader(b""), Writer::limited(8)));
        let permit = sender.reserve().await.unwrap();
        {
            let mut waiting = pin!(sender.reserve());
            assert!(poll(waiting.as_mut()).is_pending());
            let error = sender
                .start_request(request(Full::new(Bytes::from_static(b"unchanged"))))
                .await
                .err()
                .unwrap();
            let (request, error) = error.into_parts();
            assert_eq!(error.kind(), ErrorKind::Limit);
            assert_eq!(
                request.into_body().collect(20).await.unwrap().bytes().as_ref(),
                b"unchanged"
            );
        }
        let mut waiting = pin!(sender.reserve());
        assert!(poll(waiting.as_mut()).is_pending());
        drop(permit);
        // Releasing admission wakes and reserves priority for the registered
        // waiter, even before that future is polled again.
        assert_eq!(sender.reserve().await.err().unwrap().kind(), ErrorKind::Limit);
        let permit = waiting.await.unwrap();
        drop(driver);
        let error = permit
            .send(request(Full::new(Bytes::from_static(b"still mine"))))
            .err()
            .unwrap();
        assert_eq!(error.error().kind(), ErrorKind::Closed);
        assert_eq!(
            error
                .into_parts()
                .0
                .into_body()
                .collect(20)
                .await
                .unwrap()
                .bytes()
                .as_ref(),
            b"still mine"
        );
        assert_eq!(sender.reserve().await.err().unwrap().kind(), ErrorKind::Closed);
        let error = sender
            .send_request(request(Full::new(Bytes::from_static(b"recover convenience"))))
            .await
            .unwrap_err();
        let vakya::client::SendError::Submission(error) = error else {
            panic!("request ownership was never transferred");
        };
        assert_eq!(
            error
                .into_parts()
                .0
                .into_body()
                .collect(32)
                .await
                .unwrap()
                .bytes()
                .as_ref(),
            b"recover convenience"
        );
    });
}

struct Delayed<'a> {
    gate: Gate,
    produced: &'a Cell<bool>,
    recycled: &'a Cell<usize>,
}
impl Body for Delayed<'_> {
    type Data = Bytes;
    type Error = Infallible;
    async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, Infallible> {
        if self.produced.get() {
            return Ok(None);
        }
        self.gate.wait().await;
        self.produced.set(true);
        Ok(Some(Frame::data(Bytes::from_static(b"upload"))))
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(if self.produced.get() { 0 } else { 6 })
    }
    fn trailer_hint(&self) -> TrailerHint {
        TrailerHint::None
    }
    fn recycle(&mut self, _: Bytes) {
        self.recycled.set(self.recycled.get() + 1);
    }
}

#[test]
fn early_finals_preserve_borrowed_uploads_and_reuse_waits_for_both_directions() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for status in [200, 400] {
            let gate = Gate::default();
            let produced = Cell::new(false);
            let recycled = Cell::new(0);
            let wire = Bytes::from(format!("HTTP/1.1 {status} Response\r\nContent-Length: 2\r\n\r\nok"));
            let next_response = Gate::default();
            let (writer, bytes) = ObservedWriter::new();
            let (sender, driver) = Builder::new().handshake((
                Reader::new([
                    ReadStep::Data(wire),
                    ReadStep::Wait(next_response.clone()),
                    ReadStep::Data(Bytes::from_static(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )),
                ]),
                writer,
            ));
            let pending = sender
                .start_request(request(Delayed {
                    gate: gate.clone(),
                    produced: &produced,
                    recycled: &recycled,
                }))
                .await
                .unwrap();
            let mut driver = pin!(driver.run());
            let mut response = pin!(pending.response());
            assert!(poll(driver.as_mut()).is_pending());
            let Poll::Ready(Ok(response)) = poll(response.as_mut()) else {
                panic!("early head missing")
            };
            assert_eq!(response.status().as_u16(), status);
            assert!(!produced.get());
            let mut reserve = pin!(sender.reserve());
            assert!(poll(reserve.as_mut()).is_pending());
            let mut collect = pin!(response.into_body().collect(8));
            assert!(poll(collect.as_mut()).is_pending());
            assert!(poll(driver.as_mut()).is_pending());
            let mut collected = None;
            for _ in 0..10 {
                assert!(poll(driver.as_mut()).is_pending());
                if let Poll::Ready(result) = poll(collect.as_mut()) {
                    collected = Some(result.unwrap());
                    break;
                }
            }
            assert_eq!(collected.expect("body completed").bytes().as_ref(), b"ok");
            assert!(poll(driver.as_mut()).is_pending());
            assert!(poll(reserve.as_mut()).is_pending());
            gate.open();
            assert!(poll(driver.as_mut()).is_pending());
            assert!(produced.get());
            assert_eq!(recycled.get(), 1);
            let permit = reserve.await.unwrap();
            let pending = permit
                .send(request(Delayed {
                    gate,
                    produced: &produced,
                    recycled: &recycled,
                }))
                .unwrap();
            assert!(poll(driver.as_mut()).is_pending());
            next_response.open();
            let (result, response) = pair(driver, pending.response()).await;
            result.unwrap();
            response.unwrap();
            assert!(output(&bytes).contains("\r\n\r\nupload"));
        }
    });
}

#[test]
fn explicit_upload_abort_after_final_delivery_retires_connection() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let produced = Cell::new(false);
        let recycled = Cell::new(0);
        let (sender, driver) = Builder::new().handshake((
            reader(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"),
            Writer::limited(128),
        ));
        let pending = sender
            .start_request(request(Delayed {
                gate: Gate::default(),
                produced: &produced,
                recycled: &recycled,
            }))
            .await
            .unwrap();
        let control = pending.control();
        let mut driver = pin!(driver.run());
        let mut response = pin!(pending.response());
        assert!(poll(driver.as_mut()).is_pending());
        assert!(matches!(poll(response.as_mut()), Poll::Ready(Ok(_))));
        control.abort();
        driver.await.unwrap();
        assert!(!produced.get());
        assert_eq!(sender.reserve().await.err().unwrap().kind(), ErrorKind::Closed);
    });
}

#[test]
fn waiter_and_incoming_abandonment_retire_without_waiting_for_producer() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for before in [true, false] {
            let produced = Cell::new(false);
            let recycled = Cell::new(0);
            let (sender, driver) = Builder::new().handshake((
                reader(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"),
                Writer::limited(128),
            ));
            let pending = sender
                .start_request(request(Delayed {
                    gate: Gate::default(),
                    produced: &produced,
                    recycled: &recycled,
                }))
                .await
                .unwrap();
            let mut driver = pin!(driver.run());
            assert!(poll(driver.as_mut()).is_pending());
            if before {
                drop(pending);
                assert_eq!(driver.await.unwrap_err().kind(), ErrorKind::Canceled);
            } else {
                let response = pending.response().await.unwrap();
                drop(response);
                driver.await.unwrap();
            }
            assert!(!produced.get());
            assert_eq!(sender.reserve().await.err().unwrap().kind(), ErrorKind::Closed);
        }
    });
}

#[test]
fn last_sender_drop_finishes_accepted_work_and_dropped_permits_wake_idle_driver() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (sender, driver) = Builder::new().handshake((
            reader(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"),
            Writer::limited(128),
        ));
        let pending = sender.start_request(request(Empty::new())).await.unwrap();
        drop(sender);
        let (result, response) = pair(driver.run(), pending.response()).await;
        result.unwrap();
        response.unwrap();
        let (sender, driver) = Builder::new().handshake::<_, Empty>((reader(b""), Writer::limited(128)));
        let permit = sender.reserve().await.unwrap();
        drop(sender);
        let mut driver = pin!(driver.run());
        assert!(poll(driver.as_mut()).is_pending());
        drop(permit);
        driver.await.unwrap();
    });
}

#[test]
fn encoding_peer_and_transport_failures_reach_driver_and_waiter() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for case in 0..3 {
            let reader = if case == 2 {
                Reader::new([ReadStep::Error(io::Error::other("original read failure"))])
            } else {
                reader(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n")
            };
            let (sender, driver) = Builder::new().handshake((reader, Writer::limited(128)));
            let mut request = request(Empty::new());
            if case == 0 {
                request.headers_mut().insert("content-length", "5".parse().unwrap());
            }
            let pending = sender.start_request(request).await.unwrap();
            let (driver, response) = pair(driver.run(), pending.response()).await;
            let expected = [ErrorKind::LocalMessage, ErrorKind::InvalidMessage, ErrorKind::Io][case];
            assert_eq!(driver.unwrap_err().kind(), expected);
            let error = response.unwrap_err();
            assert_eq!(error.kind(), expected);
            if case == 2 {
                assert_eq!(
                    std::error::Error::source(&error)
                        .unwrap()
                        .downcast_ref::<io::Error>()
                        .unwrap()
                        .to_string(),
                    "original read failure"
                );
            }
        }
    });
}

#[test]
fn send_failure_settles_submitted_read_and_preserves_its_source() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let complete = Gate::default();
        let reader = Reader::new([
            ReadStep::Wait(complete.clone()),
            ReadStep::Error(karmaio::runtime::operation_canceled()),
        ]);
        let writer = Writer::new([support::transport::WriteStep::Error(io::Error::other(
            "original write failure",
        ))]);
        let (sender, driver) = Builder::new().handshake((reader, writer));
        let pending = sender.start_request(request(Empty::new())).await.unwrap();
        let mut driver = pin!(driver.run());
        assert!(poll(driver.as_mut()).is_pending());
        let mut response = pin!(pending.response());
        assert!(poll(response.as_mut()).is_pending());
        complete.open();
        let (driver, response) = pair(driver, response).await;
        for error in [driver.unwrap_err(), response.unwrap_err()] {
            assert_eq!(error.kind(), ErrorKind::Io);
            assert_eq!(
                std::error::Error::source(&error)
                    .unwrap()
                    .downcast_ref::<io::Error>()
                    .unwrap()
                    .to_string(),
                "original write failure"
            );
        }
    });
}

#[cfg(feature = "server")]
#[test]
fn supplied_tcp_duplex_echo_keeps_upload_alive_after_final_response() {
    karmaio::Runtime::new().unwrap().block_on(async {
        karmaio::time::timeout(std::time::Duration::from_secs(5), async {
            let listener =
                karmaio::net::tcp::TcpListener::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
            let (connected, accepted) = pair(
                karmaio::net::tcp::TcpStream::connect(listener.local_addr().unwrap()),
                listener.accept(),
            )
            .await;
            let client = connected.unwrap();
            let (server, _) = accepted.unwrap();
            let service = vakya::service_fn(
                async |(request, context): (Request<vakya::Incoming>, vakya::server::RequestContext)| {
                    drop(context);
                    Ok::<_, Infallible>(vakya::Response::new(request.into_body()))
                },
            );
            let server = vakya::server::conn::http1::Builder::new().serve_tcp(server, service);
            let gate = Gate::default();
            let produced = Cell::new(false);
            let recycled = Cell::new(0);
            let (sender, driver) = Builder::new().handshake_tcp(client);
            let application = async {
                let mut trailers = vakya::HeaderMap::new();
                trailers.insert("x-upload", "complete".parse().unwrap());
                let mut request = request(
                    Delayed {
                        gate: gate.clone(),
                        produced: &produced,
                        recycled: &recycled,
                    }
                    .with_trailers(trailers),
                );
                request.headers_mut().insert("connection", "close".parse().unwrap());
                let response = sender.send_request(request).await.unwrap();
                assert_eq!(response.status(), 200);
                assert!(!produced.get());
                gate.open();
                let collected = response.into_body().collect(16).await.unwrap();
                assert_eq!(collected.bytes().as_ref(), b"upload");
                assert_eq!(collected.trailers().unwrap()["x-upload"], "complete");
            };
            let ((client, server), ()) = pair(pair(driver.run(), server.run()), application).await;
            client.unwrap();
            server.unwrap();
            assert_eq!(recycled.get(), 1);
        })
        .await
        .expect("duplex exchange timed out");
    });
}

#[test]
fn upload_abort_racing_successful_flush_still_retires_connection() {
    struct FlushGate {
        writer: Writer,
        gate: Gate,
        submitted: Rc<Cell<bool>>,
    }
    impl AsyncWrite for FlushGate {
        async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
            self.writer.write(buffer).await
        }
        async fn write_vectored<V: IoVectoredBuf>(&mut self, buffers: V) -> BufResult<usize, V> {
            self.writer.write_vectored(buffers).await
        }
        async fn flush(&mut self) -> io::Result<()> {
            self.submitted.set(true);
            self.gate.wait().await;
            // Model an operation that completes successfully as cancellation
            // races with its completion. Requesting cancellation is not its result.
            Ok(())
        }
        async fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        let gate = Gate::default();
        let submitted = Rc::new(Cell::new(false));
        let writer = FlushGate {
            writer: Writer::limited(128),
            gate: gate.clone(),
            submitted: submitted.clone(),
        };
        let (sender, driver) =
            Builder::new().handshake((reader(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"), writer));
        let pending = sender.start_request(request(Empty::new())).await.unwrap();
        let control = pending.control();
        let mut driver = pin!(driver.run());
        assert!(poll(driver.as_mut()).is_pending());
        assert!(submitted.get());
        let response = pending.response().await.unwrap();
        drop(response);
        assert!(poll(driver.as_mut()).is_pending());
        control.abort();
        assert!(poll(driver.as_mut()).is_pending());
        gate.open();
        driver.await.unwrap();
        assert_eq!(sender.reserve().await.err().unwrap().kind(), ErrorKind::Closed);
    });
}
