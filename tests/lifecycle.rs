#![cfg(all(feature = "client", feature = "server"))]
mod support;
use bytes::Bytes;
use karmaio::{
    buf::{BufResult, IoBuf, IoVectoredBuf},
    io::AsyncWrite,
};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    future::Future,
    io,
    pin::pin,
    rc::Rc,
};
use support::future::{pair, poll};
use support::transport::{Gate, ReadStep, Reader, Writer};
use support::transport::{ObservedWriter, output};
use vakya::{
    Request, Response,
    body::{Body, Empty, Frame, Incoming, SizeHint, TrailerHint},
    client::conn::http1::Builder as Client,
    connection::ConnectionOutcome,
    error::{Error, ErrorDetail, ErrorKind, LimitKind},
    server::{RequestContext, conn::http1::Builder as Server},
    service::service_fn,
};
fn reader(bytes: &'static [u8]) -> Reader {
    Reader::new([ReadStep::Data(Bytes::from_static(bytes))])
}

#[test]
fn graceful_stops_client_admission_but_finishes_accepted_work() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (writer, wire) = ObservedWriter::new();
        let (sender, connection) =
            Client::new().handshake::<_, Empty>((reader(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n"), writer));
        let pending = sender
            .start_request(
                Request::builder()
                    .uri("/")
                    .header("host", "example")
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let control = connection.control();
        control.graceful_shutdown();
        assert!(sender.reserve().await.is_err());
        let (driver, response) = pair(connection.run(), pending.response()).await;
        assert!(matches!(driver.unwrap(), ConnectionOutcome::Closed));
        assert_eq!(response.unwrap().status(), 200);
        assert!(output(&wire).starts_with("GET / HTTP/1.1"));
    });
}

#[test]
fn abort_terminates_pending_service_and_closes_reservations() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let gate = Gate::default();
        let called = Cell::new(false);
        let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
            called.set(true);
            gate.wait().await;
            Ok::<_, Infallible>(Response::new(Empty::new()))
        });
        let connection = Server::new().serve_connection(
            (reader(b"GET / HTTP/1.1\r\nhost: example\r\n\r\n"), Writer::new([])),
            service,
        );
        let control = connection.control();
        let mut run = pin!(connection.run());
        assert!(poll(run.as_mut()).is_pending());
        assert!(called.get());
        control.abort();
        assert_eq!(run.await.unwrap_err().kind(), ErrorKind::Canceled);

        let (sender, connection) = Client::new().handshake::<_, Empty>((reader(b""), Writer::new([])));
        let permit = sender.reserve().await.unwrap();
        connection.control().abort();
        let error = permit.send(Request::new(Empty::new())).err().unwrap();
        assert_eq!(error.error().kind(), ErrorKind::Closed);
        assert_eq!(connection.run().await.unwrap_err().kind(), ErrorKind::Canceled);
    });
}

#[test]
fn graceful_deadline_escalates_while_service_waits() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
            std::future::pending::<()>().await;
            Ok::<_, Infallible>(Response::new(Empty::new()))
        });
        let connection = Server::new().serve_connection(
            (reader(b"GET / HTTP/1.1\r\nhost: example\r\n\r\n"), Writer::new([])),
            service,
        );
        let control = connection.control();
        let mut run = pin!(connection.run());
        assert!(poll(run.as_mut()).is_pending());
        control.graceful_shutdown_with_deadline(std::time::Instant::now() + std::time::Duration::from_millis(2));
        control.graceful_shutdown(); // A later unbounded request cannot relax it.
        assert_eq!(run.await.unwrap_err().kind(), ErrorKind::Timeout);
    });
}

#[test]
fn write_progress_timeout_excludes_waiting_for_application_frames() {
    struct Waiting;
    impl Body for Waiting {
        type Data = Bytes;
        type Error = Infallible;
        async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, Infallible> {
            std::future::pending().await
        }
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        let service =
            service_fn(async |_: (Request<Incoming>, RequestContext)| Ok::<_, Infallible>(Response::new(Waiting)));
        let mut builder = Server::new();
        builder
            .write_progress_timeout(Some(std::time::Duration::from_millis(2)))
            .unwrap();
        let connection = builder.serve_connection(
            (reader(b"GET / HTTP/1.1\r\nhost: example\r\n\r\n"), Writer::new([])),
            service,
        );
        let control = connection.control();
        let mut run = pin!(connection.run());
        assert!(poll(run.as_mut()).is_pending());
        karmaio::time::sleep(std::time::Duration::from_millis(6)).await;
        assert!(poll(run.as_mut()).is_pending());
        control.abort();
        assert_eq!(run.await.unwrap_err().kind(), ErrorKind::Canceled);
    });
}

#[test]
fn tcp_head_and_demanded_body_timeouts_cancel_real_reads() {
    use karmaio::net::tcp::TcpListener;
    use std::{io::Write, time::Duration};
    karmaio::Runtime::new().unwrap().block_on(async {
        for body in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
            let mut peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            if body {
                peer.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\n").unwrap();
            }
            let mut builder = Client::new();
            if body {
                builder.body_progress_timeout(Some(Duration::from_millis(2))).unwrap();
            } else {
                builder.head_timeout(Some(Duration::from_millis(2))).unwrap();
            }
            let (sender, connection) = builder.handshake::<_, Empty>(stream.into_split());
            let pending = sender
                .start_request(
                    Request::builder()
                        .uri("/")
                        .header("host", "example")
                        .body(Empty::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            let app = async {
                match pending.response().await {
                    Ok(mut response) => {
                        assert!(body);
                        // No demand means no peer-progress clock is running.
                        karmaio::time::sleep(Duration::from_millis(6)).await;
                        let error = Body::next_frame(response.body_mut()).await.unwrap_err();
                        assert_eq!(error.kind(), ErrorKind::Timeout);
                        assert_eq!(
                            error.detail(),
                            Some(vakya::error::ErrorDetail::Timeout(if body {
                                vakya::error::TimeoutPhase::BodyProgress
                            } else {
                                vakya::error::TimeoutPhase::Head
                            }))
                        );
                    }
                    Err(error) => {
                        assert!(!body);
                        assert_eq!(error.kind(), ErrorKind::Timeout);
                        assert_eq!(
                            error.detail(),
                            Some(vakya::error::ErrorDetail::Timeout(if body {
                                vakya::error::TimeoutPhase::BodyProgress
                            } else {
                                vakya::error::TimeoutPhase::Head
                            }))
                        );
                    }
                }
            };
            let (result, ()) = pair(connection.run(), app).await;
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Timeout);
        }
    });
}

#[test]
fn write_timeout_recovers_payload_before_recycling() {
    struct One {
        data: Option<Bytes>,
        recycled: Rc<Cell<usize>>,
    }
    impl Body for One {
        type Data = Bytes;
        type Error = Infallible;
        async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, Infallible> {
            Ok(self.data.take().map(Frame::data))
        }
        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(1)
        }
        fn trailer_hint(&self) -> TrailerHint {
            TrailerHint::None
        }
        fn recycle(&mut self, _: Bytes) {
            self.recycled.set(self.recycled.get() + 1);
        }
    }
    struct PayloadGate {
        gate: Gate,
        submitted: Rc<Cell<bool>>,
    }
    impl AsyncWrite for PayloadGate {
        async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
            BufResult(Ok(buffer.as_init().len()), buffer)
        }
        async fn write_vectored<B: IoVectoredBuf>(&mut self, buffers: B) -> BufResult<usize, B> {
            self.submitted.set(true);
            self.gate.wait().await;
            BufResult(Err(karmaio::runtime::operation_canceled()), buffers)
        }
        async fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        async fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        let recycled = Rc::new(Cell::new(0));
        let submitted = Rc::new(Cell::new(false));
        let gate = Gate::default();
        let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
            Ok::<_, Infallible>(Response::new(One {
                data: Some(Bytes::from_static(b"x")),
                recycled: recycled.clone(),
            }))
        });
        let mut builder = Server::new();
        builder
            .write_progress_timeout(Some(std::time::Duration::from_millis(2)))
            .unwrap();
        let connection = builder.serve_connection(
            (
                reader(b"GET / HTTP/1.1\r\nhost: example\r\n\r\n"),
                PayloadGate {
                    gate: gate.clone(),
                    submitted: submitted.clone(),
                },
            ),
            service,
        );
        let mut run = pin!(connection.run());
        assert!(poll(run.as_mut()).is_pending());
        assert!(submitted.get());
        karmaio::time::sleep(std::time::Duration::from_millis(6)).await;
        assert!(poll(run.as_mut()).is_pending());
        assert_eq!(recycled.get(), 0);
        gate.open();
        assert_eq!(run.await.unwrap_err().kind(), ErrorKind::Timeout);
        assert_eq!(recycled.get(), 1);
    });
}

#[test]
fn server_graceful_cancels_idle_read_without_calling_service() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let listener =
            karmaio::net::tcp::TcpListener::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
        let _peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
            panic!("idle shutdown admitted a request");
            #[allow(unreachable_code)]
            Ok::<_, Infallible>(Response::new(Empty::new()))
        });
        let connection = Server::new().serve_tcp(stream, service);
        let control = connection.control();
        let mut run = pin!(connection.run());
        assert!(poll(run.as_mut()).is_pending());
        control.graceful_shutdown();
        assert!(matches!(run.await.unwrap(), ConnectionOutcome::Closed));
    });
}

#[test]
fn head_deadline_does_not_reset_on_incremental_progress() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let gate = Gate::default();
        let mut builder = Server::new();
        builder.head_timeout(Some(std::time::Duration::from_millis(2))).unwrap();
        let service =
            service_fn(async |_: (Request<Incoming>, RequestContext)| Ok::<_, Infallible>(Response::new(Empty::new())));
        let reader = Reader::new([
            ReadStep::Data(Bytes::from_static(b"GET / HTTP/1.1\r\n")),
            ReadStep::Wait(gate.clone()),
            ReadStep::Data(Bytes::from_static(b"host: example\r\n\r\n")),
        ]);
        let connection = builder.serve_connection((reader, Writer::new([])), service);
        let mut run = pin!(connection.run());
        assert!(poll(run.as_mut()).is_pending());
        karmaio::time::sleep(std::time::Duration::from_millis(6)).await;
        assert!(poll(run.as_mut()).is_pending()); // Deadline waits for the retained read.
        gate.open();
        assert_eq!(run.await.unwrap_err().kind(), ErrorKind::Timeout);
    });
}

#[test]
fn drain_deadline_abandons_and_settles_a_stalled_tcp_body() {
    use std::io::Write;
    karmaio::Runtime::new().unwrap().block_on(async {
        let listener =
            karmaio::net::tcp::TcpListener::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
        let mut peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        peer.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\n").unwrap();
        let mut builder = Client::new();
        builder.drain_timeout(std::time::Duration::from_millis(10)).unwrap();
        let (sender, connection) = builder.handshake_tcp::<Empty>(stream);
        let pending = sender
            .start_request(
                Request::builder()
                    .uri("/")
                    .header("host", "example")
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let app = async {
            let response = pending.response().await.unwrap();
            assert_eq!(
                response.into_body().drain(1).await.unwrap_err().kind(),
                ErrorKind::Timeout
            );
        };
        let (result, ()) = pair(connection.run(), app).await;
        assert!(matches!(result.unwrap(), ConnectionOutcome::Closed));
        assert!(sender.reserve().await.is_err());
    });
}

struct ShutdownGate {
    calls: Rc<Cell<usize>>,
    complete: Gate,
    failure: bool,
}
impl AsyncWrite for ShutdownGate {
    async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
        BufResult(Ok(buffer.as_init().len()), buffer)
    }
    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        self.calls.set(self.calls.get() + 1);
        self.complete.wait().await;
        if self.failure {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "shutdown failed"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn normal_server_closure_settles_idle_read_then_shuts_down_once() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for mode in ["before", "active", "idle", "eof", "close"] {
            let read_done = Gate::default();
            let complete = Gate::default();
            let calls = Rc::new(Cell::new(0));
            let slot = Rc::new(RefCell::new(None::<vakya::connection::ConnectionControl>));
            let service_slot = slot.clone();
            let input = match mode {
                "idle" => Reader::new([
                    ReadStep::Wait(read_done.clone()),
                    ReadStep::Error(karmaio::runtime::operation_canceled()),
                ]),
                "close" => reader(b"GET / HTTP/1.1\r\nhost: example\r\nconnection: close\r\n\r\n"),
                "active" => reader(b"GET / HTTP/1.1\r\nhost: example\r\n\r\n"),
                _ => Reader::new([]),
            };
            let service = service_fn(async move |_: (Request<Incoming>, RequestContext)| {
                if mode == "active" {
                    service_slot.borrow().as_ref().unwrap().graceful_shutdown();
                }
                Ok::<_, Infallible>(Response::new(Empty::new()))
            });
            let connection = Server::new().serve_connection(
                (
                    input,
                    ShutdownGate {
                        calls: calls.clone(),
                        complete: complete.clone(),
                        failure: false,
                    },
                ),
                service,
            );
            let control = connection.control();
            *slot.borrow_mut() = Some(control.clone());
            if mode == "before" {
                control.graceful_shutdown();
            }
            let mut run = pin!(connection.run());
            assert!(poll(run.as_mut()).is_pending(), "{mode}");
            if mode == "idle" {
                control.graceful_shutdown();
                assert!(poll(run.as_mut()).is_pending());
                assert_eq!(calls.get(), 0, "shutdown preceded read settlement");
                read_done.open();
                assert!(poll(run.as_mut()).is_pending());
            }
            assert_eq!(calls.get(), 1, "{mode}");
            assert!(poll(run.as_mut()).is_pending());
            assert_eq!(calls.get(), 1);
            complete.open();
            assert!(matches!(run.await.unwrap(), ConnectionOutcome::Closed));
        }
    });
}

#[test]
fn abort_and_deadline_settle_shutdown_and_preserve_original_failure() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for deadline in [false, true] {
            for failure in [false, true] {
                let complete = Gate::default();
                let calls = Rc::new(Cell::new(0));
                let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
                    Ok::<_, Infallible>(Response::new(Empty::new()))
                });
                let connection = Server::new().serve_connection(
                    (
                        Reader::new([]),
                        ShutdownGate {
                            calls: calls.clone(),
                            complete: complete.clone(),
                            failure,
                        },
                    ),
                    service,
                );
                let control = connection.control();
                let mut run = pin!(connection.run());
                assert!(poll(run.as_mut()).is_pending());
                assert_eq!(calls.get(), 1);
                if deadline {
                    control.graceful_shutdown_with_deadline(std::time::Instant::now());
                    assert!(poll(run.as_mut()).is_pending());
                    karmaio::time::sleep(std::time::Duration::from_millis(2)).await;
                } else {
                    control.abort();
                }
                assert!(poll(run.as_mut()).is_pending(), "shutdown must settle");
                complete.open();
                let error = run.await.unwrap_err();
                assert_eq!(
                    error.kind(),
                    if failure {
                        ErrorKind::Io
                    } else if deadline {
                        ErrorKind::Timeout
                    } else {
                        ErrorKind::Canceled
                    }
                );
                if failure {
                    use std::error::Error as _;
                    assert_eq!(
                        error.source().unwrap().downcast_ref::<io::Error>().unwrap().kind(),
                        io::ErrorKind::BrokenPipe
                    );
                }
                assert_eq!(calls.get(), 1);
            }
        }
    });
}

#[test]
fn both_roles_flush_heads_and_streamed_data_before_production_finishes() {
    struct Buffered {
        staged: Vec<u8>,
        visible: Rc<RefCell<Vec<u8>>>,
    }
    impl AsyncWrite for Buffered {
        async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
            self.staged.extend_from_slice(buffer.as_init());
            BufResult(Ok(buffer.as_init().len()), buffer)
        }
        async fn write_vectored<B: IoVectoredBuf>(&mut self, buffers: B) -> BufResult<usize, B> {
            let mut count = 0;
            for bytes in buffers.iter_slice() {
                self.staged.extend_from_slice(bytes);
                count += bytes.len();
            }
            BufResult(Ok(count), buffers)
        }
        async fn flush(&mut self) -> io::Result<()> {
            self.visible.borrow_mut().extend(std::mem::take(&mut self.staged));
            Ok(())
        }
        async fn shutdown(&mut self) -> io::Result<()> {
            self.flush().await
        }
    }
    struct Waiting {
        produce: Gate,
        sent: bool,
    }
    impl Body for Waiting {
        type Data = Bytes;
        type Error = Infallible;
        async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, Infallible> {
            if self.sent {
                return std::future::pending().await;
            }
            self.produce.wait().await;
            self.sent = true;
            Ok(Some(Frame::data(Bytes::from_static(b"payload"))))
        }
        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(if self.sent { 0 } else { 7 })
        }
        fn trailer_hint(&self) -> TrailerHint {
            TrailerHint::None
        }
    }
    async fn observe<F>(
        run: F,
        control: vakya::connection::ConnectionControl,
        visible: Rc<RefCell<Vec<u8>>>,
        produce: Gate,
    ) where
        F: Future<Output = Result<ConnectionOutcome<Reader, Buffered>, Error>>,
    {
        let mut run = pin!(run);
        assert!(poll(run.as_mut()).is_pending());
        let head = visible.borrow().clone();
        assert!(head.ends_with(b"\r\n\r\n"));
        assert!(
            String::from_utf8(head.clone())
                .unwrap()
                .contains("content-length: 7\r\n")
        );
        produce.open();
        assert!(poll(run.as_mut()).is_pending());
        assert_eq!(&visible.borrow()[head.len()..], b"payload");
        control.abort();
        assert_eq!(run.await.unwrap_err().kind(), ErrorKind::Canceled);
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        let visible = Rc::new(RefCell::new(Vec::new()));
        let produce = Gate::default();
        let (sender, connection) = Client::new().handshake((
            reader(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n"),
            Buffered {
                staged: Vec::new(),
                visible: visible.clone(),
            },
        ));
        let pending = sender
            .start_request(
                Request::builder()
                    .uri("/")
                    .header("host", "example")
                    .body(Waiting {
                        produce: produce.clone(),
                        sent: false,
                    })
                    .unwrap(),
            )
            .await
            .unwrap();
        let control = connection.control();
        observe(connection.run(), control, visible, produce).await;
        drop(pending);

        let visible = Rc::new(RefCell::new(Vec::new()));
        let produce = Gate::default();
        let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
            Ok::<_, Infallible>(Response::new(Waiting {
                produce: produce.clone(),
                sent: false,
            }))
        });
        let connection = Server::new().serve_connection(
            (
                reader(b"GET / HTTP/1.1\r\nhost: example\r\n\r\n"),
                Buffered {
                    staged: Vec::new(),
                    visible: visible.clone(),
                },
            ),
            service,
        );
        let control = connection.control();
        observe(connection.run(), control, visible, produce.clone()).await;
    });
}

#[test]
fn draining_enforces_payload_and_wire_budgets_in_both_roles() {
    fn check<T>(result: Result<T, Error>, expected: Option<LimitKind>) {
        match (result, expected) {
            (Ok(_), None) => {}
            (Err(error), Some(limit)) => {
                assert_eq!(error.kind(), ErrorKind::Limit);
                assert_eq!(error.detail(), Some(ErrorDetail::IncomingLimit(limit)));
            }
            (Err(error), None) => panic!("unexpected drain failure: {error}"),
            (Ok(_), Some(limit)) => panic!("missing {limit:?} failure"),
        }
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        for (wire, payload_limit, allowance, expected) in [
            (b"4\r\nbody\r\n0\r\nx-end: yes\r\n\r\n".as_slice(), 4, 64, None),
            (b"4\r\nbody\r\n0\r\n\r\n", 3, 64, Some(LimitKind::DrainPayload)),
            (b"1\r\nx\r\n0\r\n\r\n", 1, 0, Some(LimitKind::DrainWire)),
            (b"1\r\nx\r\n0\r\n\r\n", 1, 64, None),
        ] {
            let input = [
                b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n".as_slice(),
                wire,
            ]
            .concat();
            let mut client = Client::new();
            client.drain_wire_allowance(allowance);
            let (sender, connection) =
                client.handshake::<_, Empty>((Reader::new([ReadStep::Data(Bytes::from(input))]), Writer::new([])));
            let pending = sender
                .start_request(
                    Request::builder()
                        .uri("/")
                        .header("host", "example")
                        .body(Empty::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            let app = async {
                check(
                    pending.response().await.unwrap().into_body().drain(payload_limit).await,
                    expected,
                );
            };
            let (result, ()) = pair(connection.run(), app).await;
            check(result, expected);

            // Observe draining outside the service: receive failure may cancel
            // the service before its drain wait observes the producer error.
            let retained = RefCell::new(None);
            let admitted = Gate::default();
            let service = service_fn(async |(request, _): (Request<Incoming>, RequestContext)| {
                retained.replace(Some(request.into_body()));
                admitted.open();
                Ok::<_, Infallible>(Response::new(Empty::new()))
            });
            let input = [
                b"POST / HTTP/1.1\r\nhost: example\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n"
                    .as_slice(),
                wire,
            ]
            .concat();
            let mut server = Server::new();
            server.drain_wire_allowance(allowance);
            let connection = server.serve_connection(
                (Reader::new([ReadStep::Data(Bytes::from(input))]), Writer::new([])),
                service,
            );
            let app = async {
                admitted.wait().await;
                let body = retained.borrow_mut().take().unwrap();
                check(body.drain(payload_limit).await, expected);
            };
            let (result, ()) = pair(connection.run(), app).await;
            check(result, expected);
        }
    });
}

#[test]
fn asymmetric_head_limits_are_enforced_in_both_roles() {
    karmaio::Runtime::new().unwrap().block_on(async {
        // Permit a large outgoing head with a small incoming budget, then
        // independently reject each direction without changing the other.
        for (incoming, outgoing, peer_large, success) in [
            (128, 1024, false, true),
            (128, 1024, true, false),
            (1024, 128, false, false),
        ] {
            let padding = "a".repeat(400);
            let peer_header = if peer_large {
                format!("x-peer: {padding}\r\n")
            } else {
                String::new()
            };
            let response = format!("HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n{peer_header}\r\n");
            let mut client = Client::new();
            client
                .incoming_head_limits(incoming, 10)
                .unwrap()
                .outgoing_head_limit(outgoing)
                .unwrap();
            let (sender, connection) =
                client.handshake::<_, Empty>((Reader::new([ReadStep::Data(Bytes::from(response))]), Writer::new([])));
            let app = sender.send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "example")
                    .header("x-local", &padding)
                    .body(Empty::new())
                    .unwrap(),
            );
            let (driver, response) = pair(connection.run(), app).await;
            let expected = if peer_large {
                ErrorDetail::IncomingLimit(LimitKind::HeadBytes)
            } else {
                ErrorDetail::OutgoingLimit(LimitKind::HeadBytes)
            };
            if success {
                driver.unwrap();
                response.unwrap();
            } else {
                let error = driver.unwrap_err();
                assert_eq!(error.kind(), ErrorKind::Limit);
                assert_eq!(error.detail(), Some(expected));
                let error = response.unwrap_err();
                assert_eq!(error.error().kind(), ErrorKind::Limit);
                assert_eq!(error.error().detail(), Some(expected));
            }

            let request = format!("GET / HTTP/1.1\r\nhost: example\r\nconnection: close\r\n{peer_header}\r\n");
            let mut server = Server::new();
            server
                .incoming_head_limits(incoming, 10)
                .unwrap()
                .outgoing_head_limit(outgoing)
                .unwrap();
            let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
                Ok::<_, Infallible>(
                    Response::builder()
                        .header("x-local", &padding)
                        .body(Empty::new())
                        .unwrap(),
                )
            });
            let result = server
                .serve_connection(
                    (Reader::new([ReadStep::Data(Bytes::from(request))]), Writer::new([])),
                    service,
                )
                .run()
                .await;
            if success {
                result.unwrap();
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.kind(), ErrorKind::Limit);
                assert_eq!(error.detail(), Some(expected));
            }
        }
    });
}
