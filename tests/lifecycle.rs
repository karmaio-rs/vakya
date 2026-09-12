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
    future::{Future, poll_fn},
    io,
    pin::{Pin, pin},
    rc::Rc,
    task::{Context, Poll, Waker},
};
use support::transport::{Gate, ReadStep, Reader, Writer};
use vakya::{
    Empty, ErrorKind, Incoming, Request, Response,
    client::conn::http1::Builder as Client,
    connection::ConnectionOutcome,
    server::{RequestContext, conn::http1::Builder as Server},
    service_fn,
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
fn abort_drops_pending_service_and_closes_reservations() {
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
    impl vakya::Body for Waiting {
        type Data = Bytes;
        type Error = Infallible;
        async fn next_frame(&mut self) -> Result<Option<vakya::Frame<Bytes>>, Infallible> {
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
                        let error = vakya::Body::next_frame(response.body_mut()).await.unwrap_err();
                        assert_eq!(error.kind(), ErrorKind::Timeout);
                    }
                    Err(error) => {
                        assert!(!body);
                        assert_eq!(error.kind(), ErrorKind::Timeout);
                    }
                }
            };
            let (result, ()) = pair(connection.run(), app).await;
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Timeout);
        }
    });
}

#[test]
fn draining_enforces_payload_and_framing_budgets() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for (wire, limit, success) in [
            (b"4\r\nbody\r\n0\r\nx-end: yes\r\n\r\n".to_vec(), 4, true),
            (b"4\r\nbody\r\n0\r\n\r\n".to_vec(), 3, false),
            // Valid tiny chunks spend substantially more framing than payload.
            (
                [b"1;".as_slice(), &vec![b'a'; 8000], b"\r\nx\r\n"].concat().repeat(9),
                9,
                false,
            ),
        ] {
            let input = [
                b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n".as_slice(),
                &wire,
            ]
            .concat();
            let (sender, connection) = Client::new()
                .handshake::<_, Empty>((Reader::new([ReadStep::Data(Bytes::from(input))]), Writer::new([])));
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
                let result = response.into_body().drain(limit).await;
                if success {
                    result.unwrap();
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::Limit);
                }
            };
            let (driver, ()) = pair(connection.run(), app).await;
            if success {
                driver.unwrap();
            }
        }
    });
}

#[test]
fn write_timeout_recovers_payload_before_recycling() {
    struct One {
        data: Option<Bytes>,
        recycled: Rc<Cell<usize>>,
    }
    impl vakya::Body for One {
        type Data = Bytes;
        type Error = Infallible;
        async fn next_frame(&mut self) -> Result<Option<vakya::Frame<Bytes>>, Infallible> {
            Ok(self.data.take().map(vakya::Frame::data))
        }
        fn size_hint(&self) -> vakya::SizeHint {
            vakya::SizeHint::with_exact(1)
        }
        fn trailer_hint(&self) -> vakya::TrailerHint {
            vakya::TrailerHint::None
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
    impl vakya::Body for Waiting {
        type Data = Bytes;
        type Error = Infallible;
        async fn next_frame(&mut self) -> Result<Option<vakya::Frame<Bytes>>, Infallible> {
            if self.sent {
                return std::future::pending().await;
            }
            self.produce.wait().await;
            self.sent = true;
            Ok(Some(vakya::Frame::data(Bytes::from_static(b"payload"))))
        }
        fn size_hint(&self) -> vakya::SizeHint {
            vakya::SizeHint::with_exact(if self.sent { 0 } else { 7 })
        }
        fn trailer_hint(&self) -> vakya::TrailerHint {
            vakya::TrailerHint::None
        }
    }
    async fn observe<F>(
        run: F,
        control: vakya::connection::ConnectionControl,
        visible: Rc<RefCell<Vec<u8>>>,
        produce: Gate,
    ) where
        F: Future<Output = Result<ConnectionOutcome<Reader, Buffered>, vakya::Error>>,
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
fn configured_drain_wire_allowance_reaches_both_roles() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for allowance in [0, 64] {
            let wire = b"1\r\nx\r\n0\r\n\r\n";
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
                let result = pending.response().await.unwrap().into_body().drain(1).await;
                if allowance == 0 {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::Limit);
                } else {
                    result.unwrap();
                }
            };
            let (result, ()) = pair(connection.run(), app).await;
            if allowance != 0 {
                result.unwrap();
            }

            let observed = Cell::new(false);
            let service = service_fn(async |(request, _): (Request<Incoming>, RequestContext)| {
                observed.set(true);
                let result = request.into_body().drain(1).await;
                if allowance == 0 {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::Limit);
                } else {
                    result.unwrap();
                }
                observed.set(true);
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
            let result = server
                .serve_connection(
                    (Reader::new([ReadStep::Data(Bytes::from(input))]), Writer::new([])),
                    service,
                )
                .run()
                .await;
            if allowance != 0 {
                result.unwrap();
            } else {
                assert_eq!(result.unwrap_err().kind(), ErrorKind::Limit);
            }
            assert!(observed.get());
        }
    });
}
