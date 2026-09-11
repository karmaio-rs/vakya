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
    time::Duration,
};
use support::transport::{Gate, ReadStep, Reader, Writer};
use vakya::{
    Body, BodyExt, Empty, ErrorKind, Frame, Full, Incoming, Request, Response, SizeHint, TrailerHint,
    client::{ResponseEvent, conn::http1::Builder as Client},
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

fn info(status: u16) -> Response<()> {
    Response::builder().status(status).body(()).unwrap()
}
fn upload() -> Request<Full<Bytes>> {
    Request::builder()
        .method("POST")
        .uri("/")
        .header("host", "test")
        .header("expect", "100-continue")
        .body(Full::new(Bytes::from_static(b"upload")))
        .unwrap()
}

#[test]
fn server_informationals_are_acknowledged_after_write_and_enforce_limits() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (writer, bytes) = ObservedWriter::new();
        let service = service_fn(async |(_, mut context): (Request<Incoming>, RequestContext)| {
            for status in [100, 101, 200] {
                assert_eq!(
                    context.send_informational(info(status)).await.unwrap_err().kind(),
                    ErrorKind::LocalMessage
                );
            }
            let mut invalid = info(103);
            invalid.headers_mut().insert("content-length", "1".parse().unwrap());
            assert_eq!(
                context.send_informational(invalid).await.unwrap_err().kind(),
                ErrorKind::LocalMessage
            );
            context.send_informational(info(103)).await.unwrap();
            assert!(output(&bytes).ends_with("\r\n\r\n"));
            assert!(output(&bytes).contains("103 Early Hints"));
            assert_eq!(
                context.send_informational(info(102)).await.unwrap_err().kind(),
                ErrorKind::Limit
            );
            Ok::<_, Infallible>(Response::new(Empty::new()))
        });
        Server::new()
            .max_informational(1)
            .auto_date(false)
            .serve_connection(
                (
                    reader(b"GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"),
                    writer,
                ),
                service,
            )
            .run()
            .await
            .unwrap();
        let wire = output(&bytes);
        assert_eq!(wire.matches("HTTP/1.1").count(), 2);
        assert!(wire.find("103").unwrap() < wire.find("200").unwrap());
    });
}

#[test]
fn continue_and_final_release_upload_before_observation() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for first in [100, 200, 400] {
            let wire = if first == 100 {
                "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string()
            } else {
                format!("HTTP/1.1 {first} Response\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            };
            let (writer, bytes) = ObservedWriter::new();
            let (sender, driver) = Client::new()
                .continue_wait(Some(Duration::from_secs(30)))
                .unwrap()
                .handshake((Reader::new([ReadStep::Data(Bytes::from(wire))]), writer));
            let mut pending = sender.start_request(upload()).await.unwrap();
            let mut driver = pin!(driver.run());
            for _ in 0..10 {
                assert!(poll(driver.as_mut()).is_pending());
                if output(&bytes).ends_with("upload") {
                    break;
                }
            }
            assert!(output(&bytes).ends_with("upload"));
            // Consume the first event; final delivery terminates observation.
            {
                let mut next = pin!(pending.next_event());
                let event = poll(next.as_mut());
                assert!(matches!(event, Poll::Ready(Ok(Some(_)))));
            }
            if first == 100 {
                let (result, response) = pair(driver, pending.response()).await;
                result.unwrap();
                assert_eq!(response.unwrap().status(), 200);
            } else {
                assert!(pending.next_event().await.unwrap().is_none());
                drop(pending);
                driver.await.unwrap();
            }
        }
    });
}

#[test]
fn observation_is_ordered_bounded_and_count_limited() {
    karmaio::Runtime::new().unwrap().block_on(async {
        struct CountRead {
            inner: Reader,
            reads: Rc<Cell<usize>>,
        }
        impl karmaio::io::AsyncRead for CountRead {
            async fn read<B: karmaio::buf::IoBufMut>(&mut self, buffer: B) -> BufResult<usize, B> {
                self.reads.set(self.reads.get() + 1);
                self.inner.read(buffer).await
            }
        }
        let reads = Rc::new(Cell::new(0));
        let reader = CountRead {
            reads: reads.clone(),
            inner: Reader::new([
                ReadStep::Data(Bytes::from_static(b"HTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\n")),
                ReadStep::Data(Bytes::from_static(b"HTTP/1.1 102 Processing\r\n\r\n")),
            ]),
        };
        let (sender, driver) = Client::new()
            .max_informational(1)
            .handshake((reader, Writer::limited(256)));
        let request = Request::builder()
            .uri("/")
            .header("host", "test")
            .body(Empty::new())
            .unwrap();
        let mut pending = sender.start_request(request).await.unwrap();
        {
            let mut next = pin!(pending.next_event());
            assert!(poll(next.as_mut()).is_pending());
        }
        let mut driver = pin!(driver.run());
        for _ in 0..3 {
            assert!(poll(driver.as_mut()).is_pending());
        }
        assert_eq!(reads.get(), 1);
        let Some(ResponseEvent::Informational(head)) = pending.next_event().await.unwrap() else {
            panic!("missing 103")
        };
        assert_eq!(head.headers()["link"], "</a>");
        let (driver, event) = pair(driver, pending.next_event()).await;
        assert_eq!(driver.unwrap_err().kind(), ErrorKind::Limit);
        assert_eq!(event.unwrap_err().kind(), ErrorKind::Limit);
        assert!(pending.next_event().await.unwrap().is_none());
    });
}

#[test]
fn continue_timeout_sends_without_a_peer_permission() {
    struct NotifyWrite {
        writer: Writer,
        uploaded: Gate,
    }
    impl AsyncWrite for NotifyWrite {
        async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
            let result = self.writer.write(buffer).await;
            if self.writer.output.ends_with(b"upload") {
                self.uploaded.open();
            }
            result
        }
        async fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        async fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        karmaio::time::timeout(Duration::from_secs(3), async {
            let uploaded = Gate::default();
            let reader = Reader::new([
                ReadStep::Wait(uploaded.clone()),
                ReadStep::Data(Bytes::from_static(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )),
            ]);
            let (sender, driver) = Client::new()
                .continue_wait(Some(Duration::from_millis(1)))
                .unwrap()
                .handshake((
                    reader,
                    NotifyWrite {
                        writer: Writer::limited(128),
                        uploaded: uploaded.clone(),
                    },
                ));
            let pending = sender.start_request(upload()).await.unwrap();
            let (result, response) = pair(driver.run(), pending.response()).await;
            result.unwrap();
            response.unwrap();
            assert!(uploaded.is_open());
        })
        .await
        .expect("Continue timeout did not release the upload");
    });
}

#[test]
fn expect_echo_survives_context_drop_and_buffered_payload_without_late_continue() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for fragmented in [false,true] {
            let steps = if fragmented { vec![ReadStep::Data(Bytes::from_static(b"POST / HTTP/1.1\r\nHost: test\r\nExpect: 100-continue\r\nContent-Length: 6\r\nConnection: close\r\n\r\n")),ReadStep::Data(Bytes::from_static(b"upload"))] }
                else { vec![ReadStep::Data(Bytes::from_static(b"POST / HTTP/1.1\r\nHost: test\r\nExpect: 100-continue\r\nContent-Length: 6\r\nConnection: close\r\n\r\nupload"))] };
            let service = service_fn(async |(request, context): (Request<Incoming>, RequestContext)| { drop(context); Ok::<_, Infallible>(Response::new(request.into_body())) });
            let (writer, bytes) = ObservedWriter::new();
            Server::new().auto_date(false).serve_connection((Reader::new(steps), writer), service).run().await.unwrap();
            let wire = output(&bytes); assert!(wire.ends_with("upload")); assert_eq!(wire.matches("HTTP/1.1").count(), 1); assert!(!wire.contains("100 Continue"));
        }
    });
}

#[test]
fn late_context_sends_fail_without_emitting_an_extra_head() {
    struct Late {
        context: Option<RequestContext>,
    }
    impl Body for Late {
        type Data = Bytes;
        type Error = Infallible;
        async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, Infallible> {
            if let Some(mut context) = self.context.take() {
                assert_eq!(
                    context.send_informational(info(103)).await.unwrap_err().kind(),
                    ErrorKind::LocalMessage
                );
            }
            Ok(None)
        }
        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(0)
        }
        fn trailer_hint(&self) -> TrailerHint {
            TrailerHint::None
        }
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        let service = service_fn(async |(_, context): (Request<Incoming>, RequestContext)| {
            Ok::<_, Infallible>(Response::new(Late { context: Some(context) }))
        });
        let (writer, bytes) = ObservedWriter::new();
        Server::new()
            .serve_connection(
                (
                    reader(b"GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"),
                    writer,
                ),
                service,
            )
            .run()
            .await
            .unwrap();
        assert_eq!(output(&bytes).matches("HTTP/1.1").count(), 1);
    });
}

#[test]
fn supplied_tcp_continue_supports_demand_before_and_after_service_return() {
    karmaio::Runtime::new().unwrap().block_on(async {
        karmaio::time::timeout(Duration::from_secs(5), async {
            for consume_first in [false, true] {
                let listener =
                    karmaio::net::tcp::TcpListener::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
                        .unwrap();
                let (connected, accepted) = pair(
                    karmaio::net::tcp::TcpStream::connect(listener.local_addr().unwrap()),
                    listener.accept(),
                )
                .await;
                let (server, _) = accepted.unwrap();
                let service = service_fn(
                    async |(mut request, mut context): (Request<Incoming>, RequestContext)| {
                        context.send_informational(info(103)).await.unwrap();
                        if consume_first {
                            let body = request.body_mut().collect(16).await.unwrap();
                            assert_eq!(body.bytes().as_ref(), b"upload");
                            Ok::<_, Infallible>(Response::new(vakya::Either::Left(Full::new(body.into_parts().0))))
                        } else {
                            drop(context);
                            Ok(Response::new(vakya::Either::Right(request.into_body())))
                        }
                    },
                );
                let server = Server::new().serve_tcp(server, service);
                let (sender, driver) = Client::new()
                    .continue_wait(Some(Duration::from_secs(30)))
                    .unwrap()
                    .handshake_tcp(connected.unwrap());
                let application = async {
                    let mut request = upload();
                    request.headers_mut().insert("connection", "close".parse().unwrap());
                    let mut pending = sender.start_request(request).await.unwrap();
                    let mut statuses = vec![];
                    while let Some(event) = pending.next_event().await.unwrap() {
                        match event {
                            ResponseEvent::Informational(head) => statuses.push(head.status().as_u16()),
                            ResponseEvent::Final(response) => {
                                assert_eq!(
                                    response.into_body().collect(16).await.unwrap().bytes().as_ref(),
                                    b"upload"
                                );
                            }
                        }
                    }
                    assert_eq!(statuses, if consume_first { vec![103, 100] } else { vec![103] });
                };
                let ((client, server), ()) = pair(pair(driver.run(), server.run()), application).await;
                client.unwrap();
                server.unwrap();
            }
        })
        .await
        .expect("Continue exchange timed out");
    });
}

#[test]
fn canceled_informational_waiter_does_not_drop_in_flight_head_before_final() {
    struct PausedWrite {
        writer: Writer,
        started: Gate,
        complete: Gate,
        bytes: Rc<RefCell<Vec<u8>>>,
    }
    impl AsyncWrite for PausedWrite {
        async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
            self.started.open();
            self.complete.wait().await;
            let result = self.writer.write(buffer).await;
            *self.bytes.borrow_mut() = self.writer.output.clone();
            result
        }
        async fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        async fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        let started = Gate::default();
        let complete = Gate::default();
        let bytes = Rc::new(RefCell::new(Vec::new()));
        let writer = PausedWrite {
            writer: Writer::limited(256),
            started: started.clone(),
            complete: complete.clone(),
            bytes: bytes.clone(),
        };
        let service = service_fn(async |(_, mut context): (Request<Incoming>, RequestContext)| {
            {
                let mut informational = pin!(context.send_informational(info(103)));
                assert!(poll(informational.as_mut()).is_pending());
                started.wait().await;
                // Leave the send future's scope while the transport retains its head.
            }
            Ok::<_, Infallible>(Response::new(Empty::new()))
        });
        let mut driver = pin!(
            Server::new()
                .auto_date(false)
                .serve_connection(
                    (
                        reader(b"GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"),
                        writer
                    ),
                    service
                )
                .run()
        );
        assert!(poll(driver.as_mut()).is_pending());
        assert!(started.is_open());
        assert!(poll(driver.as_mut()).is_pending());
        assert!(bytes.borrow().is_empty());
        complete.open();
        driver.await.unwrap();
        let wire = output(&bytes);
        assert!(wire.starts_with("HTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 200 OK\r\n"));
    });
}

#[test]
fn receive_failure_settles_an_in_flight_informational_write() {
    struct PendingWrite {
        started: Gate,
        complete: Gate,
    }
    impl AsyncWrite for PendingWrite {
        async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
            self.started.open();
            self.complete.wait().await;
            BufResult(Err(karmaio::runtime::operation_canceled()), buffer)
        }
        async fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        async fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        let started = Gate::default();
        let complete = Gate::default();
        let reader = Reader::new([
            ReadStep::Data(Bytes::from_static(
                b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 1\r\n\r\n",
            )),
            ReadStep::Wait(started.clone()),
            ReadStep::Error(io::Error::other("original receive failure")),
        ]);
        let service = service_fn(
            async |(mut request, mut context): (Request<Incoming>, RequestContext)| {
                let _ = pair(context.send_informational(info(103)), request.body_mut().collect(8)).await;
                Ok::<_, Infallible>(Response::new(Empty::new()))
            },
        );
        let mut driver = pin!(
            Server::new()
                .serve_connection(
                    (
                        reader,
                        PendingWrite {
                            started: started.clone(),
                            complete: complete.clone()
                        }
                    ),
                    service
                )
                .run()
        );
        assert!(poll(driver.as_mut()).is_pending());
        assert!(started.is_open());
        assert!(poll(driver.as_mut()).is_pending());
        complete.open();
        let error = driver.await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Io);
        assert_eq!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<io::Error>()
                .unwrap()
                .to_string(),
            "original receive failure"
        );
    });
}
