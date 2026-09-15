#![cfg(all(feature = "client", feature = "server"))]
mod support;
use bytes::Bytes;
use karmaio::{
    buf::{BufResult, IoBuf, IoVectoredBuf},
    io::{AsyncRead, AsyncWrite},
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
    Request, Response,
    body::{Body, BodyExt, Empty, Frame, Incoming, SizeHint, TrailerHint},
    client::conn::http1::Builder as Client,
    connection::ConnectionOutcome,
    error::ErrorKind,
    server::{RequestContext, conn::http1::Builder as Server},
    service::service_fn,
    upgrade::UpgradeKind,
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

fn request<B>(body: B, tunnel: bool) -> Request<B> {
    let request = Request::builder()
        .method(if tunnel { "CONNECT" } else { "POST" })
        .uri(if tunnel { "test:443" } else { "/" })
        .header("host", if tunnel { "test:443" } else { "test" });
    if tunnel {
        request.body(body).unwrap()
    } else {
        request
            .header("connection", "upgrade")
            .header("upgrade", "test/1")
            .body(body)
            .unwrap()
    }
}
fn response(tunnel: bool) -> Response<Empty> {
    if tunnel {
        Response::new(Empty::new())
    } else {
        Response::builder()
            .status(101)
            .header("connection", "upgrade")
            .header("upgrade", "test/1")
            .body(Empty::new())
            .unwrap()
    }
}
fn response_wire(tunnel: bool) -> &'static [u8] {
    if tunnel {
        b"HTTP/1.1 200 OK\r\n\r\nraw"
    } else {
        b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: test/1\r\n\r\nraw"
    }
}

#[test]
fn server_transfers_settled_halves_and_serves_read_ahead_before_transport() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for tunnel in [false, true] {
            let head: &'static [u8] = if tunnel {
                b"CONNECT test:443 HTTP/1.1\r\nHost: test:443\r\n\r\nraw"
            } else {
                b"POST / HTTP/1.1\r\nHost: test\r\nConnection: upgrade\r\nUpgrade: test/1\r\n\r\nraw"
            };
            let service =
                service_fn(async |(_, _): (Request<Incoming>, RequestContext)| Ok::<_, Infallible>(response(tunnel)));
            let result = Server::new()
                .auto_date(false)
                .serve_connection(
                    (
                        Reader::new([
                            ReadStep::Data(Bytes::from_static(head)),
                            ReadStep::Data(Bytes::from_static(b"tail")),
                        ]),
                        Writer::limited(128),
                    ),
                    service,
                )
                .run()
                .await
                .unwrap();
            let ConnectionOutcome::Upgraded(mut upgraded) = result else {
                panic!("missing handoff")
            };
            assert_eq!(
                upgraded.kind(),
                if tunnel {
                    UpgradeKind::Tunnel
                } else {
                    UpgradeKind::Protocol
                }
            );
            assert_eq!(upgraded.read_ahead(), b"raw");
            let (result, empty) = upgraded.read(Vec::new()).await.into_parts();
            assert_eq!(result.unwrap(), 0);
            assert!(empty.is_empty());
            assert_eq!(upgraded.read_ahead(), b"raw");
            let (result, bytes) = upgraded.read(Vec::with_capacity(2)).await.into_parts();
            assert_eq!(result.unwrap(), 2);
            assert_eq!(bytes, b"ra");
            let (result, bytes) = upgraded.read(Vec::with_capacity(16)).await.into_parts();
            assert_eq!(result.unwrap(), 1);
            assert_eq!(bytes, b"w");
            let (result, bytes) = upgraded.read(Vec::with_capacity(16)).await.into_parts();
            assert_eq!(result.unwrap(), 4);
            assert_eq!(bytes, b"tail");
            upgraded
                .write(Bytes::from_static(b"application"))
                .await
                .into_parts()
                .0
                .unwrap();
            let (reader, writer, remaining, _) = upgraded.into_parts();
            assert_eq!(reader.submissions, 2);
            assert!(remaining.is_empty());
            assert!(writer.output.ends_with(b"application"));
            assert_eq!(String::from_utf8(writer.output).unwrap().matches("HTTP/1.1").count(), 1);
        }
    });
}

#[test]
fn server_request_upgrade_future_receives_settled_typed_transport() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let claimed = RefCell::new(None);
        let service = service_fn(async |(_, mut context): (Request<Incoming>, RequestContext)| {
            claimed.replace(Some(context.on_upgrade::<Reader, Writer>()));
            Ok::<_, Infallible>(response(false))
        });
        let outcome = Server::new()
            .auto_date(false)
            .serve_connection(
                (
                    reader(b"GET / HTTP/1.1\r\nHost: test\r\nConnection: upgrade\r\nUpgrade: test/1\r\n\r\nraw"),
                    Writer::limited(128),
                ),
                service,
            )
            .run()
            .await
            .unwrap();

        assert!(matches!(
            outcome,
            ConnectionOutcome::UpgradeClaimed(UpgradeKind::Protocol)
        ));
        let upgraded = claimed.into_inner().expect("service claimed upgrade").await.unwrap();
        assert_eq!(upgraded.kind(), UpgradeKind::Protocol);
        assert_eq!(upgraded.read_ahead(), b"raw");
    });
}

#[test]
fn client_delivers_final_head_and_retires_admission_on_handoff() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for tunnel in [false, true] {
            let (sender, driver) = Client::new().handshake((reader(response_wire(tunnel)), Writer::limited(128)));
            let pending = sender.start_request(request(Empty::new(), tunnel)).await.unwrap();
            let (outcome, response) = pair(driver.run(), pending.response()).await;
            let response = response.unwrap();
            assert_eq!(response.status(), if tunnel { 200 } else { 101 });
            assert!(response.into_body().collect(0).await.unwrap().bytes().is_empty());
            let ConnectionOutcome::Upgraded(upgraded) = outcome.unwrap() else {
                panic!("missing handoff")
            };
            let (_, _, bytes, kind) = upgraded.into_parts();
            assert_eq!(bytes, b"raw"[..]);
            assert_eq!(
                kind,
                if tunnel {
                    UpgradeKind::Tunnel
                } else {
                    UpgradeKind::Protocol
                }
            );
            assert_eq!(sender.reserve().await.err().unwrap().kind(), ErrorKind::Closed);
        }
    });
}

#[test]
fn client_response_upgrade_future_receives_settled_typed_transport() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (sender, driver) = Client::new().handshake((reader(response_wire(false)), Writer::limited(128)));
        let (outcome, response) = pair(
            driver.run(),
            sender.send_request_with_upgrade::<Reader, Writer>(request(Empty::new(), false)),
        )
        .await;
        let (response, upgrade) = response.unwrap();
        assert_eq!(response.status(), 101);
        assert!(matches!(
            outcome.unwrap(),
            ConnectionOutcome::UpgradeClaimed(UpgradeKind::Protocol)
        ));
        let upgraded = upgrade.await.unwrap();
        assert_eq!(upgraded.read_ahead(), b"raw");
    });
}

#[test]
fn upgrade_future_fails_when_the_response_does_not_upgrade() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (sender, driver) = Client::new().handshake((
            reader(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"),
            Writer::limited(128),
        ));
        let (outcome, response) = pair(
            driver.run(),
            sender.send_request_with_upgrade::<Reader, Writer>(request(Empty::new(), false)),
        )
        .await;
        assert!(matches!(outcome.unwrap(), ConnectionOutcome::Closed));
        let (response, upgrade) = response.unwrap();
        assert_eq!(response.status(), 204);
        assert_eq!(upgrade.await.unwrap_err().kind(), ErrorKind::Upgrade);
    });
}

#[test]
fn mismatched_switches_fail_before_server_writes_or_client_handoff() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let service = service_fn(async |(_, _): (Request<Incoming>, RequestContext)| {
            let mut response = response(false);
            response.headers_mut().insert("upgrade", "other".parse().unwrap());
            Ok::<_, Infallible>(response)
        });
        let (writer, bytes) = ObservedWriter::new();
        let error = Server::new()
            .serve_connection(
                (
                    reader(b"GET / HTTP/1.1\r\nHost: test\r\nConnection: upgrade\r\nUpgrade: test/1\r\n\r\n"),
                    writer,
                ),
                service,
            )
            .run()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Upgrade);
        assert!(bytes.borrow().is_empty());
        for tunnel in [false, true] {
            let (sender, driver) = Client::new().handshake((
                reader(b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: other\r\n\r\nraw"),
                Writer::limited(128),
            ));
            let pending = sender.start_request(request(Empty::new(), tunnel)).await.unwrap();
            let (driver, response) = pair(driver.run(), pending.response()).await;
            assert_eq!(driver.unwrap_err().kind(), ErrorKind::Upgrade);
            assert_eq!(response.unwrap_err().kind(), ErrorKind::Upgrade);
        }
    });
}

struct Upload {
    gate: Gate,
    sent: bool,
    fail: bool,
    recycled: Rc<Cell<usize>>,
}
impl Body for Upload {
    type Data = Bytes;
    type Error = io::Error;
    async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, io::Error> {
        if self.sent {
            return Ok(None);
        }
        self.gate.wait().await;
        if self.fail {
            return Err(io::Error::other("upload failed"));
        }
        self.sent = true;
        Ok(Some(Frame::data(Bytes::from_static(b"body"))))
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(if self.sent { 0 } else { 4 })
    }
    fn trailer_hint(&self) -> TrailerHint {
        TrailerHint::None
    }
    fn recycle(&mut self, _: Bytes) {
        self.recycled.set(self.recycled.get() + 1)
    }
}

#[test]
fn early_switch_waits_for_upload_and_failure_never_transfers_io() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for fail in [false, true] {
            let gate = Gate::default();
            let recycled = Rc::new(Cell::new(0));
            let (sender, driver) = Client::new().handshake((reader(response_wire(false)), Writer::limited(128)));
            let pending = sender
                .start_request(request(
                    Upload {
                        gate: gate.clone(),
                        sent: false,
                        fail,
                        recycled: recycled.clone(),
                    },
                    false,
                ))
                .await
                .unwrap();
            let mut driver = pin!(driver.run());
            assert!(poll(driver.as_mut()).is_pending());
            assert_eq!(pending.response().await.unwrap().status(), 101);
            assert!(poll(driver.as_mut()).is_pending());
            gate.open();
            let result = driver.await;
            if fail {
                assert_eq!(result.unwrap_err().kind(), ErrorKind::Body)
            } else {
                let ConnectionOutcome::Upgraded(upgraded) = result.unwrap() else {
                    panic!("handoff missing")
                };
                assert_eq!(upgraded.read_ahead(), b"raw");
                assert_eq!(recycled.get(), 1);
            }
        }
    });
}

#[test]
fn server_switch_waits_for_retained_request_body_to_finish() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let held=RefCell::new(None);
        let service=service_fn(async |(request,_): (Request<Incoming>, RequestContext)| {held.replace(Some(request.into_body()));Ok::<_,Infallible>(response(false))});
        let (writer,bytes)=ObservedWriter::new();
        let mut driver=pin!(Server::new().auto_date(false).serve_connection((reader(b"POST / HTTP/1.1\r\nHost: test\r\nConnection: upgrade\r\nUpgrade: test/1\r\nContent-Length: 4\r\n\r\nbodyraw"),writer),service).run());
        for _ in 0..5 {assert!(poll(driver.as_mut()).is_pending());}
        assert!(output(&bytes).contains("101 Switching Protocols"));
        let incoming=held.borrow_mut().take().unwrap();
        let (outcome,body)=pair(driver,incoming.collect(4)).await;
        assert_eq!(body.unwrap().bytes().as_ref(),b"body");
        let ConnectionOutcome::Upgraded(upgraded)=outcome.unwrap() else {panic!("missing handoff")};
        assert_eq!(upgraded.read_ahead(),b"raw");
    });
}

#[test]
fn supplied_tcp_handoff_leaves_both_transports_open_for_application_io() {
    use karmaio::io::{AsyncReadExt, AsyncWriteExt};
    karmaio::Runtime::new().unwrap().block_on(async {
        karmaio::time::timeout(std::time::Duration::from_secs(5), async {
            for tunnel in [false, true] {
                let listener =
                    karmaio::net::tcp::TcpListener::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
                        .unwrap();
                let (connected, accepted) = pair(
                    karmaio::net::tcp::TcpStream::connect(listener.local_addr().unwrap()),
                    listener.accept(),
                )
                .await;
                let (socket, _) = accepted.unwrap();
                let service = service_fn(async |(_, _): (Request<Incoming>, RequestContext)| {
                    Ok::<_, Infallible>(response(tunnel))
                });
                let server = Server::new().serve_tcp(socket, service);
                let (sender, client) = Client::new().handshake_tcp(connected.unwrap());
                let pending = sender.start_request(request(Empty::new(), tunnel)).await.unwrap();
                let ((server, client), response) = pair(pair(server.run(), client.run()), pending.response()).await;
                response.unwrap();
                let ConnectionOutcome::Upgraded(mut server) = server.unwrap() else {
                    panic!("server handoff missing")
                };
                let ConnectionOutcome::Upgraded(mut client) = client.unwrap() else {
                    panic!("client handoff missing")
                };
                let (sent, read) = pair(
                    server.write_all(Bytes::from_static(b"ping")),
                    client.read_exact(Vec::with_capacity(4)),
                )
                .await;
                assert_eq!(sent.into_parts().0.unwrap(), 4);
                let (result, bytes) = read.into_parts();
                assert_eq!(result.unwrap(), 4);
                assert_eq!(bytes, b"ping");
                let (sent, read) = pair(
                    client.write_all(Bytes::from_static(b"pong")),
                    server.read_exact(Vec::with_capacity(4)),
                )
                .await;
                assert_eq!(sent.into_parts().0.unwrap(), 4);
                let (result, bytes) = read.into_parts();
                assert_eq!(result.unwrap(), 4);
                assert_eq!(bytes, b"pong");
            }
        })
        .await
        .expect("handoff timed out");
    });
}

#[test]
fn normal_responses_close_without_transferring_and_abandoned_uploads_cannot_upgrade() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (sender, driver) = Client::new().handshake((
            reader(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
            Writer::limited(128),
        ));
        let pending = sender.start_request(request(Empty::new(), false)).await.unwrap();
        let (outcome, response) = pair(driver.run(), pending.response()).await;
        assert_eq!(response.unwrap().status(), 403);
        assert!(matches!(outcome.unwrap(), ConnectionOutcome::Closed));
        let (sender, driver) = Client::new().handshake((reader(response_wire(false)), Writer::limited(128)));
        let pending = sender
            .start_request(request(
                Upload {
                    gate: Gate::default(),
                    sent: false,
                    fail: false,
                    recycled: Rc::new(Cell::new(0)),
                },
                false,
            ))
            .await
            .unwrap();
        let control = pending.control();
        let mut driver = pin!(driver.run());
        assert!(poll(driver.as_mut()).is_pending());
        assert_eq!(pending.response().await.unwrap().status(), 101);
        control.abort();
        assert!(matches!(driver.await.unwrap(), ConnectionOutcome::Closed));
    });
}

#[test]
fn server_handoff_waits_for_flush_and_preserves_flush_failures() {
    struct Flush {
        writer: Writer,
        gate: Gate,
        fail: bool,
    }
    impl AsyncWrite for Flush {
        async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
            self.writer.write(buffer).await
        }
        async fn flush(&mut self) -> io::Result<()> {
            self.gate.wait().await;
            if self.fail {
                Err(io::Error::other("handoff flush failed"))
            } else {
                Ok(())
            }
        }
        async fn shutdown(&mut self) -> io::Result<()> {
            panic!("handoff must not shut down its transport")
        }
    }
    karmaio::Runtime::new().unwrap().block_on(async {
        for fail in [false, true] {
            let gate = Gate::default();
            let service =
                service_fn(async |(_, _): (Request<Incoming>, RequestContext)| Ok::<_, Infallible>(response(false)));
            let writer = Flush {
                writer: Writer::limited(128),
                gate: gate.clone(),
                fail,
            };
            let mut driver = pin!(
                Server::new()
                    .serve_connection(
                        (
                            reader(
                                b"GET / HTTP/1.1\r\nHost: test\r\nConnection: upgrade\r\nUpgrade: test/1\r\n\r\nraw"
                            ),
                            writer
                        ),
                        service
                    )
                    .run()
            );
            assert!(poll(driver.as_mut()).is_pending());
            gate.open();
            let result = driver.await;
            if fail {
                let error = result.unwrap_err();
                assert_eq!(error.kind(), ErrorKind::Io);
                assert_eq!(
                    std::error::Error::source(&error)
                        .unwrap()
                        .downcast_ref::<io::Error>()
                        .unwrap()
                        .to_string(),
                    "handoff flush failed"
                );
            } else {
                let ConnectionOutcome::Upgraded(io) = result.unwrap() else {
                    panic!("handoff missing")
                };
                assert_eq!(io.read_ahead(), b"raw");
            }
        }
    });
}
