#![cfg(feature = "client")]
mod support;
use bytes::Bytes;
use karmaio::{
    buf::{BufResult, IoBuf},
    io::AsyncWrite,
};
use std::{
    cell::Cell,
    convert::Infallible,
    future::{Future, poll_fn},
    io,
    pin::{Pin, pin},
    rc::Rc,
    task::{Context, Poll, Waker},
};
use support::transport::{Gate, ReadStep, Reader, Writer};
use vakya::{
    Request,
    body::{Body, Empty, Frame, SizeHint, TrailerHint},
    client::{CloseReason, ExchangeOutcome, conn::http1::Builder},
    connection::ConnectionOutcome,
    error::{ErrorDetail, ErrorKind, TimeoutPhase},
    upgrade::UpgradeKind,
};

fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
async fn pair<A: Future, B: Future>(first: A, second: B) -> (A::Output, B::Output) {
    let mut first = pin!(first);
    let mut second = pin!(second);
    let (mut a, mut b) = (None, None);
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
fn reader(bytes: &'static [u8]) -> Reader {
    Reader::new([ReadStep::Data(Bytes::from_static(bytes))])
}
fn request<B>(body: B) -> Request<B> {
    Request::builder().uri("/").header("host", "test").body(body).unwrap()
}

struct TerminalBody {
    gate: Gate,
    produced: bool,
}
impl Body for TerminalBody {
    type Data = Bytes;
    type Error = Infallible;
    async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, Infallible> {
        if self.produced {
            self.gate.wait().await;
            Ok(None)
        } else {
            self.produced = true;
            Ok(Some(Frame::data(Bytes::from_static(b"x"))))
        }
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(u64::from(!self.produced))
    }
    fn trailer_hint(&self) -> TrailerHint {
        TrailerHint::None
    }
}

#[test]
fn reuse_waits_for_producer_terminal_checks_and_not_application_owned_payloads() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let gate = Gate::default();
        let (sender, driver) = Builder::new().handshake((
            reader(b"HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\ny"),
            Writer::new([]),
        ));
        let shutdown = driver.control();
        let mut pending = sender
            .start_request(request(TerminalBody {
                gate: gate.clone(),
                produced: false,
            }))
            .await
            .unwrap();
        let completion = pending.take_completion().unwrap();
        assert!(pending.take_completion().is_none());
        let mut completion = pin!(completion);
        assert!(poll(completion.as_mut()).is_pending()); // Receipt doesn't drive HTTP.
        let mut driver = pin!(driver.run());
        assert!(poll(driver.as_mut()).is_pending());
        let mut response = pending.response().await.unwrap();
        assert!(poll(completion.as_mut()).is_pending());
        let frame = {
            let mut next = pin!(response.body_mut().next_frame());
            assert!(poll(next.as_mut()).is_pending());
            assert!(poll(driver.as_mut()).is_pending());
            next.await.unwrap().unwrap()
        };
        let payload = frame.into_data().unwrap();
        assert_eq!(payload.as_ref(), b"y");
        assert!(poll(driver.as_mut()).is_pending());
        assert!(!sender.is_ready());
        assert!(poll(completion.as_mut()).is_pending());
        gate.open();
        assert!(poll(driver.as_mut()).is_pending());
        assert_eq!(completion.await.unwrap(), ExchangeOutcome::Reusable);
        assert!(sender.is_ready());
        assert_eq!(payload.as_ref(), b"y"); // Payload lifetime is independent.
        shutdown.graceful_shutdown();
        driver.await.unwrap();
    });
}

struct ShutdownWriter {
    complete: Gate,
    calls: Rc<Cell<usize>>,
    failure: bool,
}
impl AsyncWrite for ShutdownWriter {
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
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "original shutdown failure"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn closing_receipts_wait_for_shutdown_and_outer_failure_composition() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for mode in ["policy", "sender", "graceful", "abort", "deadline", "failure"] {
            let complete = Gate::default();
            let calls = Rc::new(Cell::new(0));
            let wire = if matches!(mode, "sender" | "graceful") {
                b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n".as_slice()
            } else {
                b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".as_slice()
            };
            let (sender, driver) = Builder::new().handshake((
                reader(wire),
                ShutdownWriter {
                    complete: complete.clone(),
                    calls: calls.clone(),
                    failure: mode == "failure",
                },
            ));
            let control = driver.control();
            let mut pending = sender.start_request(request(Empty::new())).await.unwrap();
            let mut completion = pin!(pending.take_completion().unwrap());
            if mode == "graceful" {
                control.graceful_shutdown();
            }
            if mode == "sender" {
                drop(sender);
            }
            let mut driver = pin!(driver.run());
            assert!(poll(driver.as_mut()).is_pending());
            pending.response().await.unwrap();
            assert!(poll(driver.as_mut()).is_pending());
            assert_eq!(calls.get(), 1);
            assert!(poll(completion.as_mut()).is_pending(), "{mode}");
            if mode == "abort" {
                control.abort();
            }
            if mode == "deadline" {
                control.graceful_shutdown_with_deadline(std::time::Instant::now());
                assert!(poll(driver.as_mut()).is_pending());
                karmaio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            assert!(poll(driver.as_mut()).is_pending());
            assert!(poll(completion.as_mut()).is_pending());
            complete.open();
            let result = driver.await;
            let receipt = completion.await;
            match mode {
                "abort" | "deadline" | "failure" => {
                    let (driver, receipt) = (result.unwrap_err(), receipt.unwrap_err());
                    assert_eq!(receipt.kind(), driver.kind());
                    assert_eq!(receipt.detail(), driver.detail());
                    if mode == "failure" {
                        assert!(std::ptr::eq(
                            std::error::Error::source(&driver).unwrap(),
                            std::error::Error::source(&receipt).unwrap()
                        ));
                    }
                    if mode == "deadline" {
                        assert_eq!(receipt.detail(), Some(ErrorDetail::Timeout(TimeoutPhase::Shutdown)));
                    }
                    if mode == "failure" {
                        assert_eq!(receipt.kind(), ErrorKind::Io);
                        assert_eq!(
                            std::error::Error::source(&receipt).unwrap().to_string(),
                            "original shutdown failure"
                        );
                    }
                }
                _ => {
                    result.unwrap();
                    assert_eq!(
                        receipt.unwrap(),
                        ExchangeOutcome::Closed(match mode {
                            "sender" => CloseReason::ApplicationClosure,
                            "graceful" => CloseReason::Shutdown,
                            _ => CloseReason::MessagePolicy,
                        })
                    );
                }
            }
        }
    });
}

#[test]
fn receipt_drop_does_not_cancel_and_driver_drop_never_certifies_settlement() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for active in [false, true] {
            let (sender, driver) =
                Builder::new().handshake((reader(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n"), Writer::new([])));
            let mut pending = sender.start_request(request(Empty::new())).await.unwrap();
            let completion = pending.take_completion().unwrap();
            if active {
                let mut run = Box::pin(driver.run());
                assert!(poll(run.as_mut()).is_pending());
                pending.response().await.unwrap();
                drop(run);
            } else {
                drop(driver);
                drop(pending);
            }
            let error = completion.await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Closed);
            assert_eq!(error.detail(), Some(ErrorDetail::DriverLost));
        }
        let (sender, driver) = Builder::new().handshake((
            reader(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"),
            Writer::new([]),
        ));
        let mut pending = sender.start_request(request(Empty::new())).await.unwrap();
        drop(pending.take_completion().unwrap());
        assert!(pending.take_completion().is_none());
        let (result, response) = pair(driver.run(), pending.response()).await;
        result.unwrap();
        response.unwrap();
    });
}

#[test]
fn read_ahead_and_preparation_failures_reach_the_receipt() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for local in [false, true] {
            let (sender, driver) = Builder::new().handshake((
                reader(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\nunsolicited"),
                Writer::new([]),
            ));
            let mut request = request(Empty::new());
            if local {
                request.headers_mut().insert("content-length", "5".parse().unwrap());
            }
            let mut pending = sender.start_request(request).await.unwrap();
            let completion = pending.take_completion().unwrap();
            let (driver, response) = pair(driver.run(), pending.response()).await;
            let error = completion.await.unwrap_err();
            assert_eq!(
                error.kind(),
                if local {
                    ErrorKind::LocalMessage
                } else {
                    ErrorKind::InvalidMessage
                }
            );
            assert_eq!(error.kind(), driver.unwrap_err().kind());
            if local {
                assert_eq!(error.kind(), response.unwrap_err().kind());
            } else {
                response.unwrap();
            }
        }
    });
}

#[test]
fn upgrade_receipts_follow_claimed_and_unclaimed_transport_delivery() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for tunnel in [false, true] {
            for claim in ["none", "boxed", "typed", "abandoned"] {
                let wire = if tunnel {
                    b"HTTP/1.1 200 OK\r\n\r\nraw".as_slice()
                } else {
                    b"HTTP/1.1 101 Switching Protocols\r\nconnection: upgrade\r\nupgrade: test/1\r\n\r\nraw".as_slice()
                };
                let (sender, driver) = Builder::new().handshake((reader(wire), Writer::new([])));
                let request = if tunnel {
                    Request::builder()
                        .method("CONNECT")
                        .uri("test:443")
                        .header("host", "test:443")
                        .body(Empty::new())
                        .unwrap()
                } else {
                    Request::builder()
                        .uri("/")
                        .header("host", "test")
                        .header("connection", "upgrade")
                        .header("upgrade", "test/1")
                        .body(Empty::new())
                        .unwrap()
                };
                let mut pending = sender.start_request(request).await.unwrap();
                let completion = pending.take_completion().unwrap();
                let boxed = (claim == "boxed").then(|| pending.on_upgrade());
                let typed = (claim == "typed").then(|| pending.on_upgrade_typed::<Reader, Writer>());
                if claim == "abandoned" {
                    drop(pending.on_upgrade());
                }
                let driver = driver.boxed();
                let run = if claim == "typed" {
                    driver.run_without_shutdown()
                } else {
                    driver.run()
                };
                let (result, response) = pair(run, pending.response()).await;
                response.unwrap();
                let kind = if tunnel {
                    UpgradeKind::Tunnel
                } else {
                    UpgradeKind::Protocol
                };
                assert_eq!(completion.await.unwrap(), ExchangeOutcome::Upgraded(kind));
                match result.unwrap() {
                    ConnectionOutcome::UpgradeClaimed(actual) => assert_eq!(actual, kind),
                    ConnectionOutcome::Upgraded(_) => assert!(matches!(claim, "none" | "abandoned")),
                    _ => panic!("missing handoff"),
                }
                if let Some(boxed) = boxed {
                    assert_eq!(boxed.await.unwrap().kind(), kind);
                }
                if let Some(typed) = typed {
                    assert_eq!(typed.await.unwrap().kind(), kind);
                }
            }
        }
    });
}

#[test]
fn abandoned_body_receipt_waits_for_retained_read_settlement() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let complete = Gate::default();
        let input = Reader::new([
            ReadStep::Data(Bytes::from_static(b"HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\n")),
            ReadStep::Wait(complete.clone()),
            ReadStep::Error(karmaio::runtime::operation_canceled()),
        ]);
        let (sender, driver) = Builder::new().handshake((input, Writer::new([])));
        let mut pending = sender.start_request(request(Empty::new())).await.unwrap();
        let mut receipt = pin!(pending.take_completion().unwrap());
        let mut driver = pin!(driver.run());
        assert!(poll(driver.as_mut()).is_pending());
        let mut response = pending.response().await.unwrap();
        {
            let mut frame = pin!(response.body_mut().next_frame());
            assert!(poll(frame.as_mut()).is_pending());
            assert!(poll(driver.as_mut()).is_pending());
        }
        drop(response);
        assert!(poll(driver.as_mut()).is_pending());
        assert!(poll(receipt.as_mut()).is_pending());
        complete.open();
        driver.await.unwrap();
        assert_eq!(
            receipt.await.unwrap(),
            ExchangeOutcome::Closed(CloseReason::BodyAbandonment)
        );
    });
}
