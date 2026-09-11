#![cfg(all(feature = "tls", feature = "client", feature = "server"))]
mod support;
use bytes::Bytes;
use karmaio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::tcp::{TcpListener, TcpStream},
    runtime::spawn_local,
    tls::{ClientTlsStream, ServerTlsStream, TlsAcceptor, TlsConnector},
};
use std::{
    cell::Cell,
    convert::Infallible,
    future::{Future, poll_fn},
    pin::pin,
    rc::Rc,
    sync::Arc,
    task::Poll,
};
use support::transport::Gate;
use vakya::{
    Body, BodyExt, Empty, ErrorKind, Frame, Incoming, Request, Response, SizeHint, TrailerHint,
    client::{ResponseEvent, conn::http1::Builder as Client},
    connection::ConnectionOutcome,
    server::{RequestContext, conn::http1::Builder as Server},
    service_fn,
    tls::{HTTP_11_ALPN, TlsInfo, rustls},
};

fn decode_hex(input: &str) -> Vec<u8> {
    let mut high = None;
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for byte in input.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
        let value = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => panic!("invalid hexadecimal fixture"),
        };
        match high.take() {
            Some(high) => bytes.push((high << 4) | value),
            None => high = Some(value),
        }
    }
    assert!(high.is_none(), "hexadecimal fixture has an incomplete byte");
    bytes
}

fn configs(alpn: Option<&[u8]>) -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(decode_hex(include_str!(
            "fixtures/tls/ca.der.hex"
        ))))
        .unwrap();
    let mut client = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(decode_hex(
        include_str!("fixtures/tls/localhost-key.der.hex"),
    )));
    let mut server = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![rustls::pki_types::CertificateDer::from(decode_hex(include_str!(
                "fixtures/tls/localhost.der.hex"
            )))],
            key,
        )
        .unwrap();
    client.alpn_protocols = alpn.into_iter().map(<[u8]>::to_vec).collect();
    server.alpn_protocols = client.alpn_protocols.clone();
    (Arc::new(client), Arc::new(server))
}

async fn connected(alpn: Option<&[u8]>) -> (ClientTlsStream<TcpStream>, ServerTlsStream<TcpStream>) {
    let (client, server) = configs(alpn);
    let listener = TcpListener::bind("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
    let address = listener.local_addr().unwrap();
    let accept = spawn_local(async move {
        let (socket, _) = listener.accept().await.unwrap();
        TlsAcceptor::new(server).accept(socket).await.unwrap()
    });
    let socket = TcpStream::connect(address).await.unwrap();
    let client = TlsConnector::new(client)
        .connect("localhost".try_into().unwrap(), socket)
        .await
        .unwrap();
    (client, accept.await.unwrap())
}

async fn pair<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let mut a = pin!(a);
    let mut b = pin!(b);
    let mut first = None;
    let mut second = None;
    poll_fn(|cx| {
        if first.is_none()
            && let Poll::Ready(value) = a.as_mut().poll(cx)
        {
            first = Some(value);
        }
        if second.is_none()
            && let Poll::Ready(value) = b.as_mut().poll(cx)
        {
            second = Some(value);
        }
        if first.is_some() && second.is_some() {
            Poll::Ready((first.take().unwrap(), second.take().unwrap()))
        } else {
            Poll::Pending
        }
    })
    .await
}

fn request<B>(body: B) -> Request<B> {
    Request::builder()
        .uri("/")
        .header("host", "localhost")
        .body(body)
        .unwrap()
}

fn metadata(info: &TlsInfo, alpn: Option<&[u8]>, server: bool) {
    assert_eq!(info.alpn_protocol(), alpn);
    assert!(info.protocol_version().is_some());
    assert!(info.cipher_suite().is_some());
    assert!(info.handshake_kind().is_some());
    assert_eq!(info.server_name(), server.then_some("localhost"));
}

#[test]
fn supplied_tls_reuses_http_and_attaches_metadata_to_every_head() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for alpn in [None, Some(HTTP_11_ALPN)] {
            let (client, server) = connected(alpn).await;
            let service = service_fn(
                async move |(request, mut context): (Request<Incoming>, RequestContext)| {
                    metadata(request.extensions().get::<TlsInfo>().unwrap(), alpn, true);
                    context
                        .send_informational(Response::builder().status(103).body(()).unwrap())
                        .await
                        .unwrap();
                    Ok::<_, Infallible>(Response::new(Empty::new()))
                },
            );
            let server = Server::new().serve_tls(server, service).unwrap();
            let serving = spawn_local(server.run());
            let (sender, client) = Client::new().handshake_tls::<_, Empty>(client).unwrap();
            let control = client.control();
            let driving = spawn_local(client.run());
            for _ in 0..2 {
                let mut pending = sender.start_request(request(Empty::new())).await.unwrap();
                let Some(ResponseEvent::Informational(head)) = pending.next_event().await.unwrap() else {
                    panic!("missing informational");
                };
                metadata(head.extensions().get::<TlsInfo>().unwrap(), alpn, false);
                let final_head = pending.response().await.unwrap();
                metadata(final_head.extensions().get::<TlsInfo>().unwrap(), alpn, false);
            }
            control.graceful_shutdown();
            drop(sender);
            assert!(matches!(driving.await.unwrap().unwrap(), ConnectionOutcome::Closed));
            assert!(matches!(serving.await.unwrap().unwrap(), ConnectionOutcome::Closed));
        }
    });
}

#[test]
fn incompatible_alpn_is_rejected_before_http_admission() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (client, server) = connected(Some(b"h2")).await;
        let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
            panic!("incompatible TLS stream reached service");
            #[allow(unreachable_code)]
            Ok::<_, Infallible>(Response::new(Empty::new()))
        });
        assert_eq!(
            Client::new().handshake_tls::<_, Empty>(client).err().unwrap().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            Server::new().serve_tls(server, service).err().unwrap().kind(),
            ErrorKind::Unsupported
        );
    });
}

struct Delayed {
    gate: Gate,
    recycled: Rc<Cell<usize>>,
    sent: bool,
}
impl Body for Delayed {
    type Data = Bytes;
    type Error = Infallible;
    async fn next_frame(&mut self) -> Result<Option<Frame<Bytes>>, Infallible> {
        if self.sent {
            return Ok(None);
        }
        self.gate.wait().await;
        self.sent = true;
        Ok(Some(Frame::data(Bytes::from_static(b"duplex"))))
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(if self.sent { 0 } else { 6 })
    }
    fn trailer_hint(&self) -> TrailerHint {
        TrailerHint::None
    }
    fn recycle(&mut self, _: Bytes) {
        self.recycled.set(self.recycled.get() + 1);
    }
}

#[test]
fn encrypted_echo_keeps_upload_alive_after_early_final_head() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (client, server) = connected(Some(HTTP_11_ALPN)).await;
        let calls = Cell::new(0);
        let service = service_fn(async |(request, _): (Request<Incoming>, RequestContext)| {
            calls.set(calls.get() + 1);
            Ok::<_, Infallible>(Response::new(request.into_body()))
        });
        let server = Server::new().serve_tls(server, service).unwrap();
        let gate = Gate::default();
        let recycled = Rc::new(Cell::new(0));
        let (sender, client) = Client::new().handshake_tls(client).unwrap();
        let control = client.control();
        let app = async {
            let body = Delayed {
                gate: gate.clone(),
                recycled: recycled.clone(),
                sent: false,
            };
            let response = sender.send_request(request(body)).await.unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(recycled.get(), 0);
            gate.open();
            let collected = response.into_body().collect(16).await.unwrap();
            assert_eq!(collected.bytes().as_ref(), b"duplex");
            control.graceful_shutdown();
            drop(sender);
        };
        let ((client, server), ()) = pair(pair(client.run(), server.run()), app).await;
        client.unwrap();
        server.unwrap();
        assert_eq!(recycled.get(), 1);
        assert_eq!(calls.get(), 1);
    });
}

#[test]
fn tls_handoff_preserves_encrypted_io_for_upgrade_and_connect() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for tunnel in [false, true] {
            let (client, server) = connected(Some(HTTP_11_ALPN)).await;
            let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
                let response = if tunnel {
                    Response::builder().status(200)
                } else {
                    Response::builder()
                        .status(101)
                        .header("connection", "upgrade")
                        .header("upgrade", "test/1")
                };
                Ok::<_, Infallible>(response.body(Empty::new()).unwrap())
            });
            let server = Server::new().serve_tls(server, service).unwrap();
            let (sender, client) = Client::new().handshake_tls(client).unwrap();
            let request = if tunnel {
                Request::builder()
                    .method("CONNECT")
                    .uri("localhost:443")
                    .header("host", "localhost:443")
            } else {
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .header("connection", "upgrade")
                    .header("upgrade", "test/1")
            };
            let pending = sender.start_request(request.body(Empty::new()).unwrap()).await.unwrap();
            let ((server, client), response) = pair(pair(server.run(), client.run()), pending.response()).await;
            assert!(response.unwrap().extensions().get::<TlsInfo>().is_some());
            let ConnectionOutcome::Upgraded(mut server) = server.unwrap() else {
                panic!("missing server handoff");
            };
            let ConnectionOutcome::Upgraded(mut client) = client.unwrap() else {
                panic!("missing client handoff");
            };
            let (sent, read) = pair(
                server.write_all(Bytes::from_static(b"ping")),
                client.read_exact(Vec::with_capacity(4)),
            )
            .await;
            sent.0.unwrap();
            assert_eq!(read.1, b"ping");
            read.0.unwrap();
            let (sent, read) = pair(
                client.write_all(Bytes::from_static(b"pong")),
                server.read_exact(Vec::with_capacity(4)),
            )
            .await;
            sent.0.unwrap();
            assert_eq!(read.1, b"pong");
            read.0.unwrap();
            let (a, b) = pair(client.shutdown(), server.shutdown()).await;
            a.unwrap();
            b.unwrap();
        }
    });
}

#[test]
fn abort_settles_a_pending_decrypted_read() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (_client, server) = connected(Some(HTTP_11_ALPN)).await;
        let service =
            service_fn(async |_: (Request<Incoming>, RequestContext)| Ok::<_, Infallible>(Response::new(Empty::new())));
        let server = Server::new().serve_tls(server, service).unwrap();
        let control = server.control();
        let mut run = pin!(server.run());
        assert!(poll_fn(|cx| Poll::Ready(run.as_mut().poll(cx))).await.is_pending());
        control.abort();
        assert_eq!(run.await.unwrap_err().kind(), ErrorKind::Canceled);
    });
}

#[test]
fn tls_protocol_failure_retains_the_original_rustls_source() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (client, server) = connected(Some(HTTP_11_ALPN)).await;
        let (mut raw, _) = server.into_parts().unwrap();
        raw.write_all(Bytes::from_static(b"HTTP/1.1 200 OK\r\n\r\n"))
            .await
            .0
            .unwrap();
        let (sender, client) = Client::new().handshake_tls::<_, Empty>(client).unwrap();
        let pending = sender.start_request(request(Empty::new())).await.unwrap();
        let (driver, response) = pair(client.run(), pending.response()).await;
        for error in [driver.err().unwrap(), response.err().unwrap()] {
            assert_eq!(error.kind(), ErrorKind::Tls);
            let source = std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap();
            assert!(source.get_ref().unwrap().is::<rustls::Error>());
        }
    });
}
