#![cfg(all(feature = "client", feature = "server"))]
mod support;

use bytes::Bytes;
use std::{
    convert::Infallible,
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};
use support::transport::{ReadStep, Reader, Writer};
use vakya::{
    Request, Response,
    body::{Empty, Incoming},
    client::conn::http1::Builder as Client,
    connection::ConnectionOutcome,
    error::{Error, ErrorDetail, ErrorKind, LimitKind},
    server::{conn::http1::Builder as Server, request_service_fn},
};

fn fragmented(bytes: Vec<u8>) -> Reader {
    Reader::new(
        bytes
            .into_iter()
            .map(|byte| ReadStep::Data(Bytes::copy_from_slice(&[byte]))),
    )
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
        match (first.as_ref(), second.as_ref()) {
            (Some(_), Some(_)) => Poll::Ready((first.take().unwrap(), second.take().unwrap())),
            _ => Poll::Pending,
        }
    })
    .await
}

fn check<T>(result: Result<T, Error>, allowed: bool) {
    match result {
        Ok(_) => assert!(allowed, "extensions should exceed the configured budget"),
        Err(error) => {
            assert!(!allowed, "{error}");
            assert_eq!(error.kind(), ErrorKind::Limit);
            assert_eq!(
                error.detail(),
                Some(ErrorDetail::IncomingLimit(LimitKind::ChunkExtensionBytes))
            );
        }
    }
}

#[test]
fn both_roles_enforce_total_extension_budgets_after_other_limit_updates() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for (body, budget, allowed) in [
            (&b"1\r\nx\r\n0\r\n\r\n"[..], 0, true),
            (&b"1;a\r\nx\r\n0\r\n\r\n"[..], 0, false),
            (&b"1;a\r\nx\r\n1;b\r\ny\r\n0\r\n\r\n"[..], 4, true),
            (&b"1;a\r\nx\r\n1;b\r\ny\r\n0\r\n\r\n"[..], 3, false),
            (&b"0;a\r\n\r\n"[..], 0, false),
        ] {
            let mut client = Client::new();
            client.incoming_chunk_extension_limit(budget);
            client.incoming_framing_limits(128, 256, 8).unwrap();
            assert!(client.incoming_framing_limits(0, 900, 4).is_err());
            client.incoming_head_limits(512, 8).unwrap();
            client.outgoing_head_limit(256).unwrap();
            client.outgoing_trailer_limit(256).unwrap();
            let mut wire = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
            wire.extend_from_slice(body);
            let (sender, connection) = client.handshake::<_, Empty>((fragmented(wire), Writer::limited(3)));
            let pending = sender
                .start_request(
                    Request::builder()
                        .uri("/")
                        .header("host", "test")
                        .body(Empty::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            drop(sender);
            let (run, drain) = pair(connection.run(), async {
                pending.response().await?.into_body().drain(32).await
            })
            .await;
            check(run, allowed);
            check(drain, allowed);

            let mut server = Server::new();
            server.incoming_chunk_extension_limit(budget);
            server.incoming_framing_limits(128, 256, 8).unwrap();
            assert!(server.incoming_framing_limits(900, 0, 4).is_err());
            server.incoming_head_limits(512, 8).unwrap();
            server.outgoing_head_limit(256).unwrap();
            server.outgoing_trailer_limit(256).unwrap();
            let mut wire = b"POST / HTTP/1.1\r\nhost: test\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
            wire.extend_from_slice(body);
            let service = request_service_fn(async |request: Request<Incoming>| {
                let _ = request.into_body().drain(32).await;
                Ok::<_, Infallible>(Response::new(Empty::new()))
            });
            let run = server
                .serve_connection((fragmented(wire), Writer::limited(3)), service)
                .run()
                .await;
            if allowed {
                assert!(matches!(&run, Ok(ConnectionOutcome::Closed)));
            }
            check(run, allowed);
        }
    });
}
