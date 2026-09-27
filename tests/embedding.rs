#![cfg(any(feature = "client", feature = "server"))]
mod support;
use bytes::Bytes;
use std::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};
use support::transport::{ReadStep, Reader, Writer};
#[cfg(feature = "server")]
use vakya::body::Empty;
#[cfg(feature = "client")]
use vakya::body::Full;
use vakya::{Request, connection::ConnectionOutcome};

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

#[cfg(feature = "client")]
#[test]
fn boxed_client_keeps_borrowed_body_and_concrete_outcome() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let mut body = Full::new(Bytes::from_static(b"upload"));
        let (sender, connection) = vakya::client::conn::http1::Builder::new().handshake((
            Reader::new([ReadStep::Data(Bytes::from_static(
                b"HTTP/1.1 204 No Content\r\nconnection: close\r\n\r\n",
            ))]),
            Writer::new([]),
        ));
        let connection: vakya::client::conn::http1::BoxedConnection<'_, Reader, Writer> = connection.boxed();
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
        let (outcome, response) = pair(connection.run(), pending.response()).await;
        assert!(matches!(outcome.unwrap(), ConnectionOutcome::Closed));
        assert_eq!(response.unwrap().status(), 204);
    });
}

#[cfg(feature = "server")]
#[test]
fn boxed_server_keeps_borrowed_local_service_and_control() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let calls = std::cell::Cell::new(0);
        let service = vakya::server::request_service_fn(async |_: Request<vakya::body::Incoming>| {
            calls.set(calls.get() + 1);
            Ok::<_, std::convert::Infallible>(vakya::Response::new(Empty::new()))
        });
        let connection =
            vakya::server::conn::http1::Builder::new().serve_connection((Reader::new([]), Writer::new([])), service);
        let control = connection.control();
        let connection: vakya::server::conn::http1::BoxedConnection<'_, Reader, Writer> = connection.boxed();
        control.graceful_shutdown();
        assert!(matches!(connection.run().await.unwrap(), ConnectionOutcome::Closed));
        assert_eq!(calls.get(), 0);
    });
}
