//! An echo server that copies POST bodies back to the client.
//!
//! Run with `cargo run --example echo --features server`.
use std::{convert::Infallible, io, net::SocketAddr};

use bytes::Bytes;
use vakya::{
    Method, Request, Response, StatusCode,
    body::{Body, BodyExt, BoxBody, CollectError, Empty, Full, Incoming},
    error::Error,
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
};

async fn echo(
    (req, _context): (Request<Incoming>, RequestContext),
) -> Result<Response<BoxBody<'static, Bytes, Error>>, Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok(Response::new(full(
            "Try POSTing data to /echo such as: `curl localhost:3000/echo -XPOST -d \"hello world\"`",
        ))),
        (&Method::POST, "/echo") => Ok(Response::new(map_bytes(req.into_body()).boxed())),
        (&Method::POST, "/echo/uppercase") => {
            let body = req.into_body().map_frame(|frame| {
                frame.map_data(|data| {
                    Bytes::from(
                        data.as_ref()
                            .iter()
                            .copied()
                            .map(|byte| byte.to_ascii_uppercase())
                            .collect::<Vec<_>>(),
                    )
                })
            });
            Ok(Response::new(body.boxed()))
        }
        (&Method::POST, "/echo/reversed") => {
            let max = req.body().size_hint().upper().unwrap_or(u64::MAX);
            if max > 64 * 1024 {
                let mut resp = Response::new(full("Body too big"));
                *resp.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
                return Ok(resp);
            }

            let whole_body = match req.into_body().collect(64 * 1024).await {
                Ok(collected) => collected,
                Err(CollectError::LimitExceeded { .. }) => {
                    let mut resp = Response::new(full("Body too big"));
                    *resp.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
                    return Ok(resp);
                }
                Err(CollectError::Body(error)) => return Err(error),
                Err(error) => return Err(io::Error::other(error.to_string()).into()),
            };
            let reversed = whole_body.as_ref().iter().rev().copied().collect::<Vec<u8>>();
            Ok(Response::new(full(reversed)))
        }
        _ => {
            let mut not_found = Response::new(empty());
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

fn map_bytes(body: Incoming) -> impl vakya::body::Body<Data = Bytes, Error = Error> {
    body.map_frame(|frame| frame.map_data(|data| Bytes::copy_from_slice(data.as_ref())))
}

fn empty() -> BoxBody<'static, Bytes, Error> {
    Empty::new().map_err(infallible).boxed()
}

fn full(chunk: impl Into<Bytes>) -> BoxBody<'static, Bytes, Error> {
    Full::new(chunk.into()).map_err(infallible).boxed()
}

fn infallible(never: Infallible) -> Error {
    match never {}
}

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = karmaio::net::tcp::TcpListener::bind(addr)?;
    println!("Listening on http://{addr}");

    loop {
        let (socket, _) = listener.accept().await?;
        karmaio::runtime::spawn_local(async move {
            if let Err(err) = Builder::new().serve_tcp(socket, service_fn(echo)).run().await {
                eprintln!("Error serving connection: {err:?}");
            }
        });
    }
}
