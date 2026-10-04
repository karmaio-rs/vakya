//! Echo incoming frames with concrete body dispatch and owned payload forwarding.
//!
//! Run with `cargo run --example owned_echo --features server`, then
//! `curl localhost:3000/echo -X POST -d 'hello'`.
use bytes::Bytes;
use std::{convert::Infallible, net::SocketAddr};
use vakya::{
    Method, Request, Response, StatusCode,
    body::{Either, Full, Incoming},
    server::{conn::http1::Builder, request_service_fn},
};

type EchoBody = Either<Full<Bytes>, Incoming>;

async fn echo(request: Request<Incoming>) -> Result<Response<EchoBody>, Infallible> {
    match (request.method(), request.uri().path()) {
        (&Method::GET, "/") => Ok(Response::new(Either::Left(Full::new(Bytes::from_static(
            b"POST an owned streaming body to /echo.\n",
        ))))),
        (&Method::POST, "/echo") => {
            // Incoming hands its original data and trailer frames directly to
            // the response writer. Either preserves each variant's recycling
            // route; recovered payloads are released after writes settle.
            Ok(Response::new(Either::Right(request.into_body())))
        }
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Either::Left(Full::new(Bytes::from_static(b"Not found\n"))))
            .unwrap()),
    }
}

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = karmaio::net::tcp::TcpListener::bind(address)?;
    println!("Listening on http://{address}");
    loop {
        let (stream, _) = listener.accept().await?;
        karmaio::runtime::spawn_local(async move {
            if let Err(error) = Builder::new().serve_tcp(stream, request_service_fn(echo)).run().await {
                eprintln!("Connection failed: {error:?}");
            }
        });
    }
}
