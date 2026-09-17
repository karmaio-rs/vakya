//! A simple server that returns "Hello World!".
//!
//! Run with `cargo run --example hello --features server`.
use std::{convert::Infallible, net::SocketAddr};

use bytes::Bytes;
use vakya::{
    Request, Response,
    body::{Full, Incoming},
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
};

async fn hello(_: (Request<Incoming>, RequestContext)) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(Response::new(Full::new(Bytes::from("Hello World!"))))
}

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = karmaio::net::tcp::TcpListener::bind(addr)?;
    println!("Listening on http://{addr}");

    loop {
        let (socket, _) = listener.accept().await?;
        karmaio::runtime::spawn_local(async move {
            if let Err(err) = Builder::new().serve_tcp(socket, service_fn(hello)).run().await {
                eprintln!("Error serving connection: {err:?}");
            }
        });
    }
}
