//! Send HTTP 103 Early Hints before the final response.
//!
//! Run with `cargo run --example informational --features server`.
use std::net::SocketAddr;

use bytes::Bytes;
use vakya::{
    Request, Response,
    body::{Full, Incoming},
    error::Error,
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
};

async fn early_hints(
    (_request, mut context): (Request<Incoming>, RequestContext),
) -> Result<Response<Full<Bytes>>, Error> {
    let hints = Response::builder()
        .status(103)
        .header("link", "</style.css>; rel=preload; as=style")
        .body(())
        .expect("valid Early Hints");
    context.send_informational(hints).await?;
    Ok(Response::new(Full::new(Bytes::from_static(
        b"Hello after Early Hints\n",
    ))))
}

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = karmaio::net::tcp::TcpListener::bind(addr)?;
    println!("Listening on http://{addr}");

    loop {
        let (socket, _) = listener.accept().await?;
        karmaio::runtime::spawn_local(async move {
            if let Err(err) = Builder::new().serve_tcp(socket, service_fn(early_hints)).run().await {
                eprintln!("Error serving connection: {err:?}");
            }
        });
    }
}
