//! A reverse proxy that forwards requests to another HTTP service.
//!
//! Run the [`hello`](hello.rs) example in another terminal, then:
//! `cargo run --example gateway --features "client,server"`.
use std::{io, net::SocketAddr};

use vakya::{
    Request, Response,
    body::Incoming,
    client::conn::http1::Builder as ClientBuilder,
    server::{RequestContext, conn::http1::Builder as ServerBuilder},
    service::service_fn,
};

async fn gateway(
    (mut req, _context): (Request<Incoming>, RequestContext),
    out_addr: SocketAddr,
) -> Result<Response<Incoming>, io::Error> {
    let uri_string = format!(
        "http://{out_addr}{}",
        req.uri().path_and_query().map(|path| path.as_str()).unwrap_or("/")
    );
    *req.uri_mut() = uri_string.parse().map_err(io::Error::other)?;

    let stream = karmaio::net::tcp::TcpStream::connect(out_addr).await?;
    let (sender, conn) = ClientBuilder::new().handshake_tcp::<Incoming>(stream);
    karmaio::runtime::spawn_local(async move {
        if let Err(err) = conn.run().await {
            eprintln!("Connection failed: {err:?}");
        }
    });

    sender
        .send_request(req)
        .await
        .map_err(|err| io::Error::other(err.to_string()))
}

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let in_addr = SocketAddr::from(([127, 0, 0, 1], 3001));
    let out_addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = karmaio::net::tcp::TcpListener::bind(in_addr)?;
    println!("Listening on http://{in_addr}");
    println!("Proxying on http://{out_addr}");

    loop {
        let (socket, _) = listener.accept().await?;
        karmaio::runtime::spawn_local(async move {
            let service = service_fn(async move |req| gateway(req, out_addr).await);
            if let Err(err) = ServerBuilder::new().serve_tcp(socket, service).run().await {
                eprintln!("Failed to serve the connection: {err:?}");
            }
        });
    }
}
