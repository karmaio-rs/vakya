//! Serve one application-selected file without path routing or filesystem policy.
//!
//! Run `cargo run --example file_server --features server,fs -- ./Cargo.toml`.
use std::{io, net::SocketAddr};
use vakya::{
    Request, Response,
    body::{FileBody, Incoming},
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
};

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os().nth(1).ok_or("provide a file to serve")?;
    let file = karmaio::fs::File::open(path).await?;
    let address = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = karmaio::net::tcp::TcpListener::bind(address)?;
    println!("Serving the supplied file at http://{address}");
    loop {
        let (stream, _) = listener.accept().await?;
        // Every response has its own metadata snapshot and positional cursor.
        // Cloning the supplied handle does not introduce a shared seek cursor.
        let file = file.clone();
        karmaio::runtime::spawn_local(async move {
            let service = service_fn(async |_: (Request<Incoming>, RequestContext)| {
                Ok::<_, io::Error>(Response::new(FileBody::new(file.clone()).await?))
            });
            if let Err(error) = Builder::new().serve_tcp(stream, service).run().await {
                eprintln!("Connection failed: {error:?}");
            }
        });
    }
}
