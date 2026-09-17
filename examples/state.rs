//! Share request-local state across connections with `Rc` and `Cell`.
//!
//! Vakya does not require `Send` or `Sync` on application state. Run with
//! `cargo run --example state --features server`.
use std::{cell::Cell, net::SocketAddr, rc::Rc};

use bytes::Bytes;
use vakya::{
    Request, Response,
    body::{Full, Incoming},
    error::Error,
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
};

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = karmaio::net::tcp::TcpListener::bind(addr)?;
    println!("Listening on http://{addr}");

    let counter = Rc::new(Cell::new(0usize));
    loop {
        let (socket, _) = listener.accept().await?;
        let counter = counter.clone();
        karmaio::runtime::spawn_local(async move {
            let service = service_fn(async move |(_req, _context): (Request<Incoming>, RequestContext)| {
                let count = counter.get();
                counter.set(count + 1);
                Ok::<_, Error>(Response::new(Full::new(Bytes::from(format!("Request #{count}")))))
            });
            if let Err(err) = Builder::new().serve_tcp(socket, service).run().await {
                eprintln!("Error serving connection: {err:?}");
            }
        });
    }
}
