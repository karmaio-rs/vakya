//! A struct that implements `Service` and shares a counter across requests.
//!
//! Run with `cargo run --example service_struct_impl --features server`.
use std::{cell::Cell, convert::Infallible, net::SocketAddr, rc::Rc};

use bytes::Bytes;
use vakya::{
    Request, Response,
    body::{Full, Incoming},
    server::{RequestContext, conn::http1::Builder},
    service::Service,
};

struct Svc {
    counter: Cell<i32>,
}

impl Service<(Request<Incoming>, RequestContext)> for Svc {
    type Response = Response<Full<Bytes>>;
    type Error = Infallible;

    async fn call(&self, (req, _context): (Request<Incoming>, RequestContext)) -> Result<Self::Response, Self::Error> {
        fn mk_response(body: String) -> Result<Response<Full<Bytes>>, Infallible> {
            Ok(Response::new(Full::new(Bytes::from(body))))
        }

        if req.uri().path() != "/favicon.ico" {
            self.counter.set(self.counter.get() + 1);
        }

        match req.uri().path() {
            "/" => mk_response(format!("home! counter = {}", self.counter.get())),
            "/posts" => mk_response(format!("posts, of course! counter = {}", self.counter.get())),
            "/authors" => mk_response(format!("authors extraordinare! counter = {}", self.counter.get())),
            _ => mk_response("oh no! not found".into()),
        }
    }
}

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = karmaio::net::tcp::TcpListener::bind(addr)?;
    println!("Listening on http://{addr}");

    let svc = Rc::new(Svc { counter: Cell::new(0) });
    loop {
        let (socket, _) = listener.accept().await?;
        let svc = svc.clone();
        karmaio::runtime::spawn_local(async move {
            if let Err(err) = Builder::new().serve_tcp(socket, svc).run().await {
                eprintln!("Failed to serve connection: {err:?}");
            }
        });
    }
}
