//! The application accepts a socket and chooses how to drive its connection.
use bytes::Bytes;
use vakya::{
    Request, Response,
    body::{Full, Incoming},
    error::Error,
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    karmaio::Runtime::new()?.block_on(async {
        let listener = karmaio::net::tcp::TcpListener::bind("127.0.0.1:8080".parse::<std::net::SocketAddr>()?)?;
        eprintln!("Accepting one connection at http://127.0.0.1:8080");
        let (socket, _) = listener.accept().await?;
        let service = service_fn(async |(request, _context): (Request<Incoming>, RequestContext)| {
            // Opt into a bounded discard so an unread body can finish before reuse.
            request.into_body().drain(64 * 1024).await?;
            Ok::<_, Error>(Response::new(Full::new(Bytes::from_static(b"Hello from Vakya\n"))))
        });
        Builder::new().serve_tcp(socket, service).run().await?;
        Ok(())
    })
}
