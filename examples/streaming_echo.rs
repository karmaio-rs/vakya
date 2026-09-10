//! Forward the owned request body after the service call has finished.
use vakya::{
    Incoming, Request, Response,
    server::{RequestContext, conn::http1::Builder},
    service_fn,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    karmaio::Runtime::new()?.block_on(async {
        let listener = karmaio::net::tcp::TcpListener::bind("127.0.0.1:8080".parse::<std::net::SocketAddr>()?)?;
        eprintln!("Accepting one echo connection at http://127.0.0.1:8080");
        let (socket, _) = listener.accept().await?;
        let service = service_fn(async |(request, context): (Request<Incoming>, RequestContext)| {
            drop(context);
            Ok::<_, std::convert::Infallible>(Response::new(request.into_body()))
        });
        Builder::new().serve_tcp(socket, service).run().await?;
        Ok(())
    })
}
