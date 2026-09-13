//! An application-owned server sends Early Hints before its final response.
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
        let service = service_fn(async |(_, mut context): (Request<Incoming>, RequestContext)| {
            let hints = Response::builder()
                .status(103)
                .header("link", "</style.css>; rel=preload; as=style")
                .body(())
                .expect("valid Early Hints");
            context.send_informational(hints).await?;
            Ok::<_, Error>(Response::new(Full::new(Bytes::from_static(
                b"Hello after Early Hints\n",
            ))))
        });
        Builder::new().serve_tcp(socket, service).run().await?;
        Ok(())
    })
}
