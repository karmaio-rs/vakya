//! Accept an HTTP protocol switch, then use the returned Karmaio transport.
use bytes::Bytes;
use karmaio::io::{AsyncWrite, AsyncWriteExt};
use vakya::{
    Empty, Incoming, Request, Response,
    connection::ConnectionOutcome,
    server::{RequestContext, conn::http1::Builder},
    service_fn,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    karmaio::Runtime::new()?.block_on(async {
        let listener = karmaio::net::tcp::TcpListener::bind("127.0.0.1:8080".parse::<std::net::SocketAddr>()?)?;
        eprintln!("Accepting one connection offering Upgrade: example/1");
        let (socket, _) = listener.accept().await?;
        let service = service_fn(async |(request, context): (Request<Incoming>, RequestContext)| {
            let response = if request
                .headers()
                .get("upgrade")
                .is_some_and(|value| value == "example/1")
            {
                Response::builder()
                    .status(101)
                    .header("connection", "upgrade")
                    .header("upgrade", "example/1")
                    .body(Empty::new())
                    .expect("valid upgrade response")
            } else {
                context.close_connection();
                Response::builder()
                    .status(400)
                    .body(Empty::new())
                    .expect("valid rejection")
            };
            Ok::<_, std::convert::Infallible>(response)
        });
        match Builder::new().serve_tcp(socket, service).run().await? {
            ConnectionOutcome::Closed => {}
            ConnectionOutcome::Upgraded(mut io) => {
                // These bytes are application protocol data, without HTTP framing.
                io.write_all(Bytes::from_static(b"Welcome to example/1\n"))
                    .await
                    .into_parts()
                    .0?;
                io.shutdown().await?;
            }
        }
        Ok(())
    })
}
