//! Accept an HTTP protocol switch through its request-scoped upgrade future.
use bytes::Bytes;
use karmaio::{
    io::{AsyncWrite, AsyncWriteExt},
    net::{
        split::{OwnedReadHalf, OwnedWriteHalf},
        tcp::TcpStream,
    },
};
use std::{cell::RefCell, rc::Rc};
use vakya::{
    Request, Response,
    body::{Empty, Incoming},
    connection::ConnectionOutcome,
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    karmaio::Runtime::new()?.block_on(async {
        let listener = karmaio::net::tcp::TcpListener::bind("127.0.0.1:8080".parse::<std::net::SocketAddr>()?)?;
        eprintln!("Accepting one connection offering Upgrade: example/1");
        let (socket, _) = listener.accept().await?;
        let upgrade = Rc::new(RefCell::new(None));
        let claimed = upgrade.clone();
        let service = service_fn(
            async move |(request, mut context): (Request<Incoming>, RequestContext)| {
                let response = if request
                    .headers()
                    .get("upgrade")
                    .is_some_and(|value| value == "example/1")
                {
                    claimed.replace(Some(
                        context.on_upgrade::<OwnedReadHalf<TcpStream>, OwnedWriteHalf<TcpStream>>(),
                    ));
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
            },
        );
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
            ConnectionOutcome::UpgradeClaimed(_) => {
                let upgrade = upgrade.borrow_mut().take().expect("accepted upgrade has a future");
                let mut io = upgrade.await?;
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
