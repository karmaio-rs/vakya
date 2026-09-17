//! A server that times out idle connections and shuts them down gracefully.
//!
//! Run with `cargo run --example graceful_shutdown --features server`.
use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::pin,
    task::Poll,
    time::{Duration, Instant},
};

use bytes::Bytes;
use vakya::{
    Request, Response,
    body::{Full, Incoming},
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
};

async fn hello(_: (Request<Incoming>, RequestContext)) -> Result<Response<Full<Bytes>>, Infallible> {
    println!("in hello before sleep");
    karmaio::time::sleep(Duration::from_secs(6)).await;
    println!("in hello after sleep");
    Ok(Response::new(Full::new(Bytes::from("Hello World!"))))
}

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = karmaio::net::tcp::TcpListener::bind(addr)?;
    println!("Listening on http://{addr}");

    loop {
        let (socket, remote_address) = listener.accept().await?;
        println!("accepted connection from {remote_address:?}");
        karmaio::runtime::spawn_local(async move {
            if let Err(err) = serve(socket).await {
                eprintln!("error serving connection: {err:?}");
            }
        });
    }
}

async fn serve(socket: karmaio::net::tcp::TcpStream) -> Result<(), vakya::error::Error> {
    let conn = Builder::new().serve_tcp(socket, service_fn(hello));
    let control = conn.control();
    let mut run = pin!(conn.run());
    let timeouts = [Duration::from_secs(5), Duration::from_secs(2)];

    for (iter, sleep_duration) in timeouts.iter().enumerate() {
        println!("iter = {iter} sleep_duration = {sleep_duration:?}");
        let mut sleep = pin!(karmaio::time::sleep(*sleep_duration));
        let finished = std::future::poll_fn(|cx| {
            if let Poll::Ready(result) = run.as_mut().poll(cx) {
                return Poll::Ready(Some(result));
            }
            match sleep.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        })
        .await;

        match finished {
            Some(result) => {
                match result {
                    Ok(_) => println!("after polling conn, no error"),
                    Err(error) => println!("error serving connection: {error:?}"),
                }
                return Ok(());
            }
            None => {
                println!("iter = {iter} got timeout_interval, calling conn.graceful_shutdown");
                if iter == 0 {
                    control.graceful_shutdown();
                } else {
                    control.graceful_shutdown_with_deadline(Instant::now());
                }
            }
        }
    }

    match run.await {
        Ok(_) => println!("after polling conn, no error"),
        Err(error) => println!("error serving connection: {error:?}"),
    }
    Ok(())
}
