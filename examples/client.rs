//! A simple CLI HTTP client that prints the response status, headers, and body.
//!
//! Run with `cargo run --example client --features client -- http://127.0.0.1:3000/`.
use std::{
    env,
    io::{self, Write},
    net::{SocketAddr, ToSocketAddrs},
};

use vakya::{
    Request, Response,
    body::{Body, BodyExt, Empty, Incoming},
    client::conn::http1::Builder,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[karmaio::main]
async fn main() -> Result<()> {
    let url = match env::args().nth(1) {
        Some(url) => url,
        None => {
            println!("Usage: client <url>");
            return Ok(());
        }
    };

    let url = url.parse::<vakya::Uri>()?;
    if url.scheme_str() != Some("http") {
        println!("This example only works with 'http' URLs.");
        return Ok(());
    }

    fetch_url(url).await
}

async fn fetch_url(url: vakya::Uri) -> Result<()> {
    let host = url.host().ok_or("uri has no host")?;
    let port = url.port_u16().unwrap_or(80);
    let addr = resolve(host, port)?;
    let stream = karmaio::net::tcp::TcpStream::connect(addr).await?;

    let (sender, conn) = Builder::new().handshake_tcp::<Empty>(stream);
    let driver = karmaio::runtime::spawn_local(async move { conn.run().await });

    let authority = url.authority().ok_or("uri has no authority")?.clone();
    let path = url.path_and_query().map(|path| path.as_str()).unwrap_or("/");
    let req = Request::builder()
        .uri(path)
        .header(http::header::HOST, authority.as_str())
        .body(Empty::new())?;

    let mut pending = sender.start_request(req).await?;
    let completion = pending.take_completion().unwrap();
    // This CLI makes one request. Dropping the last sender asks the driver to
    // finish accepted work and close when the exchange settles.
    drop(sender);
    let application: Result<()> = async { print_response(pending.response().await?).await }.await;
    // Even a limit or stdout failure drops the owned body before awaiting the
    // receipt, allowing the driver to settle abandonment and shutdown.
    let settled = completion.await;
    let driven = driver.await?;
    application?;
    let outcome = settled?;
    driven?;
    println!("\n\nExchange completed: {outcome:?}");
    Ok(())
}

async fn print_response(res: Response<Incoming>) -> Result<()> {
    println!("Response: {}", res.status());
    println!("Headers: {:#?}\n", res.headers());

    let mut body = res.into_body().limit(8 * 1024 * 1024);
    let mut stdout = io::stdout();
    while let Some(frame) = body.next_frame().await? {
        if let Ok(chunk) = frame.into_data() {
            stdout.write_all(chunk.as_ref())?;
            body.recycle(chunk);
        }
    }
    Ok(())
}

fn resolve(host: &str, port: u16) -> io::Result<SocketAddr> {
    (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "host did not resolve to a socket address"))
}
