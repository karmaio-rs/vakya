//! A simple CLI HTTP client that prints the response status, headers, and body.
//!
//! Run with `cargo run --example client --features client -- http://127.0.0.1:3000/`.
use std::{
    env,
    io::{self, Write},
    net::{SocketAddr, ToSocketAddrs},
};

use vakya::{
    Request,
    body::{Body, Empty},
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
    karmaio::runtime::spawn_local(async move {
        if let Err(err) = conn.run().await {
            eprintln!("Connection failed: {err:?}");
        }
    });

    let authority = url.authority().ok_or("uri has no authority")?.clone();
    let path = url.path_and_query().map(|path| path.as_str()).unwrap_or("/");
    let req = Request::builder()
        .uri(path)
        .header(http::header::HOST, authority.as_str())
        .body(Empty::new())?;

    let mut res = sender.send_request(req).await?;

    println!("Response: {}", res.status());
    println!("Headers: {:#?}\n", res.headers());

    let mut stdout = io::stdout();
    while let Some(frame) = res.body_mut().next_frame().await? {
        if let Some(chunk) = frame.data_ref() {
            stdout.write_all(chunk.as_ref())?;
        }
    }

    println!("\n\nDone!");
    Ok(())
}

fn resolve(host: &str, port: u16) -> io::Result<SocketAddr> {
    (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "host did not resolve to a socket address"))
}
