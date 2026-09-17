//! GET JSON, collect the body, and parse it with serde.
//!
//! Run with `cargo run --example client_json --features client`.
use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
};

use serde::Deserialize;
use vakya::{
    Request,
    body::{BodyExt, Empty},
    client::conn::http1::Builder,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Deserialize, Debug)]
struct User {
    id: i32,
    #[allow(unused)]
    name: String,
}

#[karmaio::main]
async fn main() -> Result<()> {
    let url = "http://jsonplaceholder.typicode.com/users".parse()?;
    let users = fetch_json(url).await?;
    println!("users: {users:#?}");
    let sum = users.iter().fold(0, |acc, user| acc + user.id);
    println!("sum of ids: {sum}");
    Ok(())
}

async fn fetch_json(url: vakya::Uri) -> Result<Vec<User>> {
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
    let req = Request::builder()
        .uri(url)
        .header(http::header::HOST, authority.as_str())
        .body(Empty::new())?;

    let res = sender.send_request(req).await?;
    let body = res.into_body().collect(1024 * 1024).await?;
    Ok(serde_json::from_slice(body.as_ref())?)
}

fn resolve(host: &str, port: u16) -> io::Result<SocketAddr> {
    (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "host did not resolve to a socket address"))
}
