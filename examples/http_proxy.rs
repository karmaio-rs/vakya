//! A simple HTTP proxy that forwards requests and tunnels `CONNECT`.
//!
//! 1. `cargo run --example http_proxy --features "client,server"`
//! 2. `export http_proxy=http://127.0.0.1:8100`
//! 3. `curl -i http://example.com/`
use std::{
    convert::Infallible,
    io,
    net::{SocketAddr, ToSocketAddrs},
};

use bytes::Bytes;
use karmaio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, IntoOwnedSplit},
    net::{
        split::{OwnedReadHalf, OwnedWriteHalf},
        tcp::TcpStream,
    },
};
use vakya::{
    Method, Request, Response, StatusCode,
    body::{BodyExt, BoxBody, Empty, Full, Incoming},
    client::conn::http1::Builder as ClientBuilder,
    error::Error,
    server::{RequestContext, conn::http1::Builder as ServerBuilder},
    service::service_fn,
    upgrade::Upgraded,
};

async fn proxy(
    (req, mut context): (Request<Incoming>, RequestContext),
) -> Result<Response<BoxBody<'static, Bytes, Error>>, Error> {
    println!("req: {req:?}");

    if req.method() == Method::CONNECT {
        let Some(addr) = host_addr(req.uri()) else {
            eprintln!("CONNECT host is not socket addr: {:?}", req.uri());
            let mut resp = Response::new(full("CONNECT must be to a socket address"));
            *resp.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(resp);
        };

        let upgrade = context.on_upgrade::<OwnedReadHalf<TcpStream>, OwnedWriteHalf<TcpStream>>();
        karmaio::runtime::spawn_local(async move {
            match upgrade.await {
                Ok(upgraded) => {
                    if let Err(error) = tunnel(upgraded, addr).await {
                        eprintln!("server io error: {error}");
                    }
                }
                Err(error) => eprintln!("upgrade error: {error}"),
            }
        });

        return Ok(Response::new(empty()));
    }

    let host = req.uri().host().ok_or_else(|| io::Error::other("uri has no host"))?;
    let port = req.uri().port_u16().unwrap_or(80);
    let addr = resolve(host, port)?;
    let stream = TcpStream::connect(addr).await?;
    let (sender, conn) = ClientBuilder::new()
        .preserve_header_case(true)
        .title_case_headers(true)
        .handshake_tcp::<Incoming>(stream);
    karmaio::runtime::spawn_local(async move {
        if let Err(err) = conn.run().await {
            eprintln!("Connection failed: {err:?}");
        }
    });

    let resp = sender
        .send_request(req)
        .await
        .map_err(|err| io::Error::other(err.to_string()))?;
    Ok(resp.map(|body| map_bytes(body).boxed()))
}

fn host_addr(uri: &http::Uri) -> Option<String> {
    uri.authority().map(|auth| auth.to_string())
}

fn resolve(host: &str, port: u16) -> io::Result<SocketAddr> {
    (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "host did not resolve to a socket address"))
}

fn map_bytes(body: Incoming) -> impl vakya::body::Body<Data = Bytes, Error = Error> {
    body.map_frame(|frame| frame.map_data(|data| Bytes::copy_from_slice(data.as_ref())))
}

fn empty() -> BoxBody<'static, Bytes, Error> {
    Empty::new().map_err(infallible).boxed()
}

fn full(chunk: impl Into<Bytes>) -> BoxBody<'static, Bytes, Error> {
    Full::new(chunk.into()).map_err(infallible).boxed()
}

fn infallible(never: Infallible) -> Error {
    match never {}
}

async fn tunnel(
    upgraded: Upgraded<OwnedReadHalf<TcpStream>, OwnedWriteHalf<TcpStream>>,
    addr: String,
) -> io::Result<()> {
    let dest = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "CONNECT target did not resolve"))?;
    let server = TcpStream::connect(dest).await?;
    let (client_read, client_write) = upgraded.into_split();
    let (server_read, server_write) = server.into_split();

    let client_to_server = karmaio::runtime::spawn_local(copy(client_read, server_write));
    let server_to_client = karmaio::runtime::spawn_local(copy(server_read, client_write));
    let from_client = client_to_server
        .await
        .map_err(|err| io::Error::other(err.to_string()))??;
    let from_server = server_to_client
        .await
        .map_err(|err| io::Error::other(err.to_string()))??;

    println!("client wrote {from_client} bytes and received {from_server} bytes");
    Ok(())
}

async fn copy<R, W>(mut reader: R, mut writer: W) -> io::Result<u64>
where
    R: AsyncRead,
    W: AsyncWrite,
{
    let mut total = 0u64;
    loop {
        let buf = vec![0u8; 8 * 1024];
        let (result, buf) = reader.read(buf).await.into_parts();
        let n = result?;
        if n == 0 {
            writer.shutdown().await?;
            return Ok(total);
        }
        total += n as u64;
        let (result, _) = writer.write_all(buf).await.into_parts();
        result?;
    }
}

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 8100));
    let listener = karmaio::net::tcp::TcpListener::bind(addr)?;
    println!("Listening on http://{addr}");

    loop {
        let (socket, _) = listener.accept().await?;
        karmaio::runtime::spawn_local(async move {
            if let Err(err) = ServerBuilder::new()
                .preserve_header_case(true)
                .title_case_headers(true)
                .serve_tcp(socket, service_fn(proxy))
                .run()
                .await
            {
                eprintln!("Failed to serve connection: {err:?}");
            }
        });
    }
}
