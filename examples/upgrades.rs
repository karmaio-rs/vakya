//! A server and client demonstrating HTTP upgrades.
//!
//! Run with `cargo run --example upgrades --features "client,server"`.
use std::{convert::Infallible, io, net::SocketAddr, str};

use bytes::Bytes;
use karmaio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{
        split::{OwnedReadHalf, OwnedWriteHalf},
        tcp::TcpStream,
    },
};
use vakya::{
    Request, Response, StatusCode,
    body::Empty,
    client::conn::http1::Builder as ClientBuilder,
    server::{RequestContext, conn::http1::Builder as ServerBuilder},
    service::service_fn,
    upgrade::Upgraded,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type TcpUpgraded = Upgraded<OwnedReadHalf<TcpStream>, OwnedWriteHalf<TcpStream>>;

async fn server_upgraded_io(mut upgraded: TcpUpgraded) -> io::Result<()> {
    let (result, buf) = upgraded.read_exact(vec![0; 7]).await.into_parts();
    result?;
    println!("server[foobar] recv: {:?}", str::from_utf8(&buf));

    let (result, _) = upgraded.write_all(Bytes::from_static(b"bar=foo")).await.into_parts();
    result?;
    println!("server[foobar] sent");
    Ok(())
}

async fn server_upgrade(
    (req, mut context): (Request<vakya::body::Incoming>, RequestContext),
) -> std::result::Result<Response<Empty>, Infallible> {
    let mut res = Response::new(Empty::new());
    if !req.headers().contains_key(http::header::UPGRADE) {
        *res.status_mut() = StatusCode::BAD_REQUEST;
        return Ok(res);
    }

    let upgrade = context.on_upgrade::<OwnedReadHalf<TcpStream>, OwnedWriteHalf<TcpStream>>();
    karmaio::runtime::spawn_local(async move {
        match upgrade.await {
            Ok(upgraded) => {
                if let Err(error) = server_upgraded_io(upgraded).await {
                    eprintln!("server foobar io error: {error}");
                }
            }
            Err(error) => eprintln!("upgrade error: {error}"),
        }
    });

    *res.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    res.headers_mut()
        .insert(http::header::CONNECTION, http::HeaderValue::from_static("upgrade"));
    res.headers_mut()
        .insert(http::header::UPGRADE, http::HeaderValue::from_static("foobar"));
    Ok(res)
}

async fn client_upgraded_io(mut upgraded: TcpUpgraded) -> io::Result<()> {
    let (result, _) = upgraded.write_all(Bytes::from_static(b"foo=bar")).await.into_parts();
    result?;
    println!("client[foobar] sent");

    let mut out = Vec::new();
    loop {
        let buf = vec![0u8; 64];
        let (result, buf) = upgraded.read(buf).await.into_parts();
        let n = result?;
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    println!("client[foobar] recv: {:?}", str::from_utf8(&out));
    Ok(())
}

async fn client_upgrade_request(addr: SocketAddr) -> Result<()> {
    let req = Request::builder()
        .uri("/")
        .header(http::header::HOST, addr.to_string())
        .header(http::header::CONNECTION, "upgrade")
        .header(http::header::UPGRADE, "foobar")
        .body(Empty::new())
        .expect("uri/header parse won't error");

    let stream = TcpStream::connect(addr).await?;
    let (sender, conn) = ClientBuilder::new().handshake_tcp::<Empty>(stream);
    let driver = karmaio::runtime::spawn_local(async move {
        if let Err(err) = conn.run().await {
            eprintln!("Connection failed: {err:?}");
        }
    });

    let (res, upgrade) = sender
        .send_request_with_upgrade::<OwnedReadHalf<TcpStream>, OwnedWriteHalf<TcpStream>>(req)
        .await?;
    if res.status() != StatusCode::SWITCHING_PROTOCOLS {
        panic!("Our server didn't upgrade: {}", res.status());
    }

    match upgrade.await {
        Ok(upgraded) => {
            if let Err(error) = client_upgraded_io(upgraded).await {
                eprintln!("client foobar io error: {error}");
            }
        }
        Err(error) => eprintln!("upgrade error: {error}"),
    }

    let _ = driver.await;
    Ok(())
}

#[karmaio::main]
async fn main() -> Result<()> {
    let listener = karmaio::net::tcp::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let addr = listener.local_addr()?;

    karmaio::runtime::spawn_local(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    eprintln!("Failed to accept: {error}");
                    break;
                }
            };
            karmaio::runtime::spawn_local(async move {
                if let Err(err) = ServerBuilder::new()
                    .serve_tcp(socket, service_fn(server_upgrade))
                    .run()
                    .await
                {
                    eprintln!("Error serving connection: {err:?}");
                }
            });
        }
    });

    if let Err(error) = client_upgrade_request(addr).await {
        eprintln!("client error: {error}");
    }
    Ok(())
}
