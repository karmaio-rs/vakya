//! Application-owned certificates, accepting, and TLS establishment.
//!
//! Usage: `cargo run --example https_server --features "server,tls" -- <address:port> <certificate.der> <pkcs8-key.der>`
use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use vakya::{
    Request, Response,
    body::{Full, Incoming},
    error::Error,
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
    tls::{HTTP_11_ALPN, rustls},
};

async fn hello((request, _context): (Request<Incoming>, RequestContext)) -> Result<Response<Full<Bytes>>, Error> {
    request.into_body().drain(64 * 1024).await?;
    Ok(Response::new(Full::new(Bytes::from_static(b"Hello over TLS\n"))))
}

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: https_server <address:port> <certificate.der> <pkcs8-key.der>";
    let address: SocketAddr = args.next().ok_or(usage)?.parse()?;
    let certificate = rustls::pki_types::CertificateDer::from(std::fs::read(args.next().ok_or(usage)?)?);
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(std::fs::read(
        args.next().ok_or(usage)?,
    )?));
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)?;
    config.alpn_protocols = vec![HTTP_11_ALPN.to_vec()];

    let listener = karmaio::net::tcp::TcpListener::bind(address)?;
    let acceptor = karmaio::tls::TlsAcceptor::new(Arc::new(config));
    println!("Listening for TLS connections on {}", listener.local_addr()?);

    loop {
        let (socket, _) = listener.accept().await?;
        let acceptor = acceptor.clone();
        karmaio::runtime::spawn_local(async move {
            let tls = match acceptor.accept(socket).await {
                Ok(tls) => tls,
                Err(err) => {
                    eprintln!("TLS accept failed: {err:?}");
                    return;
                }
            };
            let conn = match Builder::new().serve_tls(tls, service_fn(hello)) {
                Ok(conn) => conn,
                Err(err) => {
                    eprintln!("HTTP setup failed: {err:?}");
                    return;
                }
            };
            if let Err(err) = conn.run().await {
                eprintln!("Error serving connection: {err:?}");
            }
        });
    }
}
