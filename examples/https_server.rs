//! Application-owned certificates, accepting, and TLS establishment.
//! Usage: https_server <address:port> <certificate.der> <pkcs8-key.der>
use bytes::Bytes;
use std::sync::Arc;
use vakya::{
    Request, Response,
    body::{Full, Incoming},
    error::Error,
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
    tls::{HTTP_11_ALPN, rustls},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: https_server <address:port> <certificate.der> <pkcs8-key.der>";
    let address: std::net::SocketAddr = args.next().ok_or(usage)?.parse()?;
    let certificate = rustls::pki_types::CertificateDer::from(std::fs::read(args.next().ok_or(usage)?)?);
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(std::fs::read(
        args.next().ok_or(usage)?,
    )?));
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)?;
    config.alpn_protocols = vec![HTTP_11_ALPN.to_vec()];
    karmaio::Runtime::new()?.block_on(async {
        let listener = karmaio::net::tcp::TcpListener::bind(address)?;
        eprintln!("Accepting one TLS connection on {}", listener.local_addr()?);
        let (socket, _) = listener.accept().await?;
        let tls = karmaio::tls::TlsAcceptor::new(Arc::new(config)).accept(socket).await?;
        let service = service_fn(async |(request, _): (Request<Incoming>, RequestContext)| {
            request.into_body().drain(64 * 1024).await?;
            Ok::<_, Error>(Response::new(Full::new(Bytes::from_static(b"Hello over TLS\n"))))
        });
        Builder::new().serve_tls(tls, service)?.run().await?;
        Ok(())
    })
}
