//! Application-owned TLS configuration and dialing over an explicit socket address.
//!
//! Usage: `cargo run --example https_client --features "client,tls" -- <address:port> <server-name> <ca.der>`
use std::{
    io::{self, Write},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use vakya::{
    Request,
    body::{Body, Empty},
    client::conn::http1::Builder,
    tls::{HTTP_11_ALPN, rustls},
};

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: https_client <address:port> <server-name> <ca.der>";
    let address: SocketAddr = args.next().ok_or(usage)?.parse()?;
    let name = args.next().ok_or(usage)?;
    let ca = std::fs::read(args.next().ok_or(usage)?)?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(rustls::pki_types::CertificateDer::from(ca))?;
    // The application selects both its crypto provider and trust anchors.
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![HTTP_11_ALPN.to_vec()];
    let authority = if name.contains(':') {
        format!("[{name}]:{}", address.port())
    } else {
        format!("{name}:{}", address.port())
    };
    let peer_name = rustls::pki_types::ServerName::try_from(name)?;

    let socket = karmaio::net::tcp::TcpStream::connect(address).await?;
    let tls = karmaio::tls::TlsConnector::new(Arc::new(config))
        .connect(peer_name, socket)
        .await?;
    let (sender, connection) = Builder::new().handshake_tls::<_, Empty>(tls)?;
    let control = connection.control();
    let driver = karmaio::runtime::spawn_local(connection.run());
    let exchange = async {
        let mut response = sender
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", authority)
                    .body(Empty::new())?,
            )
            .await?;
        println!("Response: {}", response.status());
        println!("Headers: {:#?}\n", response.headers());

        let mut stdout = io::stdout();
        while let Some(frame) = response.body_mut().next_frame().await? {
            if let Some(chunk) = frame.data_ref() {
                stdout.write_all(chunk.as_ref())?;
            }
        }
        println!("\n\nDone!");
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    control.graceful_shutdown_with_deadline(Instant::now() + Duration::from_secs(5));
    drop(sender);
    let settled = driver.await?;
    exchange?;
    settled?;
    Ok(())
}
