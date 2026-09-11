//! The application establishes transport and supervises its HTTP driver.
use vakya::{
    BodyExt, Empty, Request,
    client::{ResponseEvent, conn::http1::Builder},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    karmaio::Runtime::new()?.block_on(async {
        let socket = karmaio::net::tcp::TcpStream::connect("127.0.0.1:8080".parse::<std::net::SocketAddr>()?).await?;
        let (sender, connection) = Builder::new().handshake_tcp::<Empty>(socket);
        let control = connection.control();
        let driver = karmaio::runtime::spawn_local(connection.run());
        let exchange = async {
            let request = Request::builder()
                .uri("/")
                .header("host", "127.0.0.1:8080")
                .body(Empty::new())?;
            let mut pending = sender.start_request(request).await?;
            let response = loop {
                match pending.next_event().await? {
                    Some(ResponseEvent::Informational(head)) => println!("Informational: {}", head.status()),
                    Some(ResponseEvent::Final(response)) => break response,
                    None => return Err("missing final response".into()),
                }
            };
            println!("{}", response.status());
            let collected = response.into_body().collect(1024 * 1024).await?;
            println!("{}", String::from_utf8_lossy(collected.bytes()));
            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
        control.graceful_shutdown_with_deadline(std::time::Instant::now() + std::time::Duration::from_secs(5));
        drop(sender);
        // Observe both application and driver outcomes, including after an
        // exchange error. The application bounds shutdown and keeps driving it.
        let settled = driver.await?;
        exchange?;
        settled?;
        Ok(())
    })
}
