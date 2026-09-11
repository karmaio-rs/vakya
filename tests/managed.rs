#![cfg(all(target_os = "linux", feature = "client"))]
use karmaio::{RuntimeBuilder, net::tcp::TcpStream, runtime::spawn_local};
use std::io::{Read, Write};
use vakya::{Body, Empty, Request, client::conn::http1::Builder, connection::ConnectionOutcome};

#[test]
fn retained_response_views_survive_reuse_closure_and_runtime_teardown() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let peer = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        for payload in [b"first", b"later"] {
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                socket.read_exact(&mut byte).unwrap();
                head.push(byte[0]);
            }
            let response = [b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\n".as_slice(), payload].concat();
            socket.write_all(&response).unwrap();
        }
    });
    let retained = {
        let mut runtime = RuntimeBuilder::new()
            .buffer_pool_size(1)
            .buffer_pool_buffer_len(4096)
            .build()
            .unwrap();
        runtime.block_on(async {
            let stream = TcpStream::connect(address).await.unwrap();
            let (sender, connection) = Builder::new().handshake_tcp::<Empty>(stream);
            let control = connection.control();
            let driver = spawn_local(connection.run());
            let mut retained = Vec::new();
            for expected in [b"first", b"later"] {
                let request = Request::builder()
                    .uri("/")
                    .header("host", "test")
                    .body(Empty::new())
                    .unwrap();
                let mut response = sender.send_request(request).await.unwrap();
                let mut received = Vec::new();
                while let Some(frame) = response.body_mut().next_frame().await.unwrap() {
                    let data = frame.into_data().unwrap();
                    received.extend_from_slice(data.as_ref());
                    let clone = data.clone();
                    retained.push(clone);
                    drop(data);
                }
                assert_eq!(received, expected);
            }
            control.graceful_shutdown();
            drop(sender);
            assert!(matches!(driver.await.unwrap().unwrap(), ConnectionOutcome::Closed));
            retained
        })
    };
    peer.join().unwrap();
    let bytes: Vec<u8> = retained.iter().flat_map(|data| data.as_ref().iter().copied()).collect();
    assert_eq!(bytes, b"firstlater");
}
