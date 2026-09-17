//! A JSON API plus a route that calls that API over a client connection.
//!
//! Run with `cargo run --example web_api --features "client,server"`.
use std::{convert::Infallible, io, net::SocketAddr};

use bytes::Bytes;
use vakya::{
    Method, Request, Response, StatusCode,
    body::{BodyExt, BoxBody, CollectError, Full, Incoming},
    client::conn::http1::Builder as ClientBuilder,
    error::Error,
    server::{RequestContext, conn::http1::Builder as ServerBuilder},
    service::service_fn,
};

static INDEX: &[u8] = b"<a href=\"test.html\">test.html</a>";
static INTERNAL_SERVER_ERROR: &[u8] = b"Internal Server Error";
static NOTFOUND: &[u8] = b"Not Found";
static POST_DATA: &str = r#"{"original": "data"}"#;
static URL: &str = "http://127.0.0.1:1337/json_api";

async fn client_request_response() -> Result<Response<BoxBody<'static, Bytes, Error>>, io::Error> {
    let req = Request::builder()
        .method(Method::POST)
        .uri(URL)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(POST_DATA)))
        .expect("uri/header parse from constants won't error");

    let stream = karmaio::net::tcp::TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], 1337))).await?;
    let (sender, conn) = ClientBuilder::new().handshake_tcp(stream);
    karmaio::runtime::spawn_local(async move {
        if let Err(err) = conn.run().await {
            eprintln!("Connection error: {err:?}");
        }
    });

    let web_res = sender
        .send_request(req)
        .await
        .map_err(|err| io::Error::other(err.to_string()))?;
    Ok(Response::new(map_bytes(web_res.into_body()).boxed()))
}

async fn api_post_response(req: Request<Incoming>) -> Result<Response<BoxBody<'static, Bytes, Error>>, io::Error> {
    let whole_body = collect_body(req).await?;
    let mut data: serde_json::Value =
        serde_json::from_slice(whole_body.as_ref()).map_err(|err| io::Error::other(err.to_string()))?;
    data["test"] = serde_json::Value::from("test_value");
    let json = serde_json::to_string(&data).map_err(|err| io::Error::other(err.to_string()))?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(full(json))
        .expect("header parse from constant won't error"))
}

fn api_get_response() -> Response<BoxBody<'static, Bytes, Error>> {
    let data = ["foo", "bar"];
    match serde_json::to_string(&data) {
        Ok(json) => Response::builder()
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(full(json))
            .expect("header parse from constant won't error"),
        Err(_) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(full(INTERNAL_SERVER_ERROR))
            .expect("constant status won't error"),
    }
}

async fn response_examples(
    (req, _context): (Request<Incoming>, RequestContext),
) -> Result<Response<BoxBody<'static, Bytes, Error>>, io::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") | (&Method::GET, "/index.html") => Ok(Response::new(full(INDEX))),
        (&Method::GET, "/test.html") => client_request_response().await,
        (&Method::POST, "/json_api") => api_post_response(req).await,
        (&Method::GET, "/json_api") => Ok(api_get_response()),
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(full(NOTFOUND))
            .expect("constant status won't error")),
    }
}

async fn collect_body(req: Request<Incoming>) -> Result<Bytes, io::Error> {
    match req.into_body().collect(64 * 1024).await {
        Ok(collected) => Ok(collected.bytes().clone()),
        Err(CollectError::LimitExceeded { .. }) => Err(io::Error::other("request body too large")),
        Err(error) => Err(io::Error::other(error.to_string())),
    }
}

fn map_bytes(body: Incoming) -> impl vakya::body::Body<Data = Bytes, Error = Error> {
    body.map_frame(|frame| frame.map_data(|data| Bytes::copy_from_slice(data.as_ref())))
}

fn full(chunk: impl Into<Bytes>) -> BoxBody<'static, Bytes, Error> {
    Full::new(chunk.into()).map_err(infallible).boxed()
}

fn infallible(never: Infallible) -> Error {
    match never {}
}

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 1337));
    let listener = karmaio::net::tcp::TcpListener::bind(addr)?;
    println!("Listening on http://{addr}");
    loop {
        let (socket, _) = listener.accept().await?;
        karmaio::runtime::spawn_local(async move {
            if let Err(err) = ServerBuilder::new()
                .serve_tcp(socket, service_fn(response_examples))
                .run()
                .await
            {
                eprintln!("Failed to serve connection: {err:?}");
            }
        });
    }
}
