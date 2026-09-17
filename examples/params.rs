//! Accept a form with a name and a number, then validate the fields.
//!
//! Run with `cargo run --example params --features server`.
use std::{collections::HashMap, convert::Infallible, io, net::SocketAddr};

use bytes::Bytes;
use vakya::{
    Method, Request, Response, StatusCode,
    body::{BodyExt, BoxBody, CollectError, Empty, Full, Incoming},
    error::Error,
    server::{RequestContext, conn::http1::Builder},
    service::service_fn,
};

static INDEX: &[u8] = b"<html><body><form action=\"post\" method=\"post\">Name: <input type=\"text\" name=\"name\"><br>Number: <input type=\"text\" name=\"number\"><br><input type=\"submit\"></body></html>";
static MISSING: &[u8] = b"Missing field";
static NOTNUMERIC: &[u8] = b"Number field is not numeric";

async fn param_example(
    (req, _context): (Request<Incoming>, RequestContext),
) -> Result<Response<BoxBody<'static, Bytes, Error>>, Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") | (&Method::GET, "/post") => Ok(Response::new(full(INDEX))),
        (&Method::POST, "/post") => {
            let body = collect_body(req).await?;
            let params = form_urlencoded::parse(body.as_ref())
                .into_owned()
                .collect::<HashMap<String, String>>();

            let Some(name) = params.get("name") else {
                return Ok(unprocessable(MISSING));
            };
            let Some(number) = params.get("number") else {
                return Ok(unprocessable(MISSING));
            };
            let Ok(number) = number.parse::<f64>() else {
                return Ok(unprocessable(NOTNUMERIC));
            };

            let body = format!("Hello {name}, your number is {number}");
            Ok(Response::new(full(body)))
        }
        (&Method::GET, "/get") => {
            let Some(query) = req.uri().query() else {
                return Ok(unprocessable(MISSING));
            };
            let params = form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect::<HashMap<String, String>>();
            let Some(page) = params.get("page") else {
                return Ok(unprocessable(MISSING));
            };
            Ok(Response::new(full(format!("You requested {page}"))))
        }
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(empty())
            .expect("constant status won't error")),
    }
}

async fn collect_body(req: Request<Incoming>) -> Result<Bytes, Error> {
    match req.into_body().collect(64 * 1024).await {
        Ok(collected) => Ok(collected.bytes().clone()),
        Err(CollectError::Body(error)) => Err(error),
        Err(CollectError::LimitExceeded { .. }) => Err(io::Error::other("request body too large").into()),
        Err(error) => Err(io::Error::other(error.to_string()).into()),
    }
}

fn unprocessable(chunk: &'static [u8]) -> Response<BoxBody<'static, Bytes, Error>> {
    Response::builder()
        .status(StatusCode::UNPROCESSABLE_ENTITY)
        .body(full(chunk))
        .expect("constant status won't error")
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

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 1337));
    let listener = karmaio::net::tcp::TcpListener::bind(addr)?;
    println!("Listening on http://{addr}");
    loop {
        let (socket, _) = listener.accept().await?;
        karmaio::runtime::spawn_local(async move {
            if let Err(err) = Builder::new().serve_tcp(socket, service_fn(param_example)).run().await {
                eprintln!("Error serving connection: {err:?}");
            }
        });
    }
}
