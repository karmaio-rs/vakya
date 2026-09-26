//! Native body composition, Karmaio streams, and optional local erasure.
//!
//! Run with `cargo run --example body_utilities`.
use karmaio::io::Stream;
use std::{cell::Cell, collections::VecDeque, convert::Infallible};
use vakya::{
    HeaderMap, HeaderValue, Response,
    body::{Body, BodyExt, BoxBody, Frame, Full, ReaderBody, StreamBody, TrailerHint},
};

struct Chunks(VecDeque<Vec<u8>>);

impl Stream for Chunks {
    type Item = Result<Frame<Vec<u8>>, Infallible>;

    async fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front().map(|data| Ok(Frame::data(data)))
    }
}

// Erasure is an application choice. Data and error types remain fixed.
fn response_body(streaming: bool) -> BoxBody<'static, Vec<u8>, Infallible> {
    if streaming {
        StreamBody::new(Chunks(VecDeque::from([b"hello ".to_vec(), b"world".to_vec()]))).boxed()
    } else {
        Full::new(b"hello world".to_vec()).boxed()
    }
}

#[karmaio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observed = Cell::new(0);
    let mut trailers = HeaderMap::new();
    trailers.insert("x-complete", HeaderValue::from_static("yes"));

    // Static composition can borrow local state without a task or a box.
    let body = Full::new(b"hello".to_vec())
        .inspect_frame(|frame| {
            if let Some(data) = frame.data_ref() {
                observed.set(observed.get() + data.len());
            }
        })
        .map_data(|mut data| {
            data.push(b'!');
            data
        })
        .with_trailers(trailers);
    let collected = body.collect(6).await?;
    assert_eq!(collected.as_ref(), b"hello!");
    assert_eq!(collected.trailers().unwrap()["x-complete"], "yes");
    assert_eq!(observed.get(), 5);

    // A stream's exact length is checked through its actual terminal frame.
    let mut body = StreamBody::new(Chunks(VecDeque::from([b"hello ".to_vec(), b"world".to_vec()])))
        .with_exact_length(11, TrailerHint::None)
        .limit(11);
    assert_eq!(body.size_hint().exact(), Some(11));
    let mut bytes = 0;
    while let Some(frame) = body.next_frame().await? {
        if let Ok(data) = frame.into_data() {
            bytes += data.len();
            // The buffer is returned to its source while that source remains
            // available. No whole-body collection is needed for this limit.
            body.recycle(data);
        }
    }
    assert_eq!(bytes, 11);

    // Owned readers retain a submitted read if its frame wait is dropped.
    let mut reader = ReaderBody::with_chunk_size(std::io::Cursor::new(b"reader".to_vec()), 3)?;
    let first = reader.next_frame().await?.unwrap().into_data().unwrap();
    assert_eq!(&first[..], b"rea");
    reader.recycle(first);
    let collected = reader.collect(6).await?;
    assert_eq!(collected.as_ref(), b"der");

    for streaming in [false, true] {
        let response = Response::new(response_body(streaming));
        let mut frames = response.into_body().into_frame_stream();
        let mut length = 0;
        while let Some(frame) = frames.next().await {
            let frame = frame?;
            if let Some(data) = frame.data_ref() {
                length += data.len();
            }
            // Stream consumers own the payload; no recycle callback occurs.
        }
        assert_eq!(length, 11);
    }
    println!("Native body composition and stream examples completed.");
    Ok(())
}
