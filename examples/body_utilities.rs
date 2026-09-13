//! Native composition, Karmaio streams, and optional local body erasure.
use karmaio::io::Stream;
use std::{cell::Cell, collections::VecDeque, convert::Infallible};
use vakya::{
    HeaderMap, HeaderValue, Response,
    body::{BodyExt, BoxBody, Frame, Full, StreamBody},
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    karmaio::Runtime::new()?.block_on(async {
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
            .map_frame(|frame| {
                frame.map_data(|mut data| {
                    data.push(b'!');
                    data
                })
            })
            .with_trailers(trailers);
        let collected = body.collect(6).await?;
        assert_eq!(collected.as_ref(), b"hello!");
        assert_eq!(collected.trailers().unwrap()["x-complete"], "yes");
        assert_eq!(observed.get(), 5);

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
    })
}
