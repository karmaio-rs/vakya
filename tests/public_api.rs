use karmaio::io::Stream;
use std::{cell::Cell, collections::VecDeque, rc::Rc};
use vakya::{
    HeaderMap, HeaderValue,
    body::{Body, BodyExt, CollectError, Frame, Full, SizeHint, StreamBody, TrailerHint},
};

struct Frames(VecDeque<Result<Frame<Vec<u8>>, &'static str>>);
impl Stream for Frames {
    type Item = Result<Frame<Vec<u8>>, &'static str>;
    async fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front()
    }
}

struct Reusing<'a> {
    full: Full<Vec<u8>>,
    recycled: &'a Cell<usize>,
}
impl Body for Reusing<'_> {
    type Data = Vec<u8>;
    type Error = std::convert::Infallible;
    async fn next_frame(&mut self) -> Result<Option<Frame<Vec<u8>>>, Self::Error> {
        self.full.next_frame().await
    }
    fn size_hint(&self) -> SizeHint {
        self.full.size_hint()
    }
    fn trailer_hint(&self) -> TrailerHint {
        self.full.trailer_hint()
    }
    fn is_end_stream(&self) -> bool {
        self.full.is_end_stream()
    }
    fn recycle(&mut self, _: Vec<u8>) {
        self.recycled.set(self.recycled.get() + 1);
    }
}

#[test]
fn observation_preserves_recycling_and_transformation_relinquishes_it() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let recycled = Cell::new(0);
        let observed = Rc::new(Cell::new(0));
        let mut body = Reusing {
            full: Full::new(vec![1, 2]),
            recycled: &recycled,
        }
        .inspect_frame(|frame| observed.set(observed.get() + frame.data_ref().unwrap().len()))
        .map_err(|error| error);
        assert_eq!(body.size_hint().exact(), Some(2));
        assert_eq!(body.trailer_hint(), TrailerHint::None);
        assert_eq!(BodyExt::collect(&mut body, 2).await.unwrap().as_ref(), &[1, 2]);
        assert_eq!(observed.get(), 2);
        assert_eq!(recycled.get(), 1);
        assert!(body.is_end_stream());

        let body = Reusing {
            full: Full::new(vec![3]),
            recycled: &recycled,
        }
        .map_frame(|frame| {
            frame.map_data(|mut data| {
                data.extend_from_slice(&[4, 5]);
                data
            })
        });
        assert_eq!(body.size_hint(), SizeHint::new());
        assert_eq!(body.trailer_hint(), TrailerHint::MayHave);
        assert_eq!(body.collect(3).await.unwrap().as_ref(), &[3, 4, 5]);
        assert_eq!(recycled.get(), 1);
    });
}

#[test]
fn attached_trailers_preserve_duplicate_values_in_a_single_frame() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let mut original = HeaderMap::new();
        original.append("x-check", HeaderValue::from_static("first"));
        let mut extra = HeaderMap::new();
        extra.append("x-check", HeaderValue::from_static("second"));
        extra.append("x-check", HeaderValue::from_static("third"));
        let body = StreamBody::new(Frames(VecDeque::from([Ok(Frame::trailers(original))]))).with_trailers(extra);
        let collected = body.collect(0).await.unwrap();
        let values: Vec<_> = collected.trailers().unwrap().get_all("x-check").iter().collect();
        assert_eq!(values, ["first", "second", "third"]);
    });
}

#[test]
fn pending_trailers_prevent_early_termination_and_do_not_hide_source_errors() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let mut body = vakya::body::Empty::new().with_trailers(HeaderMap::new());
        assert_eq!(body.size_hint().exact(), Some(0));
        assert_eq!(body.trailer_hint(), TrailerHint::MayHave);
        assert!(!body.is_end_stream());
        assert!(body.next_frame().await.unwrap().unwrap().trailers_ref().is_some());
        assert!(body.is_end_stream());
        assert!(body.next_frame().await.unwrap().is_none());

        for frames in [
            VecDeque::from([Err("failed")]),
            VecDeque::from([Ok(Frame::trailers(HeaderMap::new())), Err("failed")]),
        ] {
            let body = StreamBody::new(Frames(frames)).with_trailers(HeaderMap::new());
            assert!(matches!(body.collect(0).await, Err(CollectError::Body("failed"))));
        }
    });
}

#[test]
fn boxed_borrowed_producer_preserves_pending_progress_cancellation_and_recycling() {
    use std::{
        future::{Future, poll_fn},
        pin::pin,
        task::{Context, Poll, Waker},
    };

    struct CallGuard<'a>(&'a Cell<usize>);
    impl Drop for CallGuard<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }
    struct PendingBody<'a> {
        inner: Reusing<'a>,
        ready: &'a Cell<bool>,
        finished_calls: &'a Cell<usize>,
    }
    impl Body for PendingBody<'_> {
        type Data = Vec<u8>;
        type Error = std::convert::Infallible;
        async fn next_frame(&mut self) -> Result<Option<Frame<Vec<u8>>>, Self::Error> {
            let _guard = CallGuard(self.finished_calls);
            poll_fn(|cx| {
                if self.ready.get() {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
            self.inner.next_frame().await
        }
        fn size_hint(&self) -> SizeHint {
            self.inner.size_hint()
        }
        fn trailer_hint(&self) -> TrailerHint {
            self.inner.trailer_hint()
        }
        fn is_end_stream(&self) -> bool {
            self.inner.is_end_stream()
        }
        fn recycle(&mut self, data: Vec<u8>) {
            self.inner.recycle(data);
        }
    }

    let recycled = Cell::new(0);
    let finished = Cell::new(0);
    let ready = Cell::new(false);
    let mut body = PendingBody {
        inner: Reusing {
            full: Full::new(vec![7, 8]),
            recycled: &recycled,
        },
        ready: &ready,
        finished_calls: &finished,
    }
    .boxed();
    assert_eq!(body.size_hint().exact(), Some(2));
    assert_eq!(body.trailer_hint(), TrailerHint::None);
    let mut cx = Context::from_waker(Waker::noop());
    {
        let mut future = pin!(body.next_frame());
        assert!(future.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(finished.get(), 1);
    assert_eq!(recycled.get(), 0);
    // This producer retains its data on cancellation; the contract does not
    // require all producers to be resumable.
    let frame = {
        let mut future = pin!(body.next_frame());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        ready.set(true);
        let Poll::Ready(Ok(Some(frame))) = future.as_mut().poll(&mut cx) else {
            panic!("pending call did not retain its progress");
        };
        frame
    };
    assert_eq!(finished.get(), 2);
    let data = frame.into_data().unwrap();
    assert_eq!(data, [7, 8]);
    body.recycle(data);
    assert_eq!(recycled.get(), 1);
    assert!(body.is_end_stream());
    assert_eq!(body.size_hint().exact(), Some(0));
}

#[test]
fn boxed_mapped_errors_retain_the_original_local_source() {
    #[derive(Debug)]
    struct LocalError(Rc<str>);
    impl std::fmt::Display for LocalError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.0)
        }
    }
    impl std::error::Error for LocalError {}

    karmaio::Runtime::new().unwrap().block_on(async {
        let message: Rc<str> = Rc::from("local failure");
        let body = StreamBody::new(Frames(VecDeque::from([Err("source error")])))
            .map_err(|_| LocalError(Rc::clone(&message)))
            .boxed();
        let error = body.collect(0).await.unwrap_err();
        let source = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<LocalError>()
            .unwrap();
        assert!(Rc::ptr_eq(&source.0, &message));
    });
}
