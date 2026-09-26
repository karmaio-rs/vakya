use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::Rc,
};
use vakya::{
    HeaderMap,
    body::{Body, BodyExt, ExactLengthError, Frame, SizeHint, TrailerHint},
};

#[derive(Default)]
struct State {
    polls: Cell<usize>,
    drops: Cell<usize>,
    reclaimed: RefCell<Vec<Vec<u8>>>,
    events: RefCell<Vec<&'static str>>,
}
struct Source {
    frames: VecDeque<Result<Frame<Vec<u8>>, &'static str>>,
    state: Rc<State>,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.state.drops.set(self.state.drops.get() + 1);
        self.state.events.borrow_mut().push("drop");
    }
}
impl Body for Source {
    type Data = Vec<u8>;
    type Error = &'static str;
    async fn next_frame(&mut self) -> Result<Option<Frame<Vec<u8>>>, Self::Error> {
        self.state.polls.set(self.state.polls.get() + 1);
        self.frames.pop_front().transpose()
    }
    fn recycle(&mut self, data: Vec<u8>) {
        self.state.events.borrow_mut().push("recycle");
        self.state.reclaimed.borrow_mut().push(data);
    }
}
fn source(frames: impl IntoIterator<Item = Result<Frame<Vec<u8>>, &'static str>>) -> (Source, Rc<State>) {
    let state = Rc::new(State::default());
    (
        Source {
            frames: frames.into_iter().collect(),
            state: state.clone(),
        },
        state,
    )
}
fn data(bytes: &[u8]) -> Result<Frame<Vec<u8>>, &'static str> {
    Ok(Frame::data(bytes.to_vec()))
}
fn trailers() -> Result<Frame<Vec<u8>>, &'static str> {
    Ok(Frame::trailers(HeaderMap::new()))
}

#[test]
fn exact_length_forwards_owned_frames_recycling_and_independent_trailers() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let bytes = b"abc".to_vec();
        let pointer = bytes.as_ptr();
        let (source, state) = source([Ok(Frame::data(bytes)), data(b""), trailers()]);
        let mut body = source.with_exact_length(3, TrailerHint::MayHave);
        assert_eq!(body.size_hint().exact(), Some(3));
        let bytes = body.next_frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(bytes.as_ptr(), pointer);
        assert_eq!(body.size_hint().exact(), Some(0));
        assert!(!body.is_end_stream());
        body.recycle(bytes);
        let empty = body.next_frame().await.unwrap().unwrap().into_data().unwrap();
        body.recycle(empty);
        assert!(body.next_frame().await.unwrap().unwrap().trailers_ref().is_some());
        assert_eq!(body.trailer_hint(), TrailerHint::None);
        assert!(!body.is_end_stream());
        assert!(body.next_frame().await.unwrap().is_none());
        assert!(body.is_end_stream());
        let polls = state.polls.get();
        assert!(body.next_frame().await.unwrap().is_none());
        assert_eq!(state.polls.get(), polls);
        assert_eq!(state.reclaimed.borrow().len(), 2);
    });
}

#[test]
fn exact_zero_observes_empty_frames_trailers_and_source_failure() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let (source, _) = source([data(b""), trailers(), Err("origin")]);
        let mut body = source.with_exact_length(0, TrailerHint::MayHave);
        assert!(!body.is_end_stream());
        assert!(body.next_frame().await.unwrap().unwrap().data_ref().unwrap().is_empty());
        assert!(body.next_frame().await.unwrap().unwrap().trailers_ref().is_some());
        assert_eq!(body.next_frame().await.unwrap_err(), ExactLengthError::Source("origin"));
        assert!(body.is_end_stream());
        assert!(body.next_frame().await.unwrap().is_none());
    });
}

#[test]
fn exact_length_violations_reclaim_before_abandoning_and_remain_terminal() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for (frames, bytes, hint, expected, reclaim) in [
            (
                vec![data(b"ab")],
                1,
                TrailerHint::None,
                ExactLengthError::TooLong {
                    remaining: 1,
                    frame_bytes: 2,
                },
                1,
            ),
            (
                vec![],
                1,
                TrailerHint::None,
                ExactLengthError::TooShort { remaining: 1 },
                0,
            ),
            (
                vec![trailers()],
                0,
                TrailerHint::None,
                ExactLengthError::UnexpectedTrailers,
                0,
            ),
            (
                vec![trailers(), trailers()],
                0,
                TrailerHint::MayHave,
                ExactLengthError::RepeatedTrailers,
                0,
            ),
            (
                vec![trailers(), data(b"")],
                0,
                TrailerHint::MayHave,
                ExactLengthError::DataAfterTrailers,
                1,
            ),
        ] {
            let (source, state) = source(frames);
            let mut body = source.with_exact_length(bytes, hint);
            let error = loop {
                match body.next_frame().await {
                    Ok(Some(_)) => {}
                    result => break result.unwrap_err(),
                }
            };
            assert_eq!(error, expected);
            assert_eq!(state.reclaimed.borrow().len(), reclaim);
            assert_eq!(state.drops.get(), 1);
            if reclaim == 1 {
                assert_eq!(&*state.events.borrow(), &["recycle", "drop"]);
            }
            assert_eq!(body.size_hint(), SizeHint::with_exact(0));
            assert_eq!(body.trailer_hint(), TrailerHint::None);
            assert!(body.is_end_stream());
            let polls = state.polls.get();
            assert!(body.next_frame().await.unwrap().is_none());
            assert_eq!(state.polls.get(), polls);
        }
    });
}

#[test]
fn streaming_limit_rejects_whole_overflowing_frame_before_dropping_source() {
    use vakya::body::LimitError;
    karmaio::Runtime::new().unwrap().block_on(async {
        let (source, state) = source([data(b"a"), data(b"bc"), data(b"unread")]);
        let mut body = source.limit(2);
        assert_eq!(body.size_hint().upper(), Some(2));
        let first = body.next_frame().await.unwrap().unwrap().into_data().unwrap();
        body.recycle(first);
        assert_eq!(body.size_hint().upper(), Some(1));
        assert_eq!(
            body.next_frame().await.unwrap_err(),
            LimitError::LimitExceeded { limit: 2 }
        );
        assert_eq!(&*state.reclaimed.borrow(), &[b"a".to_vec(), b"bc".to_vec()]);
        assert_eq!(&*state.events.borrow(), &["recycle", "recycle", "drop"]);
        assert_eq!(state.polls.get(), 2);
        assert!(body.is_end_stream());
        assert!(body.next_frame().await.unwrap().is_none());
        assert_eq!(state.polls.get(), 2);
    });
}

#[test]
fn streaming_limit_zero_and_exact_boundary_permit_empty_data_and_trailers() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for bytes in [b"".as_slice(), b"abc".as_slice()] {
            let (source, state) = source([data(bytes), data(b""), trailers()]);
            let mut body = source.limit(bytes.len() as u64);
            let first = body.next_frame().await.unwrap().unwrap().into_data().unwrap();
            body.recycle(first);
            assert!(!body.is_end_stream());
            assert_eq!(body.trailer_hint(), TrailerHint::MayHave);
            assert!(body.next_frame().await.unwrap().unwrap().data_ref().unwrap().is_empty());
            assert!(body.next_frame().await.unwrap().unwrap().trailers_ref().is_some());
            assert!(body.next_frame().await.unwrap().is_none());
            assert_eq!(state.reclaimed.borrow().len(), 1);
            assert_eq!(body.size_hint().exact(), Some(0));
        }
    });
}

#[test]
fn streaming_limit_preserves_source_failure_and_conservative_metadata() {
    use vakya::body::{Full, LimitError};
    karmaio::Runtime::new().unwrap().block_on(async {
        assert_eq!(Full::new(b"abc".to_vec()).limit(4).size_hint().exact(), Some(3));
        let oversized = Full::new(b"abc".to_vec()).limit(2).size_hint();
        assert_eq!(oversized.lower(), 0);
        assert_eq!(oversized.upper(), Some(2));
        assert_eq!(oversized.exact(), None);
        let (source, state) = source([Err("source"), data(b"later")]);
        let mut body = source.limit(u64::MAX);
        assert_eq!(body.next_frame().await.unwrap_err(), LimitError::Source("source"));
        assert!(body.next_frame().await.unwrap().is_none());
        assert_eq!(state.polls.get(), 1);
    });
}

#[test]
fn data_mapping_passes_trailers_and_drops_transformed_recycling() {
    use bytes::Bytes;
    karmaio::Runtime::new().unwrap().block_on(async {
        let mut fields = HeaderMap::new();
        fields.append("x-end", "one".parse().unwrap());
        fields.append("x-end", "two".parse().unwrap());
        let (source, state) = source([data(b"a"), data(b""), Ok(Frame::trailers(fields.clone()))]);
        let calls = Cell::new(0);
        let mut body = source.map_data(|mut bytes| {
            calls.set(calls.get() + 1);
            bytes.push(b'!');
            Bytes::from(bytes)
        });
        assert_eq!(body.size_hint(), SizeHint::new());
        assert_eq!(body.trailer_hint(), TrailerHint::MayHave);
        for expected in [b"a!".as_slice(), b"!".as_slice()] {
            let bytes = body.next_frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(&bytes[..], expected);
            body.recycle(bytes);
        }
        assert_eq!(
            body.next_frame().await.unwrap().unwrap().into_trailers().unwrap(),
            fields
        );
        assert_eq!(calls.get(), 2);
        assert!(state.reclaimed.borrow().is_empty());
        assert!(body.next_frame().await.unwrap().is_none());
        assert!(body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(0));
    });
}

#[test]
fn data_mapping_preserves_no_trailers_and_fuses_source_errors() {
    use vakya::body::{Empty, Full};
    assert!(Empty::new().map_data(|bytes| bytes).is_end_stream());
    let body = Full::new(b"abc".to_vec()).map_data(|data| data);
    assert_eq!(body.size_hint(), SizeHint::new());
    assert_eq!(body.trailer_hint(), TrailerHint::None);
    karmaio::Runtime::new().unwrap().block_on(async {
        let (source, state) = source([Err("origin"), data(b"unread")]);
        let mut body = source.map_data(|bytes| bytes);
        assert_eq!(body.next_frame().await.unwrap_err(), "origin");
        assert!(body.is_end_stream());
        assert!(body.next_frame().await.unwrap().is_none());
        assert_eq!(state.polls.get(), 1);
    });
}
