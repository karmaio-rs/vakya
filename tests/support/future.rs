//! Polling helpers that never poll a completed future again.
use std::{
    future::{Future, poll_fn},
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
};

pub fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

pub async fn pair<A: Future, B: Future>(first: A, second: B) -> (A::Output, B::Output) {
    let mut first = pin!(first);
    let mut second = pin!(second);
    let mut a = None;
    let mut b = None;
    poll_fn(|cx| {
        if a.is_none()
            && let Poll::Ready(value) = first.as_mut().poll(cx)
        {
            a = Some(value);
        }
        if b.is_none()
            && let Poll::Ready(value) = second.as_mut().poll(cx)
        {
            b = Some(value);
        }
        if a.is_some() && b.is_some() {
            Poll::Ready((a.take().unwrap(), b.take().unwrap()))
        } else {
            Poll::Pending
        }
    })
    .await
}
