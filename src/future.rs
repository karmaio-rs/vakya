use std::{future::poll_fn, task::Poll};

/// Bounds consecutive ready iterations, including frames that submit no I/O.
pub(crate) struct WorkBudget(u8);

impl WorkBudget {
    pub(crate) const fn new() -> Self {
        Self(16)
    }

    pub(crate) async fn step(&mut self) {
        if self.0 == 0 {
            let mut first_poll = Some(());
            poll_fn(|context| {
                if first_poll.take().is_some() {
                    context.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            self.0 = 16;
        }
        self.0 -= 1;
    }
}
