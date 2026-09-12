use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

/// Creates the connection-local, capacity-one body rendezvous.
pub(crate) fn channel<T, E>() -> (Producer<T, E>, Consumer<T, E>) {
    let shared = Rc::new(Shared {
        state: RefCell::new(State::Idle),
        producer_waker: RefCell::new(None),
        consumer_waker: RefCell::new(None),
        drain_remaining: std::cell::Cell::new(None),
    });

    (
        Producer {
            shared: Rc::clone(&shared),
            lifecycle: EndpointLifecycle::Active,
        },
        Consumer {
            shared,
            lifecycle: EndpointLifecycle::Active,
        },
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EndpointLifecycle {
    Active,
    Complete,
}

struct Shared<T, E> {
    state: RefCell<State<T, E>>,
    producer_waker: RefCell<Option<Waker>>,
    consumer_waker: RefCell<Option<Waker>>,
    drain_remaining: std::cell::Cell<Option<(u64, u64)>>,
}

enum State<T, E> {
    Idle,
    Demanded,
    Offered(T),
    Closed,
    Disconnected,
    Failed(E),
    Abandoned(Option<T>),
}

/// The connection-owned endpoint of an incoming-body rendezvous.
pub(crate) struct Producer<T, E> {
    shared: Rc<Shared<T, E>>,
    lifecycle: EndpointLifecycle,
}

impl<T, E> Producer<T, E> {
    /// Charge consumed framing bytes only after a consumer opts into draining.
    #[cfg(feature = "http1")]
    pub(crate) fn charge_drain(&self, bytes: usize, payload: usize) -> bool {
        let Some((wire, data)) = self.shared.drain_remaining.get() else {
            return true;
        };
        let (Some(wire), Some(data)) = (wire.checked_sub(bytes as u64), data.checked_sub(payload as u64)) else {
            return false;
        };
        self.shared.drain_remaining.set(Some((wire, data)));
        true
    }

    /// Waits until the body consumer asks for another item.
    pub(crate) fn demand(&mut self) -> Demand<'_, T, E> {
        Demand { producer: self }
    }

    /// Offers one item and waits until the consumer takes or abandons it.
    pub(crate) fn offer(&mut self, item: T) -> Offer<'_, T, E> {
        Offer {
            producer: self,
            lifecycle: OfferLifecycle::Pending(item),
        }
    }

    /// Waits until the consumer endpoint is dropped before completion.
    pub(crate) fn abandoned(&mut self) -> Abandoned<'_, T, E> {
        Abandoned { producer: self }
    }

    /// Returns whether the consumer endpoint has been abandoned.
    pub(crate) fn is_abandoned(&self) -> bool {
        matches!(*self.shared.state.borrow(), State::Abandoned(_))
    }

    /// Closes the body normally and wakes a waiting consumer.
    pub(crate) fn close(mut self) {
        self.finish(State::Closed);
    }

    /// Closes the body with an error and wakes a waiting consumer.
    pub(crate) fn fail(mut self, error: E) {
        self.finish(State::Failed(error));
    }

    fn finish(&mut self, terminal: State<T, E>) {
        if self.lifecycle == EndpointLifecycle::Complete {
            return;
        }

        let wake = {
            let mut state = self.shared.state.borrow_mut();
            match &*state {
                State::Idle | State::Demanded => {
                    *state = terminal;
                    self.shared.consumer_waker.borrow_mut().take()
                }
                State::Abandoned(_) => None,
                State::Offered(_) => {
                    unreachable!("producer cannot finish while an offer borrows it")
                }
                State::Closed | State::Disconnected | State::Failed(_) => None,
            }
        };
        self.lifecycle = EndpointLifecycle::Complete;
        wake_one(wake);
    }

    #[cfg(test)]
    fn reference_count(&self) -> usize {
        Rc::strong_count(&self.shared)
    }
}

impl<T, E> Drop for Producer<T, E> {
    fn drop(&mut self) {
        if self.lifecycle == EndpointLifecycle::Active {
            self.finish(State::Disconnected);
        }
    }
}

/// The body-owned endpoint of an incoming-body rendezvous.
pub(crate) struct Consumer<T, E> {
    shared: Rc<Shared<T, E>>,
    lifecycle: EndpointLifecycle,
}

impl<T, E> Consumer<T, E> {
    pub(crate) fn drain_budget(&self, bytes: u64, wire_allowance: u64) {
        self.shared
            .drain_remaining
            .set(Some((bytes.saturating_add(wire_allowance), bytes)));
    }

    /// Waits for the next offered item or producer termination.
    pub(crate) fn take(&mut self) -> Take<'_, T, E> {
        Take { consumer: self }
    }

    #[cfg(test)]
    fn reference_count(&self) -> usize {
        Rc::strong_count(&self.shared)
    }
}

impl<T, E> Drop for Consumer<T, E> {
    fn drop(&mut self) {
        if self.lifecycle == EndpointLifecycle::Complete {
            return;
        }

        let wake = {
            let mut state = self.shared.state.borrow_mut();
            let offered = match std::mem::replace(&mut *state, State::Abandoned(None)) {
                State::Offered(item) => Some(item),
                State::Abandoned(item) => item,
                State::Idle | State::Demanded => None,
                terminal @ (State::Closed | State::Disconnected | State::Failed(_)) => {
                    *state = terminal;
                    return;
                }
            };
            *state = State::Abandoned(offered);
            self.shared.producer_waker.borrow_mut().take()
        };
        wake_one(wake);
    }
}

/// A producer-side wait for consumer demand.
pub(crate) struct Demand<'a, T, E> {
    producer: &'a mut Producer<T, E>,
}

impl<T, E> Future for Demand<'_, T, E> {
    type Output = Result<(), DemandError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let shared = &self.producer.shared;
        let state = shared.state.borrow_mut();

        match &*state {
            State::Demanded => Poll::Ready(Ok(())),
            State::Idle => {
                replace_waker(&mut shared.producer_waker.borrow_mut(), context.waker());
                Poll::Pending
            }
            State::Abandoned(_) => Poll::Ready(Err(DemandError::Abandoned)),
            State::Closed | State::Disconnected | State::Failed(_) => Poll::Ready(Err(DemandError::Closed)),
            State::Offered(_) => Poll::Ready(Ok(())),
        }
    }
}

impl<T, E> Drop for Demand<'_, T, E> {
    fn drop(&mut self) {
        self.producer.shared.producer_waker.borrow_mut().take();
    }
}

/// Why waiting for body demand could not continue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DemandError {
    Abandoned,
    Closed,
}

/// A producer-side wait for one offered item to be consumed.
pub(crate) struct Offer<'a, T, E> {
    producer: &'a mut Producer<T, E>,
    lifecycle: OfferLifecycle<T>,
}

enum OfferLifecycle<T> {
    Pending(T),
    Submitted,
}

impl<T, E> Unpin for Offer<'_, T, E> {}

impl<T, E> Future for Offer<'_, T, E> {
    type Output = Result<(), OfferError<T>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let shared = &this.producer.shared;

        if matches!(this.lifecycle, OfferLifecycle::Pending(_)) {
            let OfferLifecycle::Pending(item) = std::mem::replace(&mut this.lifecycle, OfferLifecycle::Submitted)
            else {
                unreachable!();
            };
            let mut state = shared.state.borrow_mut();
            match &mut *state {
                State::Idle | State::Demanded => {
                    *state = State::Offered(item);
                    replace_waker(&mut shared.producer_waker.borrow_mut(), context.waker());
                    let wake = shared.consumer_waker.borrow_mut().take();
                    drop(state);
                    wake_one(wake);
                    return Poll::Pending;
                }
                State::Abandoned(_) => {
                    return Poll::Ready(Err(OfferError::Abandoned(item)));
                }
                State::Closed | State::Disconnected | State::Failed(_) => {
                    return Poll::Ready(Err(OfferError::Closed(item)));
                }
                State::Offered(_) => unreachable!("one producer submitted overlapping offers"),
            }
        }

        let mut state = shared.state.borrow_mut();
        match &mut *state {
            State::Offered(_) => {
                replace_waker(&mut shared.producer_waker.borrow_mut(), context.waker());
                Poll::Pending
            }
            State::Idle | State::Demanded => Poll::Ready(Ok(())),
            State::Abandoned(item) => match item.take() {
                Some(item) => Poll::Ready(Err(OfferError::Abandoned(item))),
                None => Poll::Ready(Ok(())),
            },
            State::Closed | State::Disconnected | State::Failed(_) => {
                unreachable!("an in-flight offer cannot be terminated by its producer")
            }
        }
    }
}

impl<T, E> Drop for Offer<'_, T, E> {
    fn drop(&mut self) {
        self.producer.shared.producer_waker.borrow_mut().take();
        if matches!(self.lifecycle, OfferLifecycle::Pending(_)) {
            return;
        }

        let (item, wake) = {
            let mut state = self.producer.shared.state.borrow_mut();
            match std::mem::replace(&mut *state, State::Idle) {
                State::Offered(item) => (Some(item), self.producer.shared.consumer_waker.borrow_mut().take()),
                State::Abandoned(item) => {
                    *state = State::Abandoned(None);
                    (item, None)
                }
                other @ (State::Idle | State::Demanded) => {
                    *state = other;
                    (None, None)
                }
                other @ (State::Closed | State::Disconnected | State::Failed(_)) => {
                    *state = other;
                    (None, None)
                }
            }
        };
        drop(item);
        wake_one(wake);
    }
}

/// An offered item returned because the consumer could not accept it.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum OfferError<T> {
    Abandoned(T),
    Closed(T),
}

/// Explicit failure and unfinished producer Drop are distinct from clean EOF.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum TakeError<E> {
    Failed(E),
    Disconnected,
}

/// A consumer-side wait for the next body item.
pub(crate) struct Take<'a, T, E> {
    consumer: &'a mut Consumer<T, E>,
}

impl<T, E> Future for Take<'_, T, E> {
    type Output = Result<Option<T>, TakeError<E>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let consumer = &mut self.get_mut().consumer;
        let shared = &consumer.shared;
        let mut state = shared.state.borrow_mut();

        match std::mem::replace(&mut *state, State::Idle) {
            State::Idle | State::Demanded => {
                *state = State::Demanded;
                replace_waker(&mut shared.consumer_waker.borrow_mut(), context.waker());
                let wake = shared.producer_waker.borrow_mut().take();
                drop(state);
                wake_one(wake);
                Poll::Pending
            }
            State::Offered(item) => {
                let wake = shared.producer_waker.borrow_mut().take();
                drop(state);
                wake_one(wake);
                Poll::Ready(Ok(Some(item)))
            }
            State::Closed => {
                *state = State::Closed;
                drop(state);
                consumer.lifecycle = EndpointLifecycle::Complete;
                Poll::Ready(Ok(None))
            }
            State::Failed(error) => {
                *state = State::Closed;
                drop(state);
                consumer.lifecycle = EndpointLifecycle::Complete;
                Poll::Ready(Err(TakeError::Failed(error)))
            }
            State::Disconnected => {
                *state = State::Closed;
                drop(state);
                consumer.lifecycle = EndpointLifecycle::Complete;
                Poll::Ready(Err(TakeError::Disconnected))
            }
            State::Abandoned(item) => {
                *state = State::Abandoned(item);
                unreachable!("an abandoned consumer cannot be polled")
            }
        }
    }
}

impl<T, E> Drop for Take<'_, T, E> {
    fn drop(&mut self) {
        self.consumer.shared.consumer_waker.borrow_mut().take();
    }
}

/// A producer-side wait for consumer abandonment during transport I/O.
pub(crate) struct Abandoned<'a, T, E> {
    producer: &'a mut Producer<T, E>,
}

impl<T, E> Future for Abandoned<'_, T, E> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.producer.is_abandoned() {
            Poll::Ready(())
        } else {
            replace_waker(&mut self.producer.shared.producer_waker.borrow_mut(), context.waker());
            Poll::Pending
        }
    }
}

impl<T, E> Drop for Abandoned<'_, T, E> {
    fn drop(&mut self) {
        self.producer.shared.producer_waker.borrow_mut().take();
    }
}

fn replace_waker(slot: &mut Option<Waker>, waker: &Waker) {
    if slot.as_ref().is_none_or(|registered| !registered.will_wake(waker)) {
        *slot = Some(waker.clone());
    }
}

fn wake_one(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use super::{DemandError, OfferError, TakeError, channel};
    use std::{
        cell::Cell,
        future::Future,
        pin::{Pin, pin},
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    struct CounterWake(AtomicUsize);

    impl Wake for CounterWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn counter_waker() -> (Arc<CounterWake>, Waker) {
        let counter = Arc::new(CounterWake(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        (counter, waker)
    }

    fn poll<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }

    #[test]
    fn demand_before_offer_wakes_and_transfers_one_item() {
        let (mut producer, mut consumer) = channel::<usize, &'static str>();
        let (producer_wakes, producer_waker) = counter_waker();
        let (consumer_wakes, consumer_waker) = counter_waker();
        let mut take = pin!(consumer.take());

        {
            let mut demand = pin!(producer.demand());
            assert!(poll(demand.as_mut(), &producer_waker).is_pending());
            assert!(poll(take.as_mut(), &consumer_waker).is_pending());
            assert_eq!(producer_wakes.0.load(Ordering::Relaxed), 1);
            assert_eq!(poll(demand.as_mut(), &producer_waker), Poll::Ready(Ok(())));
        }

        let mut offer = pin!(producer.offer(7));
        assert!(poll(offer.as_mut(), &producer_waker).is_pending());
        assert_eq!(consumer_wakes.0.load(Ordering::Relaxed), 1);
        assert_eq!(poll(take.as_mut(), &consumer_waker), Poll::Ready(Ok(Some(7))));
        assert_eq!(poll(offer.as_mut(), &producer_waker), Poll::Ready(Ok(())));
    }

    #[test]
    fn offer_before_demand_waits_until_take() {
        let (mut producer, mut consumer) = channel::<usize, ()>();
        let (_, waker) = counter_waker();
        let mut offer = pin!(producer.offer(11));

        assert!(poll(offer.as_mut(), &waker).is_pending());
        let mut take = pin!(consumer.take());
        assert_eq!(poll(take.as_mut(), &waker), Poll::Ready(Ok(Some(11))));
        assert_eq!(poll(offer.as_mut(), &waker), Poll::Ready(Ok(())));
    }

    #[test]
    fn completion_and_failure_wake_waiting_consumer() {
        let (producer, mut consumer) = channel::<(), &'static str>();
        let (wakes, waker) = counter_waker();
        let mut take = pin!(consumer.take());
        assert!(poll(take.as_mut(), &waker).is_pending());
        producer.close();
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        assert_eq!(poll(take.as_mut(), &waker), Poll::Ready(Ok(None)));

        let (producer, mut consumer) = channel::<(), &'static str>();
        let mut take = pin!(consumer.take());
        assert!(poll(take.as_mut(), &waker).is_pending());
        producer.fail("broken");
        assert_eq!(
            poll(take.as_mut(), &waker),
            Poll::Ready(Err(TakeError::Failed("broken")))
        );
    }

    #[test]
    fn consumer_abandonment_returns_the_offered_item() {
        let (mut producer, consumer) = channel::<usize, ()>();
        let (wakes, waker) = counter_waker();
        let mut offer = pin!(producer.offer(19));
        assert!(poll(offer.as_mut(), &waker).is_pending());

        drop(consumer);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        assert_eq!(
            poll(offer.as_mut(), &waker),
            Poll::Ready(Err(OfferError::Abandoned(19)))
        );
    }

    #[test]
    fn abandonment_after_take_still_completes_the_offer() {
        let (mut producer, mut consumer) = channel::<usize, ()>();
        let (_, waker) = counter_waker();
        let mut offer = pin!(producer.offer(23));
        assert!(poll(offer.as_mut(), &waker).is_pending());
        {
            let mut take = pin!(consumer.take());
            assert_eq!(poll(take.as_mut(), &waker), Poll::Ready(Ok(Some(23))));
        }

        drop(consumer);
        assert_eq!(poll(offer.as_mut(), &waker), Poll::Ready(Ok(())));
    }

    #[test]
    fn cancellation_drops_an_offered_item_once_and_reopens_the_slot() {
        struct Tracked(Rc<Cell<usize>>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let drops = Rc::new(Cell::new(0));
        let (mut producer, mut consumer) = channel::<Tracked, ()>();
        let (_, waker) = counter_waker();
        {
            let mut offer = pin!(producer.offer(Tracked(Rc::clone(&drops))));
            assert!(poll(offer.as_mut(), &waker).is_pending());
        }
        assert_eq!(drops.get(), 1);

        let mut take = pin!(consumer.take());
        assert!(poll(take.as_mut(), &waker).is_pending());
    }

    #[test]
    fn producer_and_consumer_cancellation_wake_the_opposite_side() {
        let (mut producer, consumer) = channel::<(), ()>();
        let (_, waker) = counter_waker();
        {
            let mut demand = pin!(producer.demand());
            assert!(poll(demand.as_mut(), &waker).is_pending());
        }
        drop(consumer);
        let mut demand = pin!(producer.demand());
        assert_eq!(poll(demand.as_mut(), &waker), Poll::Ready(Err(DemandError::Abandoned)));

        let (producer, mut consumer) = channel::<(), ()>();
        {
            let mut take = pin!(consumer.take());
            assert!(poll(take.as_mut(), &waker).is_pending());
        }
        drop(producer);
        let mut take = pin!(consumer.take());
        assert_eq!(poll(take.as_mut(), &waker), Poll::Ready(Err(TakeError::Disconnected)));
    }

    #[test]
    fn changed_wakers_replace_old_registrations() {
        let (mut producer, mut consumer) = channel::<usize, ()>();
        let (old, old_waker) = counter_waker();
        let (new, new_waker) = counter_waker();
        let mut take = pin!(consumer.take());

        assert!(poll(take.as_mut(), &old_waker).is_pending());
        assert!(poll(take.as_mut(), &new_waker).is_pending());
        let mut offer = pin!(producer.offer(1));
        assert!(poll(offer.as_mut(), &new_waker).is_pending());

        assert_eq!(old.0.load(Ordering::Relaxed), 0);
        assert_eq!(new.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn offers_do_not_clone_the_shared_allocation() {
        let (mut producer, mut consumer) = channel::<usize, ()>();
        assert_eq!(producer.reference_count(), 2);
        assert_eq!(consumer.reference_count(), 2);
        let (_, waker) = counter_waker();

        for item in 0..4 {
            {
                let mut offer = pin!(producer.offer(item));
                assert!(poll(offer.as_mut(), &waker).is_pending());
                let mut take = pin!(consumer.take());
                assert_eq!(poll(take.as_mut(), &waker), Poll::Ready(Ok(Some(item))));
                assert_eq!(poll(offer.as_mut(), &waker), Poll::Ready(Ok(())));
            }
            assert_eq!(producer.reference_count(), 2);
        }
    }

    #[test]
    fn abandoned_offer_errors_preserve_items() {
        let (mut producer, consumer) = channel::<usize, ()>();
        drop(consumer);
        let (_, waker) = counter_waker();
        let mut offer = pin!(producer.offer(3));

        assert_eq!(poll(offer.as_mut(), &waker), Poll::Ready(Err(OfferError::Abandoned(3))));
    }
}
