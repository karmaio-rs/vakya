//! Optional structured observations. Never hold a span guard across an await.
use crate::error::ErrorKind;
use std::future::Future;

#[derive(Clone, Debug)]
pub(crate) struct Scope {
    #[cfg(feature = "tracing")]
    span: tracing::Span,
}

impl Default for Scope {
    fn default() -> Self {
        Self {
            #[cfg(feature = "tracing")]
            span: tracing::Span::none(),
        }
    }
}

impl Scope {
    pub(crate) fn connection(role: &'static str) -> Self {
        #[cfg(feature = "tracing")]
        {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(1);
            Self {
                span: tracing::trace_span!(target: "vakya::connection", "connection",
                connection_id = NEXT.fetch_add(1, Ordering::Relaxed), role, protocol = "http/1.1"),
            }
        }
        #[cfg(not(feature = "tracing"))]
        {
            let _ = role;
            Self {}
        }
    }

    // Return the existing future/adaptor without another async state machine.
    #[inline]
    pub(crate) fn instrument<F: Future>(&self, future: F) -> impl Future<Output = F::Output> + use<F> {
        #[cfg(feature = "tracing")]
        {
            use tracing::Instrument;
            future.instrument(self.span.clone())
        }
        #[cfg(not(feature = "tracing"))]
        {
            future
        }
    }

    #[inline]
    pub(crate) fn lifecycle(&self, event: &'static str) {
        #[cfg(feature = "tracing")]
        tracing::trace!(target: "vakya::connection", parent: &self.span, event, "HTTP connection lifecycle");
        #[cfg(not(feature = "tracing"))]
        let _ = event;
    }

    #[inline]
    pub(crate) fn outcome(&self, outcome: &'static str) {
        #[cfg(feature = "tracing")]
        tracing::trace!(target: "vakya::connection", parent: &self.span, outcome, "HTTP exchange settled");
        #[cfg(not(feature = "tracing"))]
        let _ = outcome;
    }

    #[inline]
    pub(crate) fn failure(&self, kind: ErrorKind) {
        #[cfg(feature = "tracing")]
        tracing::debug!(target: "vakya::connection", parent: &self.span, ?kind, "HTTP connection failed");
        #[cfg(not(feature = "tracing"))]
        let _ = kind;
    }
}

#[derive(Default)]
pub(crate) struct Exchanges {
    #[cfg(feature = "tracing")]
    number: u64,
}
impl Exchanges {
    pub(crate) fn next(&mut self) -> Scope {
        #[cfg(feature = "tracing")]
        {
            self.number = self.number.saturating_add(1);
            Scope {
                span: tracing::trace_span!(target: "vakya::connection", "exchange", exchange = self.number),
            }
        }
        #[cfg(not(feature = "tracing"))]
        {
            Scope {}
        }
    }
}

#[inline]
pub(crate) fn progress(operation: &'static str, bytes: usize) {
    #[cfg(feature = "tracing")]
    tracing::trace!(target: "vakya::io", operation, bytes, "HTTP I/O progress");
    #[cfg(not(feature = "tracing"))]
    let _ = (operation, bytes);
}

#[inline]
pub(crate) fn response_head(status: u16) {
    #[cfg(feature = "tracing")]
    tracing::trace!(target: "vakya::connection", status, "HTTP response head");
    #[cfg(not(feature = "tracing"))]
    let _ = status;
}

#[cfg(all(test, feature = "tracing"))]
mod tests {
    use super::*;
    use crate::test_transport::Gate;
    use std::{
        collections::HashMap,
        pin::pin,
        sync::{Arc, Mutex},
        task::{Context, Waker},
    };
    use tracing::{
        Event, Metadata, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Id, Record},
    };

    #[derive(Default)]
    struct Fields(Vec<(String, String)>);
    impl Visit for Fields {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.push((field.name().to_owned(), format!("{value:?}")));
        }
    }
    #[derive(Default)]
    struct Log {
        spans: HashMap<u64, (&'static str, Option<u64>, Fields)>,
        active: Vec<u64>,
        events: Vec<(Option<u64>, Fields)>,
    }
    #[derive(Clone)]
    struct Capture(Arc<Mutex<Log>>);
    impl Subscriber for Capture {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, attributes: &Attributes<'_>) -> Id {
            let mut log = self.0.lock().unwrap();
            let id = log.spans.len() as u64 + 1;
            let parent = attributes
                .parent()
                .map(Id::into_u64)
                .or_else(|| attributes.is_contextual().then(|| log.active.last().copied()).flatten());
            let mut fields = Fields::default();
            attributes.record(&mut fields);
            log.spans.insert(id, (attributes.metadata().name(), parent, fields));
            Id::from_u64(id)
        }
        fn record(&self, id: &Id, values: &Record<'_>) {
            values.record(&mut self.0.lock().unwrap().spans.get_mut(&id.into_u64()).unwrap().2);
        }
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, event: &Event<'_>) {
            let mut log = self.0.lock().unwrap();
            let parent = event
                .parent()
                .map(Id::into_u64)
                .or_else(|| event.is_contextual().then(|| log.active.last().copied()).flatten());
            let mut fields = Fields::default();
            event.record(&mut fields);
            log.events.push((parent, fields));
        }
        fn enter(&self, span: &Id) {
            self.0.lock().unwrap().active.push(span.into_u64());
        }
        fn exit(&self, span: &Id) {
            assert_eq!(self.0.lock().unwrap().active.pop(), Some(span.into_u64()));
        }
    }

    #[test]
    fn concurrent_futures_keep_connection_and_exchange_context_separate() {
        let log = Arc::new(Mutex::new(Log::default()));
        tracing::subscriber::with_default(Capture(log.clone()), || {
            let client = Scope::connection("client");
            let server = Scope::connection("server");
            let first = Gate::default();
            let second = Gate::default();
            async fn work(gate: &Gate, bytes: usize) {
                let exchange = Exchanges::default().next();
                exchange
                    .instrument(async {
                        progress("write", bytes);
                        gate.wait().await;
                        progress("read", bytes);
                    })
                    .await;
                exchange.outcome("reusable");
            }
            let mut a = pin!(client.instrument(work(&first, 3)));
            let mut b = pin!(server.instrument(work(&second, 7)));
            let mut cx = Context::from_waker(Waker::noop());
            assert!(a.as_mut().poll(&mut cx).is_pending());
            assert!(log.lock().unwrap().active.is_empty());
            assert!(b.as_mut().poll(&mut cx).is_pending());
            assert!(log.lock().unwrap().active.is_empty());
            progress("outside", 0);
            first.open();
            assert!(a.as_mut().poll(&mut cx).is_ready());
            second.open();
            assert!(b.as_mut().poll(&mut cx).is_ready());
            client.lifecycle("graceful shutdown requested");
            server.failure(ErrorKind::Io);
            assert!(log.lock().unwrap().active.is_empty());
        });
        let log = log.lock().unwrap();
        let connections: Vec<_> = log
            .spans
            .iter()
            .filter(|(_, (name, _, _))| *name == "connection")
            .collect();
        assert_eq!(connections.len(), 2);
        let ids: Vec<_> = connections
            .iter()
            .map(|(_, (_, _, fields))| &fields.0.iter().find(|(key, _)| key == "connection_id").unwrap().1)
            .collect();
        assert_ne!(ids[0], ids[1]);
        let mut byte_parents = HashMap::new();
        for (parent, fields) in &log.events {
            if let Some((_, bytes)) = fields.0.iter().find(|(key, _)| key == "bytes") {
                if bytes == "0" {
                    assert!(parent.is_none());
                    continue;
                }
                let parent = parent.unwrap();
                let (name, connection, _) = &log.spans[&parent];
                assert_eq!(*name, "exchange");
                assert_eq!(log.spans[&connection.unwrap()].0, "connection");
                if let Some(old) = byte_parents.insert(bytes, parent) {
                    assert_eq!(old, parent);
                }
            }
        }
        assert_eq!(byte_parents.len(), 2);
        assert_ne!(byte_parents[&"3".to_owned()], byte_parents[&"7".to_owned()]);
    }
}
