use super::RequestContext;
use crate::{Request, body::Incoming, service::Service};

/// Adapt an async request-only function to a server service.
///
/// The adapter drops the unused [`RequestContext`] before calling the function.
/// This does not abandon the request body: the function receives its original
/// owned [`Incoming`] and can consume it, retain it, or forward it as a response.
/// The adapter keeps concrete dispatch and adds no allocation, lifetime, or
/// thread bounds. Use [`crate::service::service_fn`] when the service needs
/// informational responses, closure control, or upgrade claims.
///
/// ```
/// use std::convert::Infallible;
/// use vakya::{Request, Response, body::Incoming, server::request_service_fn};
///
/// async fn echo(request: Request<Incoming>) -> Result<Response<Incoming>, Infallible> {
///     Ok(Response::new(request.into_body()))
/// }
/// let service = request_service_fn(echo);
/// ```
#[inline]
pub fn request_service_fn<F>(function: F) -> RequestServiceFn<F> {
    RequestServiceFn { function }
}

/// A concrete request-only adapter created by [`request_service_fn`].
#[derive(Clone, Copy, Debug)]
pub struct RequestServiceFn<F> {
    function: F,
}

impl<F, T, E> Service<(Request<Incoming>, RequestContext)> for RequestServiceFn<F>
where
    F: AsyncFn(Request<Incoming>) -> Result<T, E>,
{
    type Response = T;
    type Error = E;

    #[inline]
    async fn call(&self, (request, context): (Request<Incoming>, RequestContext)) -> Result<T, E> {
        drop(context);
        (self.function)(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body::{Body, Frame, IncomingData, SizeHint, TrailerHint};
    use bytes::Bytes;
    use std::{
        cell::Cell,
        convert::Infallible,
        future::Future,
        pin::pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    #[test]
    fn request_only_adapter_borrows_local_state_and_preserves_body_ownership() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let calls = Rc::new(Cell::new(0));
            let service = request_service_fn(async |request: Request<Incoming>| {
                calls.set(calls.get() + 1);
                Ok::<_, Infallible>(request.into_body())
            });
            let (mut producer, incoming) = Incoming::channel(SizeHint::with_exact(1), TrailerHint::None);
            let (context, _receiver, _upgrade) = super::super::context::channel(false);
            let mut incoming = Service::call(&service, (Request::new(incoming), context))
                .await
                .unwrap();
            assert_eq!(calls.get(), 1);
            assert!(!producer.is_abandoned());
            let bytes = Bytes::from_static(b"x");
            let address = bytes.as_ptr();
            let mut offer = pin!(producer.offer(Frame::data(IncomingData::from_bytes(bytes))));
            assert!(
                offer
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            let frame = incoming.next_frame().await.unwrap().unwrap();
            assert_eq!(frame.data_ref().unwrap().as_ref().as_ptr(), address);
            assert!(matches!(
                offer.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(()))
            ));
        });
    }

    #[test]
    fn adapter_preserves_borrowed_error_types_without_additional_bounds() {
        karmaio::Runtime::new().unwrap().block_on(async {
            let reason = String::from("local failure");
            let service = request_service_fn(async |_: Request<Incoming>| Err::<(), _>(reason.as_str()));
            let (context, _receiver, _upgrade) = super::super::context::channel(false);
            assert_eq!(
                Service::call(&service, (Request::new(Incoming::empty()), context))
                    .await
                    .unwrap_err(),
                "local failure"
            );
        });
    }
}
