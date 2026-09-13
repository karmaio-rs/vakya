//! Native asynchronous service composition.

use std::rc::Rc;

/// An asynchronous operation over a request of type `R`.
///
/// Request and response types are unrestricted here; HTTP connection APIs
/// impose their own message and body requirements. Neither the service nor
/// its generated future requires `Send`, `Sync`, `Clone`, or `'static`.
/// Calls may overlap. Mutable shared state must be coordinated explicitly,
/// without holding a dynamic borrow across an await that can admit another call.
#[allow(async_fn_in_trait)]
pub trait Service<R> {
    /// The successful result of this service.
    type Response;
    /// The error returned by this service.
    type Error;

    /// Handles a request, borrowing the service until the call completes.
    ///
    /// Dropping this future cancels the call. Implementations must keep owned
    /// resources safe on cancellation; any external side effects already
    /// performed are not rolled back. Errors retain the service's own type.
    async fn call(&self, request: R) -> Result<Self::Response, Self::Error>;
}

/// Wraps an async function or closure as a native service.
///
/// The wrapper retains the concrete future and supports closures borrowing
/// local state. It introduces no allocation or dynamic dispatch.
///
/// ```
/// use std::{cell::Cell, convert::Infallible, rc::Rc};
/// use vakya::service::{Service, service_fn};
///
/// async fn example() {
///     let calls = Cell::new(0);
///     let service = Rc::new(service_fn(async |value: u32| {
///         calls.set(calls.get() + 1);
///         Ok::<_, Infallible>(value + 1)
///     }));
///     let boxed = Box::new(service);
///     assert_eq!(Service::call(&&boxed, 41).await.unwrap(), 42);
///     assert_eq!(calls.get(), 1);
/// }
/// ```
#[inline]
pub fn service_fn<F>(function: F) -> ServiceFn<F> {
    ServiceFn { function }
}

/// A statically dispatched service created by [`service_fn`].
#[derive(Clone, Copy, Debug)]
pub struct ServiceFn<F> {
    function: F,
}

impl<R, F, T, E> Service<R> for ServiceFn<F>
where
    F: AsyncFn(R) -> Result<T, E>,
{
    type Response = T;
    type Error = E;

    #[inline]
    async fn call(&self, request: R) -> Result<T, E> {
        (self.function)(request).await
    }
}

impl<R, S: Service<R> + ?Sized> Service<R> for &S {
    type Response = S::Response;
    type Error = S::Error;

    #[inline]
    async fn call(&self, request: R) -> Result<Self::Response, Self::Error> {
        (**self).call(request).await
    }
}

impl<R, S: Service<R> + ?Sized> Service<R> for Box<S> {
    type Response = S::Response;
    type Error = S::Error;

    #[inline]
    async fn call(&self, request: R) -> Result<Self::Response, Self::Error> {
        (**self).call(request).await
    }
}

impl<R, S: Service<R> + ?Sized> Service<R> for Rc<S> {
    type Response = S::Response;
    type Error = S::Error;

    #[inline]
    async fn call(&self, request: R) -> Result<Self::Response, Self::Error> {
        (**self).call(request).await
    }
}
