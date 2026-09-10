use std::{cell::Cell, rc::Rc};

/// Request-local controls, separate from the incoming body's lifetime.
///
/// This handle is not cloneable. Dropping it has no effect on body consumption.
/// It may be retained while producing a response; requests made after a head
/// was sent affect connection reuse rather than rewriting that head.
#[derive(Debug)]
pub struct RequestContext {
    close: Rc<Cell<bool>>,
}

impl RequestContext {
    pub(crate) fn new(close: Rc<Cell<bool>>) -> Self {
        Self { close }
    }

    /// Finish this exchange and close instead of accepting another request.
    /// This does not abort a body or cancel pending I/O. Calls after this
    /// exchange has completed have no effect.
    #[inline]
    pub fn close_connection(&self) {
        self.close.set(true);
    }
}
