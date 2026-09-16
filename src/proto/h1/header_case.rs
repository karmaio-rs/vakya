use bytes::Bytes;
use http::{HeaderMap, HeaderName, header::ValueIter};

/// Original HTTP/1 field-name spellings, grouped in value order.
///
/// The type remains private, but travels in `http::Extensions` so mapping a
/// request or response body retains proxy forwarding metadata.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct HeaderCaseMap(HeaderMap<Bytes>);

impl HeaderCaseMap {
    pub(super) fn append(&mut self, name: HeaderName, original: Bytes) {
        self.0.append(name, original);
    }

    pub(super) fn get_all(&self, name: &HeaderName) -> ValueIter<'_, Bytes> {
        self.0.get_all(name).into_iter()
    }
}
