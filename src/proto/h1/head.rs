//! Private head facade: owned message types, parsing, and semantic validation.
mod scan;
mod validate;

use super::header_case::HeaderCaseMap;
use super::{BodyMode, Expectation, Persistence, TargetForm, UpgradeKind};
use crate::error::{Error, ErrorDetail, ErrorKind, LimitKind};
use http::{HeaderMap, Method, StatusCode, Uri, Version};
#[cfg(any(feature = "client", feature = "server", test))]
pub(super) use scan::HeadParser;
#[cfg(any(feature = "server", test))]
pub(super) use scan::RequestRole;
#[cfg(any(feature = "client", test))]
pub(super) use scan::ResponseRole;
pub(super) use scan::{HeaderWorkspace, own_headers};
pub(super) use validate::{parse_content_length, parse_transfer_encoding};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HeadLimits {
    max_bytes: usize,
    max_headers: usize,
}

impl HeadLimits {
    pub(super) const fn new(max_bytes: usize, max_headers: usize) -> Self {
        assert!(max_bytes > 0, "head byte limit must be nonzero");
        assert!(max_headers > 0, "header count limit must be nonzero");
        Self { max_bytes, max_headers }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HeadLimit {
    Bytes,
    Headers,
    HeaderMap,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HeadError {
    InvalidStartLine,
    InvalidHeader,
    UnsupportedVersion,
    InvalidMethod,
    InvalidTarget,
    InvalidStatus,
    MissingHost,
    InvalidHost,
    InvalidTargetForm,
    InvalidContentLength,
    AmbiguousMessageLength,
    InvalidTransferEncoding,
    UnsupportedTransferCoding,
    InvalidConnection,
    InvalidExpectation,
    InvalidUpgrade,
    Limit(HeadLimit),
}

impl std::fmt::Display for HeadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for HeadError {}

impl From<HeadError> for Error {
    fn from(error: HeadError) -> Self {
        let kind = match error {
            HeadError::Limit(_) => ErrorKind::Limit,
            HeadError::UnsupportedVersion | HeadError::UnsupportedTransferCoding => ErrorKind::Unsupported,
            _ => ErrorKind::InvalidMessage,
        };
        Self::with_source(kind, "received HTTP head failed validation", error).classified(match error {
            HeadError::Limit(limit) => Some(ErrorDetail::IncomingLimit(match limit {
                HeadLimit::Bytes => LimitKind::HeadBytes,
                HeadLimit::Headers => LimitKind::HeaderCount,
                HeadLimit::HeaderMap => LimitKind::HeadMapCapacity,
            })),
            _ => None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RequestHead {
    pub(super) method: Method,
    pub(super) target: Uri,
    pub(super) version: Version,
    pub(super) headers: HeaderMap,
    pub(super) header_case: Option<HeaderCaseMap>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ResponseHead {
    pub(super) version: Version,
    pub(super) status: StatusCode,
    pub(super) headers: HeaderMap,
    pub(super) header_case: Option<HeaderCaseMap>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ValidatedRequestHead {
    pub(super) head: RequestHead,
    pub(super) body: BodyMode,
    pub(super) persistence: Persistence,
    pub(super) target_form: TargetForm,
    pub(super) expectation: Expectation,
    pub(super) upgrade: Option<UpgradeKind>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ValidatedResponseHead {
    pub(super) upgrade: Option<UpgradeKind>,
    pub(super) head: ResponseHead,
    pub(super) body: BodyMode,
    pub(super) persistence: Persistence,
}

#[cfg(test)]
mod tests {
    use super::{HeadError, HeadLimit, HeadLimits, HeadParser, RequestHead, RequestRole, ResponseHead, ResponseRole};
    use crate::error::{Error, ErrorDetail, ErrorKind, LimitKind};
    use crate::proto::h1::{BodyMode, Expectation, Persistence, TargetForm, UpgradeKind};
    use bytes::Bytes;
    use http::{Method, StatusCode, Version, header::HOST};

    const LIMITS: HeadLimits = HeadLimits::new(1024, 8);

    #[test]
    fn request_head_is_incremental_at_every_split() {
        let input = b"GET /items?q=rust HTTP/1.1\r\nHost: example.test\r\nX-Test: yes\r\n\r\nbody";

        for split in 0..input.len() - 4 {
            let mut parser = HeadParser::<RequestRole>::request(LIMITS);
            assert_eq!(parser.head_len(&input[..split]).unwrap(), None);
        }

        let mut parser = HeadParser::<RequestRole>::request(LIMITS);
        let consumed = parser.head_len(input).unwrap().expect("complete request head");
        let head = parser.parse_shared(Bytes::copy_from_slice(&input[..consumed])).unwrap();
        assert_eq!(head.method, Method::GET);
        assert_eq!(head.target, "/items?q=rust");
        assert_eq!(head.version, Version::HTTP_11);
        assert_eq!(head.headers[HOST], "example.test");
        assert_eq!(&input[consumed..], b"body");
    }

    #[test]
    fn response_head_is_incremental_at_every_split() {
        let input = b"HTTP/1.0 204 No Content\r\nServer: vakya\r\n\r\nnext";

        for split in 0..input.len() - 4 {
            let mut parser = HeadParser::<ResponseRole>::response(LIMITS);
            assert_eq!(parser.head_len(&input[..split]).unwrap(), None);
        }

        let mut parser = HeadParser::<ResponseRole>::response(LIMITS);
        let consumed = parser.head_len(input).unwrap().expect("complete response head");
        let head = parser.parse_shared(Bytes::copy_from_slice(&input[..consumed])).unwrap();
        assert_eq!(head.status, StatusCode::NO_CONTENT);
        assert_eq!(head.version, Version::HTTP_10);
        assert_eq!(&input[consumed..], b"next");
    }

    #[test]
    fn shared_heads_reuse_input_for_targets_and_values() {
        let request = Bytes::copy_from_slice(
            b"GET /shared/resource?q=rust HTTP/1.1\r\nHost: shared.example\r\nX-Test: shared-value\r\n\r\nbody",
        );
        let request_head_len = request.windows(4).position(|window| window == b"\r\n\r\n").unwrap() + 4;
        let target_offset = request
            .windows(16)
            .position(|window| window == b"/shared/resource")
            .unwrap();
        let host_offset = request
            .windows(14)
            .position(|window| window == b"shared.example")
            .unwrap();
        let target_pointer = request[target_offset..].as_ptr();
        let host_pointer = request[host_offset..].as_ptr();

        let mut parser = HeadParser::<RequestRole>::request(LIMITS);
        assert_eq!(parser.head_len(&request).unwrap(), Some(request_head_len));
        assert_eq!(parser.head_len(&request).unwrap(), Some(request_head_len));
        let head = parser.parse_shared(request.slice(..request_head_len)).unwrap();
        drop(request);
        let next = Bytes::from_static(b"GET /next HTTP/1.1\r\nHost: next.example\r\n\r\n");
        assert_eq!(parser.head_len(&next).unwrap(), Some(next.len()));
        parser.parse_shared(next).unwrap();
        assert_eq!(head.target, "/shared/resource?q=rust");
        assert_eq!(head.headers[HOST], "shared.example");
        assert_eq!(head.target.path().as_ptr(), target_pointer);
        assert_eq!(head.headers[HOST].as_bytes().as_ptr(), host_pointer);

        let response = Bytes::copy_from_slice(b"HTTP/1.1 200 OK\r\nX-Test: shared-response\r\n\r\nnext");
        let response_head_len = response.windows(4).position(|window| window == b"\r\n\r\n").unwrap() + 4;
        let value_offset = response
            .windows(15)
            .position(|window| window == b"shared-response")
            .unwrap();
        let value_pointer = response[value_offset..].as_ptr();

        let mut parser = HeadParser::<ResponseRole>::response(LIMITS);
        assert_eq!(parser.head_len(&response).unwrap(), Some(response_head_len));
        assert_eq!(parser.head_len(&response).unwrap(), Some(response_head_len));
        let head = parser.parse_shared(response.slice(..response_head_len)).unwrap();
        assert_eq!(head.headers["x-test"].as_bytes().as_ptr(), value_pointer);
    }

    #[test]
    fn head_and_header_count_limits_are_enforced_at_boundaries() {
        let input = b"GET / HTTP/1.1\r\nA: 1\r\nB: 2\r\n\r\n";
        let mut one_header = HeadParser::<RequestRole>::request(HeadLimits::new(1024, 1));
        let consumed = one_header.head_len(input).unwrap().expect("complete request head");
        assert_eq!(
            one_header
                .parse_shared(Bytes::copy_from_slice(&input[..consumed]))
                .unwrap_err(),
            HeadError::Limit(HeadLimit::Headers)
        );

        let exact = b"GET / HTTP/1.1\r\n\r\n";
        let mut exact_parser = HeadParser::<RequestRole>::request(HeadLimits::new(exact.len(), 1));
        let consumed = exact_parser.head_len(exact).unwrap().expect("complete request head");
        exact_parser
            .parse_shared(Bytes::copy_from_slice(&exact[..consumed]))
            .unwrap();

        let mut short_parser = HeadParser::<RequestRole>::request(HeadLimits::new(exact.len() - 1, 1));
        assert_eq!(
            short_parser.head_len(exact).unwrap_err(),
            HeadError::Limit(HeadLimit::Bytes)
        );
    }

    #[test]
    fn unsupported_versions_and_malformed_start_lines_are_typed() {
        let mut request = HeadParser::<RequestRole>::request(LIMITS);
        let input = b"GET / HTTP/2.0\r\n\r\n";
        let consumed = request.head_len(input).unwrap().expect("complete request head");
        assert_eq!(
            request
                .parse_shared(Bytes::copy_from_slice(&input[..consumed]))
                .unwrap_err(),
            HeadError::UnsupportedVersion
        );
        let input = b"GET  HTTP/1.1\r\n\r\n";
        let consumed = request.head_len(input).unwrap().expect("complete request head");
        assert_eq!(
            request
                .parse_shared(Bytes::copy_from_slice(&input[..consumed]))
                .unwrap_err(),
            HeadError::InvalidStartLine
        );

        let mut response = HeadParser::<ResponseRole>::response(LIMITS);
        let input = b"HTTP/1.1 xyz Bad\r\n\r\n";
        let consumed = response.head_len(input).unwrap().expect("complete response head");
        assert_eq!(
            response
                .parse_shared(Bytes::copy_from_slice(&input[..consumed]))
                .unwrap_err(),
            HeadError::InvalidStartLine
        );
    }

    #[test]
    fn fragmented_heads_accept_newline_variants_and_reuse_parser() {
        for newline in ["\r\n", "\n"] {
            let wire = format!(
                "{newline}{newline}GET / HTTP/1.1{newline}Host: test{newline}X-Large: {}{newline}{newline}",
                "x".repeat(4096)
            );
            let mut parser = HeadParser::<RequestRole>::request(HeadLimits::new(8192, 8));
            for end in 1..wire.len() {
                assert_eq!(parser.head_len(&wire.as_bytes()[..end]).unwrap(), None);
            }
            let consumed = parser
                .head_len(wire.as_bytes())
                .unwrap()
                .expect("complete request head");
            parser
                .parse_shared(Bytes::copy_from_slice(&wire.as_bytes()[..consumed]))
                .unwrap();
            // A completed parser can immediately read the next head.
            let consumed = parser
                .head_len(wire.as_bytes())
                .unwrap()
                .expect("complete request head");
            parser
                .parse_shared(Bytes::copy_from_slice(&wire.as_bytes()[..consumed]))
                .unwrap();
        }
    }

    #[test]
    fn parser_workspace_can_be_reused_after_partial_and_error() {
        let mut parser = HeadParser::<RequestRole>::request(LIMITS);
        assert_eq!(parser.head_len(b"GET / HTTP/1.1\r\n").unwrap(), None);

        let input = b"bad start\r\n\r\n";
        let consumed = parser.head_len(input).unwrap().expect("complete malformed head");
        assert!(parser.parse_shared(Bytes::copy_from_slice(&input[..consumed])).is_err());

        let input = b"GET /ok HTTP/1.1\r\nHost: example.test\r\n\r\n";
        let consumed = parser.head_len(input).unwrap().expect("complete request head");
        parser.parse_shared(Bytes::copy_from_slice(&input[..consumed])).unwrap();
    }

    #[test]
    fn public_error_preserves_head_category_and_source() {
        let error: Error = HeadError::Limit(HeadLimit::Headers).into();
        assert_eq!(error.kind(), ErrorKind::Limit);
        assert_eq!(error.detail(), Some(ErrorDetail::IncomingLimit(LimitKind::HeaderCount)));
        for (private, public) in [
            (HeadLimit::Bytes, LimitKind::HeadBytes),
            (HeadLimit::HeaderMap, LimitKind::HeadMapCapacity),
        ] {
            assert_eq!(
                Error::from(HeadError::Limit(private)).detail(),
                Some(ErrorDetail::IncomingLimit(public))
            );
        }
        assert!(std::error::Error::source(&error).is_some_and(|source| source.is::<HeadError>()));
    }

    #[test]
    fn request_targets_and_host_are_validated_together() {
        let origin = request(b"GET /path HTTP/1.1\r\nHost: example.test\r\n\r\n")
            .validate()
            .unwrap();
        assert_eq!(origin.target_form, TargetForm::Origin);

        let absolute = request(b"GET http://example.test/path HTTP/1.1\r\nHost: example.test\r\n\r\n")
            .validate()
            .unwrap();
        assert_eq!(absolute.target_form, TargetForm::Absolute);

        let normalized = request(b"GET http://target.test:8080/path HTTP/1.1\r\nHost: conflicting.test\r\n\r\n")
            .validate()
            .unwrap();
        assert_eq!(normalized.head.headers[HOST], "target.test:8080");

        let options = request(b"OPTIONS * HTTP/1.1\r\nHost: example.test\r\n\r\n")
            .validate()
            .unwrap();
        assert_eq!(options.target_form, TargetForm::Asterisk);

        let connect = request(b"CONNECT example.test:443 HTTP/1.1\r\nHost: example.test:443\r\n\r\n")
            .validate()
            .unwrap();
        assert_eq!(connect.target_form, TargetForm::Authority);
        assert_eq!(connect.upgrade, Some(UpgradeKind::Tunnel));

        assert_eq!(
            request(b"GET / HTTP/1.1\r\n\r\n").validate().unwrap_err(),
            HeadError::MissingHost
        );
        assert_eq!(
            request(b"GET * HTTP/1.1\r\nHost: example.test\r\n\r\n")
                .validate()
                .unwrap_err(),
            HeadError::InvalidTargetForm
        );
        assert_eq!(
            request(b"CONNECT /path HTTP/1.1\r\nHost: example.test\r\n\r\n")
                .validate()
                .unwrap_err(),
            HeadError::InvalidTargetForm
        );
        assert_eq!(
            request(b"OPTIONS example.test:443 HTTP/1.1\r\nHost: example.test:443\r\n\r\n")
                .validate()
                .unwrap_err(),
            HeadError::InvalidTargetForm
        );
    }

    #[test]
    fn content_length_requires_identical_checked_values() {
        let validated =
            request(b"POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: 5, 5\r\nContent-Length: 5\r\n\r\n")
                .validate()
                .unwrap();
        assert_eq!(validated.body, BodyMode::Fixed(5));

        let zero_padded = request(b"POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: 0005, 0005\r\n\r\n")
            .validate()
            .unwrap();
        assert_eq!(zero_padded.body, BodyMode::Fixed(5));

        for value in [
            "5, 6",
            "+5",
            "-1",
            "5,,5",
            "5,",
            ",5",
            "5 5",
            "",
            "18446744073709551616",
        ] {
            let bytes = format!("POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: {value}\r\n\r\n");
            assert!(matches!(
                request(bytes.as_bytes()).validate(),
                Err(HeadError::InvalidContentLength | HeadError::AmbiguousMessageLength)
            ));
        }
    }

    #[test]
    fn transfer_encoding_is_token_aware_and_unambiguous() {
        let chunked = request(b"POST / HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: ChUnKeD\r\n\r\n")
            .validate()
            .unwrap();
        assert_eq!(chunked.body, BodyMode::Chunked);

        let conflict = request(
            b"POST / HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n",
        );
        assert_eq!(conflict.validate().unwrap_err(), HeadError::AmbiguousMessageLength);

        for value in [
            "chunked, gzip",
            "chunked, chunked",
            "chunked; q=1",
            "gzip, chunked",
            "chunked,",
            ",chunked",
            "chunked,,gzip",
        ] {
            let bytes = format!("POST / HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: {value}\r\n\r\n");
            assert!(matches!(
                request(bytes.as_bytes()).validate(),
                Err(HeadError::InvalidTransferEncoding | HeadError::UnsupportedTransferCoding)
            ));
        }
    }

    #[test]
    fn persistence_uses_version_defaults_and_connection_tokens() {
        assert_eq!(
            request(b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n")
                .validate()
                .unwrap()
                .persistence,
            Persistence::Reusable
        );
        assert_eq!(
            request(b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: xclose\r\n\r\n")
                .validate()
                .unwrap()
                .persistence,
            Persistence::Reusable
        );
        assert_eq!(
            request(b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: keep-alive, CLOSE\r\n\r\n")
                .validate()
                .unwrap()
                .persistence,
            Persistence::Close
        );
        assert_eq!(
            request(b"GET / HTTP/1.0\r\n\r\n").validate().unwrap().persistence,
            Persistence::Close
        );
        assert_eq!(
            request(b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
                .validate()
                .unwrap()
                .persistence,
            Persistence::Reusable
        );
    }

    #[test]
    fn response_body_mode_obeys_method_status_and_framing() {
        assert_eq!(
            response(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n")
                .validate(&Method::HEAD)
                .unwrap()
                .body,
            BodyMode::None
        );
        for status in [100, 204, 205, 304] {
            let bytes = format!("HTTP/1.1 {status} Status\r\nContent-Length: 9\r\n\r\n");
            assert_eq!(
                response(bytes.as_bytes()).validate(&Method::GET).unwrap().body,
                BodyMode::None
            );
        }
        assert_eq!(
            response(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n")
                .validate(&Method::GET)
                .unwrap()
                .body,
            BodyMode::Fixed(9)
        );
        let eof = response(b"HTTP/1.1 200 OK\r\n\r\n").validate(&Method::GET).unwrap();
        assert_eq!(eof.body, BodyMode::UntilEof);
        assert_eq!(eof.persistence, Persistence::Close);
        let reset = response(b"HTTP/1.1 205 Reset Content\r\n\r\n")
            .validate(&Method::GET)
            .unwrap();
        assert_eq!(reset.body, BodyMode::None);
        assert_eq!(reset.persistence, Persistence::Reusable);
        assert_eq!(
            response(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .validate(&Method::CONNECT)
                .unwrap()
                .upgrade,
            Some(UpgradeKind::Tunnel)
        );

        let head = response(b"HTTP/1.1 200 OK\r\nContent-Length: invalid\r\nTransfer-Encoding: chunked\r\n\r\n")
            .validate(&Method::HEAD)
            .unwrap();
        assert_eq!(head.body, BodyMode::None);
        let tunnel = response(
            b"HTTP/1.1 200 Connection Established\r\nContent-Length: invalid\r\nTransfer-Encoding: gzip\r\n\r\n",
        )
        .validate(&Method::CONNECT)
        .unwrap();
        assert_eq!(tunnel.upgrade, Some(UpgradeKind::Tunnel));
        assert_eq!(tunnel.body, BodyMode::None);
    }

    #[test]
    fn received_http10_fields_are_ignored_without_weakening_outgoing_validation() {
        for (field, expected) in [
            ("Expect: 100-continue\r\n", HeadError::InvalidExpectation),
            (
                "Connection: upgrade\r\nUpgrade: websocket\r\n",
                HeadError::InvalidUpgrade,
            ),
            ("Upgrade: malformed value\r\n", HeadError::InvalidUpgrade),
        ] {
            let wire = format!("POST / HTTP/1.0\r\nContent-Length: 1\r\n{field}\r\n");
            let head = request(wire.as_bytes());
            assert_eq!(head.clone().validate().unwrap_err(), expected);
            let headers = head.headers.clone();
            let received = head.validate_received().unwrap();
            assert_eq!(received.head.headers, headers);
            assert_eq!(received.expectation, Expectation::None);
            assert_eq!(received.upgrade, None);
            assert_eq!(received.body, BodyMode::Fixed(1));
            assert_eq!(received.persistence, Persistence::Close);
        }
        let tunnel = request(b"CONNECT example.test:443 HTTP/1.0\r\nUpgrade: ignored\r\n\r\n")
            .validate_received()
            .unwrap();
        assert_eq!(tunnel.upgrade, Some(UpgradeKind::Tunnel));
        assert_eq!(
            request(b"POST / HTTP/1.0\r\nExpect: unsupported\r\n\r\n")
                .validate_received()
                .unwrap_err(),
            HeadError::InvalidExpectation,
        );
    }

    #[test]
    fn upgrades_and_expectations_require_complete_token_sets() {
        let upgrade_request = request(
            b"GET /chat HTTP/1.1\r\nHost: example.test\r\nConnection: keep-alive, Upgrade\r\nUpgrade: websocket/13\r\n\r\n",
        )
        .validate()
        .unwrap();
        assert_eq!(upgrade_request.upgrade, Some(UpgradeKind::Protocol));

        let upgrade_response =
            response(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
                .validate(&Method::GET)
                .unwrap();
        assert_eq!(upgrade_response.upgrade, Some(UpgradeKind::Protocol));
        assert_eq!(upgrade_response.body, BodyMode::None);

        let expect =
            request(b"POST / HTTP/1.1\r\nHost: example.test\r\nExpect: 100-continue\r\nContent-Length: 1\r\n\r\n")
                .validate()
                .unwrap();
        assert_eq!(expect.expectation, Expectation::Continue);

        assert_eq!(
            request(b"GET / HTTP/1.1\r\nHost: example.test\r\nUpgrade: websocket\r\n\r\n")
                .validate()
                .unwrap_err(),
            HeadError::InvalidUpgrade
        );
        assert_eq!(
            response(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
                .validate(&Method::GET)
                .unwrap_err(),
            HeadError::InvalidUpgrade
        );
        assert_eq!(
            request(
                b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: close, upgrade\r\nUpgrade: websocket\r\n\r\n"
            )
            .validate()
            .unwrap_err(),
            HeadError::InvalidUpgrade
        );
        assert_eq!(
            request(b"GET / HTTP/1.0\r\nHost: example.test\r\nConnection: upgrade\r\nUpgrade: test\r\n\r\n")
                .validate()
                .unwrap_err(),
            HeadError::InvalidUpgrade
        );
        assert_eq!(
            response(b"HTTP/1.0 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: test\r\n\r\n")
                .validate(&Method::GET)
                .unwrap_err(),
            HeadError::InvalidUpgrade
        );
    }

    #[test]
    fn response_status_is_limited_to_registered_three_digit_range() {
        let mut parser = HeadParser::<ResponseRole>::response(LIMITS);
        let input = b"HTTP/1.1 600 Future\r\n\r\n";
        let consumed = parser.head_len(input).unwrap().expect("complete response head");
        assert_eq!(
            parser
                .parse_shared(Bytes::copy_from_slice(&input[..consumed]))
                .unwrap_err(),
            HeadError::InvalidStatus
        );
        let parsed = response(b"HTTP/1.1 599 Edge\r\nContent-Length: 0\r\n\r\n");
        assert_eq!(parsed.status.as_u16(), 599);
    }

    #[test]
    fn obsolete_folding_and_whitespace_before_colon_are_rejected() {
        let mut parser = HeadParser::<RequestRole>::request(LIMITS);
        let input = b"GET / HTTP/1.1\r\nHost : example.test\r\n\r\n";
        let consumed = parser.head_len(input).unwrap().expect("complete request head");
        assert_eq!(
            parser
                .parse_shared(Bytes::copy_from_slice(&input[..consumed]))
                .unwrap_err(),
            HeadError::InvalidHeader
        );
        let input = b"GET / HTTP/1.1\r\nHost: example.test\r\n folded\r\n\r\n";
        let consumed = parser.head_len(input).unwrap().expect("complete request head");
        assert_eq!(
            parser
                .parse_shared(Bytes::copy_from_slice(&input[..consumed]))
                .unwrap_err(),
            HeadError::InvalidHeader
        );
    }

    fn request(input: &[u8]) -> RequestHead {
        let mut parser = HeadParser::<RequestRole>::request(HeadLimits::new(4096, 16));
        let consumed = parser.head_len(input).unwrap().expect("complete request fixture");
        parser.parse_shared(Bytes::copy_from_slice(&input[..consumed])).unwrap()
    }

    fn response(input: &[u8]) -> ResponseHead {
        let mut parser = HeadParser::<ResponseRole>::response(HeadLimits::new(4096, 16));
        let consumed = parser.head_len(input).unwrap().expect("complete response fixture");
        parser.parse_shared(Bytes::copy_from_slice(&input[..consumed])).unwrap()
    }
}
