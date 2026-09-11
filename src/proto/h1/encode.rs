use super::inline::InlineBuf;
use super::{
    UpgradeKind,
    decode::validate_trailers,
    head::{HeadError, RequestHead, ResponseHead, parse_content_length, parse_transfer_encoding},
};
use crate::body::{SizeHint, TrailerHint};
use bytes::Bytes;
use http::{
    HeaderMap, HeaderValue, Method, StatusCode, Uri, Version,
    header::{CONTENT_LENGTH, TRANSFER_ENCODING},
};

/// Snapshot taken before polling the producer. Payload bounds and trailer
/// capability are independent of the selected wire framing.
#[derive(Clone, Copy, Debug)]
pub(super) struct BodyMetadata {
    pub(super) size: SizeHint,
    pub(super) trailers: TrailerHint,
}

#[cfg(test)]
impl BodyMetadata {
    fn exact(length: u64) -> Self {
        Self {
            size: SizeHint::with_exact(length),
            trailers: TrailerHint::None,
        }
    }

    fn unknown() -> Self {
        Self {
            size: SizeHint::new(),
            trailers: TrailerHint::MayHave,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EncodeLimits {
    pub(super) max_head_bytes: usize,
    pub(super) max_trailer_bytes: usize,
}

impl EncodeLimits {
    pub(super) const fn new(max_head_bytes: usize, max_trailer_bytes: usize) -> Self {
        assert!(
            max_head_bytes > 0 && max_trailer_bytes > 0,
            "encoder limits must be nonzero"
        );
        Self {
            max_head_bytes,
            max_trailer_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EncodeError {
    InvalidHead,
    InvalidFraming,
    LengthMismatch,
    BodyForbidden,
    TrailersForbidden,
    FrameAfterEnd,
    Unsupported,
    Limit,
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for EncodeError {}

impl From<EncodeError> for crate::Error {
    fn from(error: EncodeError) -> Self {
        let kind = match error {
            EncodeError::Limit => crate::ErrorKind::Limit,
            EncodeError::Unsupported => crate::ErrorKind::Unsupported,
            _ => crate::ErrorKind::LocalMessage,
        };
        Self::with_source(kind, "outgoing HTTP message cannot be encoded safely", error)
    }
}

use super::BodyMode;

#[derive(Debug)]
pub(super) struct EncodedHead {
    pub(super) bytes: Bytes,
    pub(super) mode: BodyMode,
    pub(super) upgrade: Option<UpgradeKind>,
    pub(super) persistence: super::Persistence,
}

pub(super) fn encode_request_head(
    method: Method,
    target: Uri,
    version: Version,
    headers: HeaderMap,
    metadata: BodyMetadata,
    limits: EncodeLimits,
) -> Result<EncodedHead, EncodeError> {
    prepare_request_head(method, target, version, headers, metadata, limits).map(|(head, _)| head)
}

pub(super) fn prepare_request_head(
    method: Method,
    target: Uri,
    version: Version,
    mut headers: HeaderMap,
    metadata: BodyMetadata,
    limits: EncodeLimits,
) -> Result<(EncodedHead, super::head::ValidatedRequestHead), EncodeError> {
    let mode = plan_request_framing(&mut headers, version, metadata)?;
    let validated = RequestHead {
        method,
        target,
        version,
        headers,
    }
    .validate()
    .map_err(map_head_error)?;
    if validated.body != mode {
        return Err(EncodeError::InvalidFraming);
    }

    let mut writer = BoundedWriter::new(limits.max_head_bytes);
    writer.push(validated.head.method.as_str().as_bytes())?;
    writer.push(b" ")?;
    writer.push(validated.head.target.to_string().as_bytes())?;
    writer.push(b" ")?;
    writer.push(version_bytes(version)?)?;
    writer.push(b"\r\n")?;
    write_headers(&mut writer, &validated.head.headers)?;
    writer.push(b"\r\n")?;
    Ok((
        EncodedHead {
            bytes: writer.finish(),
            mode,
            persistence: validated.persistence,
            upgrade: None,
        },
        validated,
    ))
}

pub(super) fn encode_response_head(
    status: StatusCode,
    version: Version,
    headers: HeaderMap,
    metadata: BodyMetadata,
    request_method: &Method,
    limits: EncodeLimits,
) -> Result<EncodedHead, EncodeError> {
    prepare_response_head(status, version, headers, metadata, request_method, limits).map(|(head, _)| head)
}

pub(super) fn prepare_response_head(
    status: StatusCode,
    version: Version,
    mut headers: HeaderMap,
    metadata: BodyMetadata,
    request_method: &Method,
    limits: EncodeLimits,
) -> Result<(EncodedHead, super::head::ValidatedResponseHead), EncodeError> {
    let mode = plan_response_framing(&mut headers, version, status, metadata, request_method)?;
    let validated = ResponseHead {
        version,
        status,
        headers,
    }
    .validate(request_method)
    .map_err(map_head_error)?;
    if validated.body != mode {
        return Err(EncodeError::InvalidFraming);
    }

    let mut writer = BoundedWriter::new(limits.max_head_bytes);
    writer.push(version_bytes(version)?)?;
    writer.push(b" ")?;
    writer.push(status.as_str().as_bytes())?;
    writer.push(b" ")?;
    writer.push(status.canonical_reason().unwrap_or("").as_bytes())?;
    writer.push(b"\r\n")?;
    write_headers(&mut writer, &validated.head.headers)?;
    writer.push(b"\r\n")?;

    Ok((
        EncodedHead {
            bytes: writer.finish(),
            mode,
            persistence: validated.persistence,
            upgrade: validated.upgrade,
        },
        validated,
    ))
}

fn plan_request_framing(
    headers: &mut HeaderMap,
    version: Version,
    metadata: BodyMetadata,
) -> Result<BodyMode, EncodeError> {
    if metadata.trailers == TrailerHint::None
        && let Some(length) = metadata.size.exact()
    {
        set_exact_length(headers, length)?;
        return Ok(BodyMode::Fixed(length));
    }
    if version != Version::HTTP_11 {
        return Err(EncodeError::Unsupported);
    }
    set_chunked(headers)?;
    Ok(BodyMode::Chunked)
}

fn plan_response_framing(
    headers: &mut HeaderMap,
    version: Version,
    status: StatusCode,
    metadata: BodyMetadata,
    request_method: &Method,
) -> Result<BodyMode, EncodeError> {
    let tunnel = request_method == Method::CONNECT && status.is_success();
    let strictly_forbidden = status.is_informational() || status == StatusCode::NO_CONTENT;
    let metadata_only = request_method == Method::HEAD || status == StatusCode::NOT_MODIFIED;

    if tunnel || strictly_forbidden {
        if metadata.size.exact() != Some(0) || metadata.trailers != TrailerHint::None {
            return Err(EncodeError::BodyForbidden);
        }
        if headers.contains_key(CONTENT_LENGTH) || headers.contains_key(TRANSFER_ENCODING) {
            return Err(EncodeError::BodyForbidden);
        }
        return Ok(BodyMode::None);
    }

    if metadata_only {
        // These headers describe the corresponding representation, but no
        // producer frames are sent and its payload bounds are not consumed.
        plan_representation_framing(headers, version, metadata)?;
        return Ok(BodyMode::None);
    }

    if status == StatusCode::RESET_CONTENT {
        if metadata.size.exact() != Some(0)
            || metadata.trailers != TrailerHint::None
            || headers.contains_key(TRANSFER_ENCODING)
        {
            return Err(EncodeError::BodyForbidden);
        }
        set_exact_length(headers, 0)?;
        return Ok(BodyMode::Fixed(0));
    }

    plan_representation_framing(headers, version, metadata)
}

fn plan_representation_framing(
    headers: &mut HeaderMap,
    version: Version,
    metadata: BodyMetadata,
) -> Result<BodyMode, EncodeError> {
    if metadata.trailers == TrailerHint::None
        && let Some(length) = metadata.size.exact()
    {
        set_exact_length(headers, length)?;
        return Ok(BodyMode::Fixed(length));
    }
    if version == Version::HTTP_11 {
        set_chunked(headers)?;
        return Ok(BodyMode::Chunked);
    }
    if metadata.trailers == TrailerHint::MayHave {
        return Err(EncodeError::TrailersForbidden);
    }
    reject_framing_headers(headers)?;
    Ok(BodyMode::UntilEof)
}

fn set_exact_length(headers: &mut HeaderMap, length: u64) -> Result<(), EncodeError> {
    if parse_transfer_encoding(headers).map_err(map_head_error)?.is_some() {
        return Err(EncodeError::InvalidFraming);
    }
    if let Some(declared) = parse_content_length(headers).map_err(map_head_error)? {
        return if declared == length {
            Ok(())
        } else {
            Err(EncodeError::LengthMismatch)
        };
    }
    let value = HeaderValue::from_str(&length.to_string()).map_err(|_| EncodeError::InvalidFraming)?;
    headers.insert(CONTENT_LENGTH, value);
    Ok(())
}

fn set_chunked(headers: &mut HeaderMap) -> Result<(), EncodeError> {
    if headers.contains_key(CONTENT_LENGTH) {
        return Err(EncodeError::InvalidFraming);
    }
    if parse_transfer_encoding(headers).map_err(map_head_error)?.is_none() {
        headers.insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
    }
    Ok(())
}

fn reject_framing_headers(headers: &HeaderMap) -> Result<(), EncodeError> {
    if headers.contains_key(CONTENT_LENGTH) || headers.contains_key(TRANSFER_ENCODING) {
        Err(EncodeError::InvalidFraming)
    } else {
        Ok(())
    }
}

fn map_head_error(error: HeadError) -> EncodeError {
    match error {
        HeadError::Limit(_) => EncodeError::Limit,
        HeadError::UnsupportedTransferCoding | HeadError::UnsupportedVersion => EncodeError::Unsupported,
        HeadError::AmbiguousMessageLength | HeadError::InvalidContentLength | HeadError::InvalidTransferEncoding => {
            EncodeError::InvalidFraming
        }
        _ => EncodeError::InvalidHead,
    }
}

fn version_bytes(version: Version) -> Result<&'static [u8], EncodeError> {
    match version {
        Version::HTTP_10 => Ok(b"HTTP/1.0"),
        Version::HTTP_11 => Ok(b"HTTP/1.1"),
        _ => Err(EncodeError::Unsupported),
    }
}

fn write_headers(writer: &mut BoundedWriter, headers: &HeaderMap) -> Result<(), EncodeError> {
    for (name, value) in headers {
        writer.push(name.as_str().as_bytes())?;
        writer.push(b": ")?;
        writer.push(value.as_bytes())?;
        writer.push(b"\r\n")?;
    }
    Ok(())
}

#[derive(Debug)]
struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl BoundedWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }

    fn push(&mut self, value: &[u8]) -> Result<(), EncodeError> {
        let length = self.bytes.len().checked_add(value.len()).ok_or(EncodeError::Limit)?;
        if length > self.limit {
            return Err(EncodeError::Limit);
        }
        self.bytes.try_reserve(value.len()).map_err(|_| EncodeError::Limit)?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn finish(self) -> Bytes {
        Bytes::from(self.bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DataFraming {
    pub(super) prefix: InlineBuf<CHUNK_PREFIX_CAPACITY>,
    pub(super) suffix: Bytes,
}

const CHUNK_PREFIX_CAPACITY: usize = usize::BITS as usize / 4 + 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BodyState {
    None,
    Fixed,
    Chunked,
    CloseDelimited,
    Done,
    Failed(EncodeError),
}

#[derive(Debug)]
pub(super) struct BodyEncoder {
    state: BodyState,
    max_trailer_bytes: usize,
    remaining_lower: u64,
    remaining_upper: Option<u64>,
    trailers: TrailerHint,
}

impl BodyEncoder {
    pub(super) fn new(mode: BodyMode, metadata: BodyMetadata, limits: EncodeLimits) -> Result<Self, EncodeError> {
        match mode {
            BodyMode::Fixed(length)
                if metadata.size.exact() != Some(length) || metadata.trailers != TrailerHint::None =>
            {
                return Err(EncodeError::InvalidFraming);
            }
            BodyMode::UntilEof if metadata.trailers != TrailerHint::None => return Err(EncodeError::TrailersForbidden),
            _ => {}
        }
        let state = match mode {
            BodyMode::None => BodyState::None,
            BodyMode::Fixed(_) => BodyState::Fixed,
            BodyMode::Chunked => BodyState::Chunked,
            BodyMode::UntilEof => BodyState::CloseDelimited,
        };
        Ok(Self {
            state,
            max_trailer_bytes: limits.max_trailer_bytes,
            remaining_lower: metadata.size.lower(),
            remaining_upper: metadata.size.upper(),
            trailers: metadata.trailers,
        })
    }

    pub(super) fn data(&mut self, length: usize) -> Result<DataFraming, EncodeError> {
        // Validate before returning framing: no bytes from a violating frame
        // may be submitted to I/O. Terminal errors remain sticky.
        match self.state {
            BodyState::Failed(error) => return Err(error),
            BodyState::Done => return self.fail(EncodeError::FrameAfterEnd),
            BodyState::None => return self.fail(EncodeError::BodyForbidden),
            _ => {}
        }
        let Ok(length64) = u64::try_from(length) else {
            return self.fail(EncodeError::LengthMismatch);
        };
        if self.remaining_upper.is_some_and(|upper| length64 > upper) {
            return self.fail(EncodeError::LengthMismatch);
        }
        self.remaining_lower = self.remaining_lower.saturating_sub(length64);
        self.remaining_upper = self.remaining_upper.map(|upper| upper - length64);
        match self.state {
            BodyState::None => self.fail(EncodeError::BodyForbidden),
            BodyState::Fixed => Ok(DataFraming {
                prefix: InlineBuf::empty(),
                suffix: Bytes::new(),
            }),
            BodyState::Chunked if length == 0 => Ok(DataFraming {
                prefix: InlineBuf::empty(),
                suffix: Bytes::new(),
            }),
            BodyState::Chunked => Ok(DataFraming {
                prefix: chunk_prefix(length),
                suffix: Bytes::from_static(b"\r\n"),
            }),
            BodyState::CloseDelimited => Ok(DataFraming {
                prefix: InlineBuf::empty(),
                suffix: Bytes::new(),
            }),
            BodyState::Done => self.fail(EncodeError::FrameAfterEnd),
            BodyState::Failed(error) => Err(error),
        }
    }

    /// Finish wire framing with trailers. The driver must still check that the
    /// producer ends successfully before marking its direction reusable; a late
    /// error or another frame invalidates the exchange even after this output.
    pub(super) fn trailers(&mut self, trailers: &HeaderMap) -> Result<Bytes, EncodeError> {
        match self.state {
            BodyState::Failed(error) => return Err(error),
            BodyState::Done => return self.fail(EncodeError::FrameAfterEnd),
            BodyState::Chunked => {}
            _ => return self.fail(EncodeError::TrailersForbidden),
        }
        if self.remaining_lower != 0 {
            return self.fail(EncodeError::LengthMismatch);
        }
        if self.trailers == TrailerHint::None || validate_trailers(trailers).is_err() {
            return self.fail(EncodeError::TrailersForbidden);
        }
        let mut writer = BoundedWriter::new(self.max_trailer_bytes);
        if writer
            .push(b"0\r\n")
            .and_then(|()| write_headers(&mut writer, trailers))
            .and_then(|()| writer.push(b"\r\n"))
            .is_err()
        {
            return self.fail(EncodeError::Limit);
        }
        self.state = BodyState::Done;
        Ok(writer.finish())
    }

    pub(super) fn finish(&mut self) -> Result<Bytes, EncodeError> {
        match self.state {
            BodyState::Failed(error) => return Err(error),
            BodyState::Done => return self.fail(EncodeError::FrameAfterEnd),
            BodyState::None => {}
            _ if self.remaining_lower != 0 => return self.fail(EncodeError::LengthMismatch),
            _ => {}
        }
        match self.state {
            BodyState::None | BodyState::Fixed | BodyState::CloseDelimited => {
                self.state = BodyState::Done;
                Ok(Bytes::new())
            }
            BodyState::Chunked => {
                self.state = BodyState::Done;
                Ok(Bytes::from_static(b"0\r\n\r\n"))
            }
            BodyState::Done => self.fail(EncodeError::FrameAfterEnd),
            BodyState::Failed(error) => Err(error),
        }
    }

    fn fail<T>(&mut self, error: EncodeError) -> Result<T, EncodeError> {
        self.state = BodyState::Failed(error);
        Err(error)
    }
}

fn chunk_prefix(mut length: usize) -> InlineBuf<CHUNK_PREFIX_CAPACITY> {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let digits = (usize::BITS - length.leading_zeros()).div_ceil(4) as usize;
    let mut bytes = [0; CHUNK_PREFIX_CAPACITY];
    let mut cursor = digits;
    while cursor > 0 {
        cursor -= 1;
        bytes[cursor] = HEX[length & 0x0f];
        length >>= 4;
    }
    bytes[digits] = b'\r';
    bytes[digits + 1] = b'\n';
    InlineBuf::from_parts(bytes, digits + 2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::h1::{
        decode::{BodyDecoder, DecodeOutcome},
        head::{HeadLimits, HeadParser, ParseOutcome, RequestRole, ResponseRole},
    };
    use http::{
        HeaderValue,
        header::{CONNECTION, HOST, UPGRADE},
    };
    use karmaio::buf::IoBuf;

    const LIMITS: EncodeLimits = EncodeLimits::new(4096, 4096);

    #[test]
    fn request_and_response_heads_round_trip() {
        let mut request_headers = HeaderMap::new();
        request_headers.insert(HOST, HeaderValue::from_static("example.test"));
        let request = encode_request_head(
            Method::POST,
            "/items".parse().unwrap(),
            Version::HTTP_11,
            request_headers,
            BodyMetadata::exact(4),
            LIMITS,
        )
        .unwrap();
        assert_eq!(request.mode, BodyMode::Fixed(4));
        let mut parser = HeadParser::<RequestRole>::request(HeadLimits::new(4096, 16));
        let ParseOutcome::Complete { head, .. } = parser.parse(&request.bytes).unwrap() else {
            panic!("incomplete request")
        };
        assert_eq!(head.validate().unwrap().body, BodyMode::Fixed(4));

        let response = encode_response_head(
            StatusCode::OK,
            Version::HTTP_11,
            HeaderMap::new(),
            BodyMetadata::unknown(),
            &Method::GET,
            LIMITS,
        )
        .unwrap();
        assert_eq!(response.mode, BodyMode::Chunked);
        let mut parser = HeadParser::<ResponseRole>::response(HeadLimits::new(4096, 16));
        let ParseOutcome::Complete { head, .. } = parser.parse(&response.bytes).unwrap() else {
            panic!("incomplete response")
        };
        assert_eq!(head.validate(&Method::GET).unwrap().body, BodyMode::Chunked);
    }

    #[test]
    fn exact_headers_must_match_body_length() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("example.test"));
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("3"));
        assert_eq!(
            encode_request_head(
                Method::POST,
                "/".parse().unwrap(),
                Version::HTTP_11,
                headers,
                BodyMetadata::exact(4),
                LIMITS
            )
            .unwrap_err(),
            EncodeError::LengthMismatch
        );
    }

    #[test]
    fn forbidden_responses_and_upgrades_are_validated() {
        for status in [StatusCode::NO_CONTENT, StatusCode::RESET_CONTENT] {
            assert_eq!(
                encode_response_head(
                    status,
                    Version::HTTP_11,
                    HeaderMap::new(),
                    BodyMetadata::exact(1),
                    &Method::GET,
                    LIMITS
                )
                .unwrap_err(),
                EncodeError::BodyForbidden
            );
        }
        assert_eq!(
            encode_response_head(
                StatusCode::NO_CONTENT,
                Version::HTTP_11,
                HeaderMap::new(),
                BodyMetadata::exact(0),
                &Method::GET,
                LIMITS
            )
            .unwrap()
            .mode,
            BodyMode::None
        );
        let reset = encode_response_head(
            StatusCode::RESET_CONTENT,
            Version::HTTP_11,
            HeaderMap::new(),
            BodyMetadata::exact(0),
            &Method::GET,
            LIMITS,
        )
        .unwrap();
        assert_eq!(reset.mode, BodyMode::Fixed(0));
        assert!(reset.bytes.windows(19).any(|bytes| bytes == b"content-length: 0\r\n"));
        let mut upgrade = HeaderMap::new();
        upgrade.insert(CONNECTION, HeaderValue::from_static("upgrade"));
        upgrade.insert(UPGRADE, HeaderValue::from_static("websocket"));
        assert_eq!(
            encode_response_head(
                StatusCode::SWITCHING_PROTOCOLS,
                Version::HTTP_11,
                upgrade,
                BodyMetadata::exact(0),
                &Method::GET,
                LIMITS
            )
            .unwrap()
            .upgrade,
            Some(UpgradeKind::Protocol)
        );
    }

    #[test]
    fn bounded_head_writer_rejects_oversized_output() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("example.test"));
        assert_eq!(
            encode_request_head(
                Method::GET,
                "/".parse().unwrap(),
                Version::HTTP_11,
                headers,
                BodyMetadata::exact(0),
                EncodeLimits::new(8, 32)
            )
            .unwrap_err(),
            EncodeError::Limit
        );
    }

    #[test]
    fn public_error_preserves_encoding_category_and_source() {
        let error: crate::Error = EncodeError::Unsupported.into();
        assert_eq!(error.kind(), crate::ErrorKind::Unsupported);
        assert!(std::error::Error::source(&error).is_some_and(|source| source.is::<EncodeError>()));
    }

    #[test]
    fn body_encoder_frames_fixed_chunked_and_trailers() {
        let mut fixed = BodyEncoder::new(BodyMode::Fixed(4), BodyMetadata::exact(4), LIMITS).unwrap();
        assert_eq!(
            fixed.data(4).unwrap(),
            DataFraming {
                prefix: InlineBuf::empty(),
                suffix: Bytes::new()
            }
        );
        assert!(fixed.finish().unwrap().is_empty());

        let mut chunked = BodyEncoder::new(BodyMode::Chunked, BodyMetadata::unknown(), LIMITS).unwrap();
        let frame = chunked.data(10).unwrap();
        assert_eq!(frame.prefix.as_init(), b"a\r\n");
        assert_eq!(frame.suffix, b"\r\n"[..]);
        let mut trailers = HeaderMap::new();
        trailers.insert("x-checksum", HeaderValue::from_static("good"));
        let ending = chunked.trailers(&trailers).unwrap();
        assert_eq!(ending, "0\r\nx-checksum: good\r\n\r\n");

        let mut decoder = BodyDecoder::new(BodyMode::Chunked);
        let wire = [b"a\r\n".as_slice(), b"0123456789\r\n", ending.as_ref()].concat();
        assert!(matches!(
            decoder.decode(&wire, false).unwrap(),
            DecodeOutcome::Data { .. }
        ));
    }

    #[test]
    fn chunk_prefix_covers_platform_length_width_without_allocation() {
        assert_eq!(chunk_prefix(1).as_init(), b"1\r\n");
        assert_eq!(chunk_prefix(0x1234_abcd).as_init(), b"1234abcd\r\n");
        assert_eq!(
            chunk_prefix(usize::MAX).as_init(),
            format!("{:x}\r\n", usize::MAX).as_bytes()
        );
    }

    #[test]
    fn body_encoder_rejects_length_and_frame_order_violations() {
        let mut too_many = BodyEncoder::new(BodyMode::Fixed(1), BodyMetadata::exact(1), LIMITS).unwrap();
        assert_eq!(too_many.data(2).unwrap_err(), EncodeError::LengthMismatch);
        let mut too_few = BodyEncoder::new(BodyMode::Fixed(2), BodyMetadata::exact(2), LIMITS).unwrap();
        too_few.data(1).unwrap();
        assert_eq!(too_few.finish().unwrap_err(), EncodeError::LengthMismatch);
        let mut none = BodyEncoder::new(BodyMode::None, BodyMetadata::exact(0), LIMITS).unwrap();
        assert_eq!(none.data(0).unwrap_err(), EncodeError::BodyForbidden);
        let mut chunked = BodyEncoder::new(BodyMode::Chunked, BodyMetadata::unknown(), LIMITS).unwrap();
        chunked.finish().unwrap();
        assert_eq!(chunked.data(1).unwrap_err(), EncodeError::FrameAfterEnd);
    }

    #[test]
    fn exact_length_trailers_select_chunked_and_preserve_payload_bounds() {
        let metadata = BodyMetadata {
            size: SizeHint::with_exact(4),
            trailers: TrailerHint::MayHave,
        };
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("example.test"));
        let request = encode_request_head(
            Method::POST,
            "/".parse().unwrap(),
            Version::HTTP_11,
            headers.clone(),
            metadata,
            LIMITS,
        )
        .unwrap();
        let response = encode_response_head(
            StatusCode::OK,
            Version::HTTP_11,
            HeaderMap::new(),
            metadata,
            &Method::GET,
            LIMITS,
        )
        .unwrap();
        for head in [request, response] {
            assert_eq!(head.mode, BodyMode::Chunked);
            assert!(head.bytes.windows(28).any(|b| b == b"transfer-encoding: chunked\r\n"));
            let mut encoder = BodyEncoder::new(head.mode, metadata, LIMITS).unwrap();
            encoder.data(4).unwrap();
            let mut trailers = HeaderMap::new();
            trailers.insert("x-checksum", HeaderValue::from_static("good"));
            assert_eq!(encoder.trailers(&trailers).unwrap(), "0\r\nx-checksum: good\r\n\r\n");
            assert_eq!(encoder.data(0).unwrap_err(), EncodeError::FrameAfterEnd);
        }
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("4"));
        assert_eq!(
            encode_request_head(
                Method::POST,
                "/".parse().unwrap(),
                Version::HTTP_11,
                headers,
                metadata,
                LIMITS
            )
            .unwrap_err(),
            EncodeError::InvalidFraming
        );
        let mut short = BodyEncoder::new(BodyMode::Chunked, metadata, LIMITS).unwrap();
        short.data(3).unwrap();
        assert_eq!(
            short.trailers(&HeaderMap::new()).unwrap_err(),
            EncodeError::LengthMismatch
        );
        assert_eq!(short.data(1).unwrap_err(), EncodeError::LengthMismatch);
        let mut long = BodyEncoder::new(BodyMode::Chunked, metadata, LIMITS).unwrap();
        assert_eq!(long.data(5).unwrap_err(), EncodeError::LengthMismatch);
        let mut unfinished = BodyEncoder::new(BodyMode::Chunked, metadata, LIMITS).unwrap();
        assert_eq!(unfinished.finish().unwrap_err(), EncodeError::LengthMismatch);
        let mut no_trailers = BodyEncoder::new(BodyMode::Chunked, metadata, LIMITS).unwrap();
        no_trailers.data(4).unwrap();
        assert_eq!(no_trailers.finish().unwrap(), "0\r\n\r\n");
    }

    #[test]
    fn bounded_unknown_payloads_and_trailer_promises_are_enforced() {
        let metadata = BodyMetadata {
            size: SizeHint::with_bounds(2, Some(4)).unwrap(),
            trailers: TrailerHint::None,
        };
        for mode in [BodyMode::Chunked, BodyMode::UntilEof] {
            let mut short = BodyEncoder::new(mode, metadata, LIMITS).unwrap();
            short.data(1).unwrap();
            assert_eq!(short.finish().unwrap_err(), EncodeError::LengthMismatch);
            let mut long = BodyEncoder::new(mode, metadata, LIMITS).unwrap();
            long.data(3).unwrap();
            assert_eq!(long.data(2).unwrap_err(), EncodeError::LengthMismatch);
            let mut valid = BodyEncoder::new(mode, metadata, LIMITS).unwrap();
            valid.data(3).unwrap();
            valid.finish().unwrap();
        }
        let mut encoder = BodyEncoder::new(BodyMode::Chunked, metadata, LIMITS).unwrap();
        encoder.data(2).unwrap();
        assert_eq!(
            encoder.trailers(&HeaderMap::new()).unwrap_err(),
            EncodeError::TrailersForbidden
        );
    }

    #[test]
    fn metadata_only_responses_suppress_payload_but_not_representation_headers() {
        for (method, status) in [(Method::HEAD, StatusCode::OK), (Method::GET, StatusCode::NOT_MODIFIED)] {
            let metadata = BodyMetadata::exact(123);
            let head =
                encode_response_head(status, Version::HTTP_11, HeaderMap::new(), metadata, &method, LIMITS).unwrap();
            assert_eq!(head.mode, BodyMode::None);
            assert!(head.bytes.windows(21).any(|b| b == b"content-length: 123\r\n"));
            let mut encoder = BodyEncoder::new(head.mode, metadata, LIMITS).unwrap();
            assert!(encoder.finish().unwrap().is_empty());
        }
        let metadata = BodyMetadata::unknown();
        assert_eq!(
            encode_response_head(
                StatusCode::OK,
                Version::HTTP_10,
                HeaderMap::new(),
                metadata,
                &Method::GET,
                LIMITS
            )
            .unwrap_err(),
            EncodeError::TrailersForbidden
        );
        let metadata = BodyMetadata {
            trailers: TrailerHint::None,
            ..metadata
        };
        let head = encode_response_head(
            StatusCode::OK,
            Version::HTTP_10,
            HeaderMap::new(),
            metadata,
            &Method::GET,
            LIMITS,
        )
        .unwrap();
        assert_eq!(head.mode, BodyMode::UntilEof);
        assert_eq!(head.persistence, super::super::Persistence::Close);
    }

    #[test]
    fn forbidden_trailers_are_rejected() {
        let mut encoder = BodyEncoder::new(BodyMode::Chunked, BodyMetadata::unknown(), LIMITS).unwrap();
        let mut trailers = HeaderMap::new();
        trailers.insert(CONTENT_LENGTH, HeaderValue::from_static("1"));
        assert_eq!(encoder.trailers(&trailers).unwrap_err(), EncodeError::TrailersForbidden);
    }
}
