use super::syntax::{is_tchar, quoted_string_len, trim_ows};
use super::{BodyMode, Expectation, Persistence, TargetForm, UpgradeKind};
use crate::error::{Error, ErrorKind};
use bytes::Bytes;
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, Version,
    header::{CONNECTION, CONTENT_LENGTH, EXPECT, HOST, TRANSFER_ENCODING, UPGRADE},
    uri::Authority,
};
use std::{marker::PhantomData, mem::MaybeUninit, slice};

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
        Self::with_source(kind, "received HTTP head failed validation", error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RequestHead {
    pub(super) method: Method,
    pub(super) target: Uri,
    pub(super) version: Version,
    pub(super) headers: HeaderMap,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ResponseHead {
    pub(super) version: Version,
    pub(super) status: StatusCode,
    pub(super) headers: HeaderMap,
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

#[derive(Debug)]
pub(super) enum RequestRole {}

#[derive(Debug)]
pub(super) enum ResponseRole {}

/// Incremental parser over a retained input prefix. After an incomplete scan,
/// the next call must retain the same bytes and append new input. Once
/// `head_len` returns a prefix length, the caller transfers exactly that prefix
/// to `parse_shared`, leaving body or upgraded-protocol bytes untouched.
#[derive(Debug)]
pub(super) struct HeadParser<R> {
    limits: HeadLimits,
    workspace: HeaderWorkspace,
    role: PhantomData<fn() -> R>,
    scan: HeadScan,
}

#[derive(Debug)]
enum HeadScan {
    Scanning { scanned: usize, line: ScanLine },
    Complete { length: usize },
}

impl Default for HeadScan {
    fn default() -> Self {
        Self::Scanning {
            scanned: 0,
            line: ScanLine::Leading,
        }
    }
}

#[derive(Debug, Default)]
enum ScanLine {
    #[default]
    Leading,
    Content,
    End,
    EndCr,
}

impl HeadScan {
    /// Finds only the head terminator; httparse remains the syntax authority.
    fn complete(&mut self, input: &[u8]) -> Option<usize> {
        let reset = match self {
            Self::Scanning { scanned, .. } => input.len() < *scanned,
            Self::Complete { length } if input.len() >= *length => return Some(*length),
            Self::Complete { .. } => true,
        };
        if reset {
            *self = Self::default();
        }

        let Self::Scanning { scanned, line } = self else {
            unreachable!("complete scan returned above")
        };
        for &byte in &input[*scanned..] {
            *scanned += 1;
            if matches!((&*line, byte), (ScanLine::End | ScanLine::EndCr, b'\n')) {
                let length = *scanned;
                *self = Self::Complete { length };
                return Some(length);
            }
            *line = match (&*line, byte) {
                (ScanLine::Leading, b'\r' | b'\n') => ScanLine::Leading,
                (ScanLine::End, b'\r') => ScanLine::EndCr,
                (_, b'\n') => ScanLine::End,
                _ => ScanLine::Content,
            };
        }
        None
    }
}

impl<R> HeadParser<R> {
    fn with_limits(limits: HeadLimits) -> Self {
        Self {
            limits,
            workspace: HeaderWorkspace::new(limits.max_headers),
            role: PhantomData,
            scan: HeadScan::default(),
        }
    }

    fn parse_input_len(&self, input: &[u8]) -> usize {
        input.len().min(self.limits.max_bytes)
    }

    /// Find a complete retained head before transferring its backing storage.
    /// Repeated calls return the same length until the caller passes that exact
    /// prefix to the role's `parse_shared`.
    pub(super) fn head_len(&mut self, input: &[u8]) -> Result<Option<usize>, HeadError> {
        let parse_len = self.parse_input_len(input);
        let complete = self.scan.complete(&input[..parse_len]);
        if complete.is_some() {
            Ok(complete)
        } else if input.len() > self.limits.max_bytes {
            self.scan = HeadScan::default();
            Err(HeadError::Limit(HeadLimit::Bytes))
        } else {
            Ok(None)
        }
    }
}

impl HeadParser<RequestRole> {
    pub(super) fn request(limits: HeadLimits) -> Self {
        Self::with_limits(limits)
    }

    pub(super) fn parse_shared(&mut self, input: Bytes) -> Result<RequestHead, HeadError> {
        if input.len() > self.limits.max_bytes {
            self.scan = HeadScan::default();
            return Err(HeadError::Limit(HeadLimit::Bytes));
        }
        let parsed = {
            let mut request = httparse::Request::new(&mut []);
            let workspace = self.workspace.for_input();
            match httparse::ParserConfig::default()
                .parse_request_with_uninit_headers(&mut request, &input, workspace)
                .map_err(map_request_error)
            {
                Ok(httparse::Status::Complete(consumed)) if consumed == input.len() => {
                    build_shared_request_head(request, &input)
                }
                Ok(httparse::Status::Complete(_)) | Ok(httparse::Status::Partial) => Err(HeadError::InvalidStartLine),
                Err(error) => Err(error),
            }
        };
        self.workspace.clear();
        self.scan = HeadScan::default();
        parsed
    }
}

impl HeadParser<ResponseRole> {
    pub(super) fn response(limits: HeadLimits) -> Self {
        Self::with_limits(limits)
    }

    pub(super) fn parse_shared(&mut self, input: Bytes) -> Result<ResponseHead, HeadError> {
        if input.len() > self.limits.max_bytes {
            self.scan = HeadScan::default();
            return Err(HeadError::Limit(HeadLimit::Bytes));
        }
        let parsed = {
            let mut response = httparse::Response::new(&mut []);
            let workspace = self.workspace.for_input();
            match httparse::ParserConfig::default()
                .parse_response_with_uninit_headers(&mut response, &input, workspace)
                .map_err(map_response_error)
            {
                Ok(httparse::Status::Complete(consumed)) if consumed == input.len() => {
                    build_shared_response_head(response, &input)
                }
                Ok(httparse::Status::Complete(_)) | Ok(httparse::Status::Partial) => Err(HeadError::InvalidStartLine),
                Err(error) => Err(error),
            }
        };
        self.workspace.clear();
        self.scan = HeadScan::default();
        parsed
    }
}

impl RequestHead {
    pub(super) fn validate(self) -> Result<ValidatedRequestHead, HeadError> {
        self.validate_for(false)
    }

    /// Apply recipient semantics without weakening locally generated requests.
    #[cfg(any(feature = "server", test))]
    pub(super) fn validate_received(self) -> Result<ValidatedRequestHead, HeadError> {
        self.validate_for(true)
    }

    fn validate_for(mut self, received: bool) -> Result<ValidatedRequestHead, HeadError> {
        let target_form = validate_request_target(&self.method, &self.target)?;
        validate_host(&self.headers, self.version)?;
        if target_form == TargetForm::Absolute {
            normalize_absolute_host(&mut self.headers, &self.target)?;
        }

        let content_length = parse_content_length(&self.headers)?;
        let transfer_encoding = parse_transfer_encoding(&self.headers)?;
        if content_length.is_some() && transfer_encoding.is_some() {
            return Err(HeadError::AmbiguousMessageLength);
        }
        if transfer_encoding.is_some() && self.version != Version::HTTP_11 {
            return Err(HeadError::InvalidTransferEncoding);
        }

        let connection = connection_options(&self.headers)?;
        let persistence = persistence(self.version, connection);
        let expectation = expectation(&self.headers, self.version, received)?;
        // HTTP/1.0 recipients ignore Upgrade, including a Connection option
        // naming it. CONNECT tunneling is independent of protocol upgrade.
        let upgrade = if received && self.version == Version::HTTP_10 && target_form != TargetForm::Authority {
            None
        } else {
            upgrade_intent(&self.headers, connection, target_form)?
        };
        if upgrade == Some(UpgradeKind::Protocol) && self.version != Version::HTTP_11 {
            return Err(HeadError::InvalidUpgrade);
        }
        let body = match (transfer_encoding, content_length) {
            (Some(TransferFraming::Chunked), None) => BodyMode::Chunked,
            (None, Some(length)) => BodyMode::Fixed(length),
            (None, None) => BodyMode::None,
            (Some(TransferFraming::Chunked), Some(_)) => unreachable!("conflict checked above"),
        };

        Ok(ValidatedRequestHead {
            head: self,
            body,
            persistence,
            target_form,
            expectation,
            upgrade,
        })
    }
}

impl ResponseHead {
    pub(super) fn validate(self, request_method: &Method) -> Result<ValidatedResponseHead, HeadError> {
        if self.status.as_u16() > 599 {
            return Err(HeadError::InvalidStatus);
        }
        let connection = connection_options(&self.headers)?;
        let mut persistence = persistence(self.version, connection);
        let upgrade = if self.status == StatusCode::SWITCHING_PROTOCOLS {
            if self.version != Version::HTTP_11 {
                return Err(HeadError::InvalidUpgrade);
            }
            validate_protocol_upgrade(&self.headers, connection)?;
            Some(UpgradeKind::Protocol)
        } else if request_method == Method::CONNECT && self.status.is_success() {
            Some(UpgradeKind::Tunnel)
        } else {
            None
        };
        let body = if upgrade.is_some()
            || request_method == Method::HEAD
            || self.status.is_informational()
            || self.status == StatusCode::NO_CONTENT
            || self.status == StatusCode::NOT_MODIFIED
        {
            BodyMode::None
        } else {
            let content_length = parse_content_length(&self.headers)?;
            let transfer_encoding = parse_transfer_encoding(&self.headers)?;
            if content_length.is_some() && transfer_encoding.is_some() {
                return Err(HeadError::AmbiguousMessageLength);
            }
            if transfer_encoding.is_some() && self.version != Version::HTTP_11 {
                return Err(HeadError::InvalidTransferEncoding);
            }
            match (transfer_encoding, content_length) {
                (Some(TransferFraming::Chunked), None) => BodyMode::Chunked,
                (None, Some(length)) => BodyMode::Fixed(length),
                (None, None) => BodyMode::UntilEof,
                (Some(TransferFraming::Chunked), Some(_)) => {
                    unreachable!("conflict checked above")
                }
            }
        };

        if matches!(body, BodyMode::UntilEof) {
            persistence = Persistence::Close;
        }

        Ok(ValidatedResponseHead {
            upgrade,
            head: self,
            body,
            persistence,
        })
    }
}

#[derive(Debug)]
pub(super) struct HeaderWorkspace {
    // The fixed allocation is reused, but no parsed reference is retained
    // after the parsing call that initialized it.
    slots: Box<[MaybeUninit<httparse::Header<'static>>]>,
}

impl HeaderWorkspace {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            slots: vec![MaybeUninit::uninit(); capacity].into_boxed_slice(),
        }
    }

    fn for_input(&mut self) -> &mut [MaybeUninit<httparse::Header<'_>>] {
        let pointer = self.slots.as_mut_ptr().cast();
        let length = self.slots.len();

        // SAFETY: changing only a reference lifetime cannot change the layout
        // of `httparse::Header`. The caller borrows this workspace exclusively
        // for the parse, converts every returned header to owned `http` values,
        // and calls `clear` before the input can be released. `Header` contains
        // no value that needs to be dropped.
        unsafe { slice::from_raw_parts_mut(pointer, length) }
    }

    pub(super) fn for_headers(&mut self) -> &mut [httparse::Header<'_>] {
        self.slots.fill(MaybeUninit::new(httparse::EMPTY_HEADER));
        let pointer = self.slots.as_mut_ptr().cast();
        let length = self.slots.len();

        // SAFETY: every slot was initialized to `EMPTY_HEADER`, and changing
        // only its reference lifetime preserves layout. The returned slice is
        // exclusively borrowed, parsed references are converted to owned
        // `http` values, and `clear` runs before the input can be released.
        unsafe { slice::from_raw_parts_mut(pointer, length) }
    }

    pub(super) fn clear(&mut self) {
        self.slots.fill(MaybeUninit::uninit());
    }
}

fn build_shared_request_head(request: httparse::Request<'_, '_>, input: &Bytes) -> Result<RequestHead, HeadError> {
    let method = Method::from_bytes(request.method.ok_or(HeadError::InvalidStartLine)?.as_bytes())
        .map_err(|_| HeadError::InvalidMethod)?;
    let target = request.path.ok_or(HeadError::InvalidStartLine)?;
    let target = Uri::from_maybe_shared(input.slice_ref(target.as_bytes())).map_err(|_| HeadError::InvalidTarget)?;
    let version = version(request.version.ok_or(HeadError::InvalidStartLine)?)?;
    let headers = shared_headers(request.headers, input)?;

    Ok(RequestHead {
        method,
        target,
        version,
        headers,
    })
}

fn build_shared_response_head(response: httparse::Response<'_, '_>, input: &Bytes) -> Result<ResponseHead, HeadError> {
    let version = version(response.version.ok_or(HeadError::InvalidStartLine)?)?;
    let code = response.code.ok_or(HeadError::InvalidStartLine)?;
    if code > 599 {
        return Err(HeadError::InvalidStatus);
    }
    let status = StatusCode::from_u16(code).map_err(|_| HeadError::InvalidStatus)?;
    let headers = shared_headers(response.headers, input)?;

    Ok(ResponseHead {
        version,
        status,
        headers,
    })
}

pub(super) fn own_headers(headers: &[httparse::Header<'_>]) -> Result<HeaderMap, HeadError> {
    let mut owned = HeaderMap::try_with_capacity(headers.len()).map_err(|_| HeadError::Limit(HeadLimit::HeaderMap))?;
    for header in headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| HeadError::InvalidHeader)?;
        let value = HeaderValue::from_bytes(header.value).map_err(|_| HeadError::InvalidHeader)?;
        owned.append(name, value);
    }
    Ok(owned)
}

fn shared_headers(headers: &[httparse::Header<'_>], input: &Bytes) -> Result<HeaderMap, HeadError> {
    let mut owned = HeaderMap::try_with_capacity(headers.len()).map_err(|_| HeadError::Limit(HeadLimit::HeaderMap))?;
    for header in headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| HeadError::InvalidHeader)?;
        let value =
            HeaderValue::from_maybe_shared(input.slice_ref(header.value)).map_err(|_| HeadError::InvalidHeader)?;
        owned.append(name, value);
    }
    Ok(owned)
}

fn version(minor: u8) -> Result<Version, HeadError> {
    match minor {
        0 => Ok(Version::HTTP_10),
        1 => Ok(Version::HTTP_11),
        _ => Err(HeadError::UnsupportedVersion),
    }
}

fn map_request_error(error: httparse::Error) -> HeadError {
    match error {
        httparse::Error::TooManyHeaders => HeadError::Limit(HeadLimit::Headers),
        httparse::Error::HeaderName | httparse::Error::HeaderValue => HeadError::InvalidHeader,
        httparse::Error::Version => HeadError::UnsupportedVersion,
        httparse::Error::NewLine | httparse::Error::Token | httparse::Error::Status => HeadError::InvalidStartLine,
    }
}

fn map_response_error(error: httparse::Error) -> HeadError {
    match error {
        httparse::Error::TooManyHeaders => HeadError::Limit(HeadLimit::Headers),
        httparse::Error::HeaderName | httparse::Error::HeaderValue => HeadError::InvalidHeader,
        httparse::Error::Version => HeadError::UnsupportedVersion,
        httparse::Error::NewLine | httparse::Error::Status | httparse::Error::Token => HeadError::InvalidStartLine,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransferFraming {
    Chunked,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ConnectionOptions {
    close: bool,
    keep_alive: bool,
    upgrade: bool,
}

fn validate_request_target(method: &Method, target: &Uri) -> Result<TargetForm, HeadError> {
    let form = if target.path() == "*" && target.scheme().is_none() && target.authority().is_none() {
        TargetForm::Asterisk
    } else if target.scheme().is_some() && target.authority().is_some() {
        TargetForm::Absolute
    } else if target.scheme().is_none() && target.authority().is_some() && target.path_and_query().is_none() {
        TargetForm::Authority
    } else if target.scheme().is_none() && target.authority().is_none() && target.path().starts_with('/') {
        TargetForm::Origin
    } else {
        return Err(HeadError::InvalidTargetForm);
    };

    match (method, form) {
        (&Method::CONNECT, TargetForm::Authority) if target.authority().is_some_and(|a| a.port().is_some()) => Ok(form),
        (&Method::CONNECT, _) => Err(HeadError::InvalidTargetForm),
        (&Method::OPTIONS, TargetForm::Asterisk) => Ok(form),
        (_, TargetForm::Asterisk | TargetForm::Authority) => Err(HeadError::InvalidTargetForm),
        _ => Ok(form),
    }
}

fn normalize_absolute_host(headers: &mut HeaderMap, target: &Uri) -> Result<(), HeadError> {
    let authority = target.authority().ok_or(HeadError::InvalidTargetForm)?;
    if authority.as_str().contains('@') {
        return Err(HeadError::InvalidTarget);
    }
    let value = HeaderValue::from_str(authority.as_str()).map_err(|_| HeadError::InvalidHost)?;
    headers.insert(HOST, value);
    Ok(())
}

fn validate_host(headers: &HeaderMap, version: Version) -> Result<(), HeadError> {
    let mut values = headers.get_all(HOST).iter();
    let first = values.next();
    if first.is_none() {
        return if version == Version::HTTP_11 {
            Err(HeadError::MissingHost)
        } else {
            Ok(())
        };
    }
    if values.next().is_some() {
        return Err(HeadError::InvalidHost);
    }

    let bytes = trim_ows(first.expect("checked above").as_bytes());
    if bytes.is_empty() || bytes.contains(&b',') || bytes.contains(&b'@') {
        return Err(HeadError::InvalidHost);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| HeadError::InvalidHost)?;
    text.parse::<Authority>()
        .map(|_| ())
        .map_err(|_| HeadError::InvalidHost)
}

pub(super) fn parse_content_length(headers: &HeaderMap) -> Result<Option<u64>, HeadError> {
    let mut parsed = None;
    let mut present = false;
    for value in headers.get_all(CONTENT_LENGTH) {
        present = true;
        for_each_list_member(value.as_bytes(), HeadError::InvalidContentLength, |member| {
            let member = trim_ows(member);
            if member.is_empty() || !member.iter().all(u8::is_ascii_digit) {
                return Err(HeadError::InvalidContentLength);
            }
            let value = member.iter().try_fold(0_u64, |value, digit| {
                value
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(u64::from(digit - b'0')))
                    .ok_or(HeadError::InvalidContentLength)
            })?;
            if parsed.is_some_and(|parsed| parsed != value) {
                return Err(HeadError::AmbiguousMessageLength);
            }
            parsed = Some(value);
            Ok(())
        })?;
    }
    if present && parsed.is_none() {
        Err(HeadError::InvalidContentLength)
    } else {
        Ok(parsed)
    }
}

pub(super) fn parse_transfer_encoding(headers: &HeaderMap) -> Result<Option<TransferFraming>, HeadError> {
    let mut codings = 0_usize;
    let mut chunked_at = None;
    let mut unsupported = false;

    for value in headers.get_all(TRANSFER_ENCODING) {
        for_each_list_member(value.as_bytes(), HeadError::InvalidTransferEncoding, |member| {
            let member = trim_ows(member);
            let (coding, parameters) = split_coding(member)?;
            if !is_token(coding) {
                return Err(HeadError::InvalidTransferEncoding);
            }
            validate_transfer_parameters(parameters)?;

            codings = codings.checked_add(1).ok_or(HeadError::InvalidTransferEncoding)?;
            if coding.eq_ignore_ascii_case(b"chunked") {
                if chunked_at.is_some() || !parameters.is_empty() {
                    return Err(HeadError::InvalidTransferEncoding);
                }
                chunked_at = Some(codings);
            } else {
                unsupported = true;
            }
            Ok(())
        })?;
    }

    if codings == 0 {
        return Ok(None);
    }
    if chunked_at != Some(codings) {
        return Err(HeadError::InvalidTransferEncoding);
    }
    if unsupported {
        return Err(HeadError::UnsupportedTransferCoding);
    }
    Ok(Some(TransferFraming::Chunked))
}

fn split_coding(member: &[u8]) -> Result<(&[u8], &[u8]), HeadError> {
    let split = member.iter().position(|byte| *byte == b';').unwrap_or(member.len());
    let coding = trim_ows(&member[..split]);
    let parameters = &member[split..];
    if coding.is_empty() {
        Err(HeadError::InvalidTransferEncoding)
    } else {
        Ok((coding, parameters))
    }
}

fn validate_transfer_parameters(mut input: &[u8]) -> Result<(), HeadError> {
    while !input.is_empty() {
        if input[0] != b';' {
            return Err(HeadError::InvalidTransferEncoding);
        }
        input = trim_ows(&input[1..]);
        let name_len = input.iter().take_while(|byte| is_tchar(**byte)).count();
        if name_len == 0 {
            return Err(HeadError::InvalidTransferEncoding);
        }
        input = trim_ows(&input[name_len..]);
        if input.first() != Some(&b'=') {
            return Err(HeadError::InvalidTransferEncoding);
        }
        input = trim_ows(&input[1..]);

        let value_len = if input.first() == Some(&b'"') {
            quoted_string_len(input).ok_or(HeadError::InvalidTransferEncoding)?
        } else {
            let length = input.iter().take_while(|byte| is_tchar(**byte)).count();
            if length == 0 {
                return Err(HeadError::InvalidTransferEncoding);
            }
            length
        };
        input = trim_ows(&input[value_len..]);
        if !input.is_empty() && input[0] != b';' {
            return Err(HeadError::InvalidTransferEncoding);
        }
    }
    Ok(())
}

fn connection_options(headers: &HeaderMap) -> Result<ConnectionOptions, HeadError> {
    let mut options = ConnectionOptions::default();
    for value in headers.get_all(CONNECTION) {
        for_each_list_member(value.as_bytes(), HeadError::InvalidConnection, |member| {
            let token = trim_ows(member);
            if !is_token(token) {
                return Err(HeadError::InvalidConnection);
            }
            if token.eq_ignore_ascii_case(b"close") {
                options.close = true;
            } else if token.eq_ignore_ascii_case(b"keep-alive") {
                options.keep_alive = true;
            } else if token.eq_ignore_ascii_case(b"upgrade") {
                options.upgrade = true;
            }
            Ok(())
        })?;
    }
    Ok(options)
}

fn persistence(version: Version, options: ConnectionOptions) -> Persistence {
    if options.close || (version == Version::HTTP_10 && !options.keep_alive) {
        Persistence::Close
    } else {
        Persistence::Reusable
    }
}

fn expectation(headers: &HeaderMap, version: Version, received: bool) -> Result<Expectation, HeadError> {
    let mut continue_count = 0_usize;
    for value in headers.get_all(EXPECT) {
        for_each_list_member(value.as_bytes(), HeadError::InvalidExpectation, |member| {
            let token = trim_ows(member);
            if !token.eq_ignore_ascii_case(b"100-continue") {
                return Err(HeadError::InvalidExpectation);
            }
            continue_count = continue_count.checked_add(1).ok_or(HeadError::InvalidExpectation)?;
            Ok(())
        })?;
    }
    match (continue_count, version) {
        (0, _) => Ok(Expectation::None),
        (_, Version::HTTP_11) => Ok(Expectation::Continue),
        (_, Version::HTTP_10) if received => Ok(Expectation::None),
        _ => Err(HeadError::InvalidExpectation),
    }
}

fn upgrade_intent(
    headers: &HeaderMap,
    connection: ConnectionOptions,
    target_form: TargetForm,
) -> Result<Option<UpgradeKind>, HeadError> {
    if target_form == TargetForm::Authority {
        return Ok(Some(UpgradeKind::Tunnel));
    }
    let has_upgrade = validate_upgrade_values(headers)?;
    match (connection.upgrade, connection.close, has_upgrade) {
        (true, false, true) => Ok(Some(UpgradeKind::Protocol)),
        (false, _, false) => Ok(None),
        _ => Err(HeadError::InvalidUpgrade),
    }
}

fn validate_protocol_upgrade(headers: &HeaderMap, connection: ConnectionOptions) -> Result<(), HeadError> {
    if connection.upgrade && !connection.close && validate_upgrade_values(headers)? {
        Ok(())
    } else {
        Err(HeadError::InvalidUpgrade)
    }
}

fn validate_upgrade_values(headers: &HeaderMap) -> Result<bool, HeadError> {
    let mut protocols = 0_usize;
    for value in headers.get_all(UPGRADE) {
        for_each_list_member(value.as_bytes(), HeadError::InvalidUpgrade, |member| {
            let member = trim_ows(member);
            let mut parts = member.split(|byte| *byte == b'/');
            let protocol = parts.next().unwrap_or_default();
            let version = parts.next();
            if !is_token(protocol) || version.is_some_and(|version| !is_token(version)) || parts.next().is_some() {
                return Err(HeadError::InvalidUpgrade);
            }
            protocols = protocols.checked_add(1).ok_or(HeadError::InvalidUpgrade)?;
            Ok(())
        })?;
    }
    Ok(protocols != 0)
}

fn for_each_list_member(
    input: &[u8],
    error: HeadError,
    mut handle: impl FnMut(&[u8]) -> Result<(), HeadError>,
) -> Result<(), HeadError> {
    if input.is_empty() {
        return Err(error);
    }
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in input.iter().copied().enumerate() {
        if escaped {
            escaped = false;
        } else if quoted && byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if byte == b',' && !quoted {
            if index == start {
                return Err(error);
            }
            handle(&input[start..index])?;
            start = index + 1;
        }
    }
    if quoted || escaped || start == input.len() {
        return Err(error);
    }
    handle(&input[start..])
}

fn is_token(value: &[u8]) -> bool {
    !value.is_empty() && value.iter().all(|byte| is_tchar(*byte))
}

#[cfg(test)]
mod tests {
    use super::{HeadError, HeadLimit, HeadLimits, HeadParser, RequestHead, RequestRole, ResponseHead, ResponseRole};
    use crate::error::{Error, ErrorKind};
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
    fn completed_head_owns_all_parsed_data() {
        let mut parser = HeadParser::<RequestRole>::request(LIMITS);
        let head = {
            let input = b"POST /owned HTTP/1.1\r\nHost: owned.test\r\n\r\n".to_vec();
            let consumed = parser.head_len(&input).unwrap().expect("complete request head");
            parser.parse_shared(Bytes::copy_from_slice(&input[..consumed])).unwrap()
        };

        assert_eq!(head.target, "/owned");
        assert_eq!(head.headers[HOST], "owned.test");
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
    fn fragmented_heads_scan_only_new_bytes_before_parsing() {
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
        for status in [100, 204, 304] {
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
        assert_eq!(reset.body, BodyMode::UntilEof);
        assert_eq!(reset.persistence, Persistence::Close);
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
