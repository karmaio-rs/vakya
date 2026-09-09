use super::syntax::{is_tchar, quoted_string_len, trim_ows};
use super::{BodyMode, Expectation, Persistence, TargetForm, UpgradeKind};
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, Version,
    header::{CONNECTION, CONTENT_LENGTH, EXPECT, HOST, TRANSFER_ENCODING, UPGRADE},
    uri::Authority,
};
use std::{marker::PhantomData, mem::MaybeUninit, slice};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct HeadLimits {
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

impl From<HeadError> for crate::Error {
    fn from(error: HeadError) -> Self {
        let kind = match error {
            HeadError::Limit(_) => crate::ErrorKind::Limit,
            HeadError::UnsupportedVersion | HeadError::UnsupportedTransferCoding => crate::ErrorKind::Unsupported,
            _ => crate::ErrorKind::InvalidMessage,
        };
        Self::with_source(kind, "received HTTP head failed validation", error)
    }
}

#[derive(Debug)]
pub(super) enum ParseOutcome<T> {
    NeedMore,
    Complete { head: T, consumed: usize },
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

/// Incremental parser over a retained input prefix. After `NeedMore`, the next
/// call must retain the same bytes and append new input. On completion or error
/// the scan resets and the workspace can parse a different head. Returned heads
/// own their fields; `consumed` leaves body or upgraded-protocol bytes untouched.
#[derive(Debug)]
pub(super) struct HeadParser<R> {
    limits: HeadLimits,
    workspace: HeaderWorkspace,
    role: PhantomData<fn() -> R>,
    partial: Option<HeadScan>,
    #[cfg(test)]
    parse_calls: usize,
}

#[derive(Debug, Default)]
struct HeadScan {
    scanned: usize,
    line: ScanLine,
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
    fn complete(&mut self, input: &[u8]) -> bool {
        if input.len() < self.scanned {
            *self = Self::default();
        }
        for &byte in &input[self.scanned..] {
            self.scanned += 1;
            self.line = match (&self.line, byte) {
                (ScanLine::Leading, b'\r' | b'\n') => ScanLine::Leading,
                (ScanLine::End | ScanLine::EndCr, b'\n') => return true,
                (ScanLine::End, b'\r') => ScanLine::EndCr,
                (_, b'\n') => ScanLine::End,
                _ => ScanLine::Content,
            };
        }
        false
    }
}

impl<R> HeadParser<R> {
    fn with_limits(limits: HeadLimits) -> Self {
        Self {
            limits,
            workspace: HeaderWorkspace::new(limits.max_headers),
            role: PhantomData,
            partial: None,
            #[cfg(test)]
            parse_calls: 0,
        }
    }

    fn parse_input_len(&self, input: &[u8]) -> usize {
        input.len().min(self.limits.max_bytes)
    }

    fn ready_to_parse(&mut self, input: &[u8]) -> bool {
        let bounded = &input[..self.parse_input_len(input)];
        self.partial.as_mut().is_none_or(|scan| scan.complete(bounded)) || input.len() >= self.limits.max_bytes
    }

    fn parsed<T>(&mut self, input: &[u8], result: &Result<ParseOutcome<T>, HeadError>) {
        #[cfg(test)]
        {
            self.parse_calls += 1;
        }
        if matches!(result, Ok(ParseOutcome::NeedMore)) {
            let mut scan = HeadScan::default();
            scan.complete(input);
            self.partial = Some(scan);
        } else {
            self.partial = None;
        }
    }

    fn incomplete<T>(&self, input_len: usize) -> Result<ParseOutcome<T>, HeadError> {
        if input_len > self.limits.max_bytes {
            Err(HeadError::Limit(HeadLimit::Bytes))
        } else {
            Ok(ParseOutcome::NeedMore)
        }
    }
}

impl HeadParser<RequestRole> {
    pub(super) fn request(limits: HeadLimits) -> Self {
        Self::with_limits(limits)
    }

    pub(super) fn parse(&mut self, input: &[u8]) -> Result<ParseOutcome<RequestHead>, HeadError> {
        if !self.ready_to_parse(input) {
            return self.incomplete(input.len());
        }
        let parse_len = self.parse_input_len(input);
        let parsed = {
            let mut request = httparse::Request::new(&mut []);
            let workspace = self.workspace.for_input();
            let status = httparse::ParserConfig::default()
                .parse_request_with_uninit_headers(&mut request, &input[..parse_len], workspace)
                .map_err(map_request_error);

            match status {
                Ok(httparse::Status::Complete(consumed)) => {
                    build_request_head(request).map(|head| ParseOutcome::Complete { head, consumed })
                }
                Ok(httparse::Status::Partial) => self.incomplete(input.len()),
                Err(error) => Err(error),
            }
        };
        self.workspace.clear();
        self.parsed(input, &parsed);
        parsed
    }
}

impl HeadParser<ResponseRole> {
    pub(super) fn response(limits: HeadLimits) -> Self {
        Self::with_limits(limits)
    }

    pub(super) fn parse(&mut self, input: &[u8]) -> Result<ParseOutcome<ResponseHead>, HeadError> {
        if !self.ready_to_parse(input) {
            return self.incomplete(input.len());
        }
        let parse_len = self.parse_input_len(input);
        let parsed = {
            let mut response = httparse::Response::new(&mut []);
            let workspace = self.workspace.for_input();
            let status = httparse::ParserConfig::default()
                .parse_response_with_uninit_headers(&mut response, &input[..parse_len], workspace)
                .map_err(map_response_error);

            match status {
                Ok(httparse::Status::Complete(consumed)) => {
                    build_response_head(response).map(|head| ParseOutcome::Complete { head, consumed })
                }
                Ok(httparse::Status::Partial) => self.incomplete(input.len()),
                Err(error) => Err(error),
            }
        };
        self.workspace.clear();
        self.parsed(input, &parsed);
        parsed
    }
}

impl RequestHead {
    pub(super) fn validate(mut self) -> Result<ValidatedRequestHead, HeadError> {
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
        let expectation = expectation(&self.headers, self.version)?;
        let upgrade = upgrade_intent(&self.headers, connection, target_form)?;
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

fn build_request_head(request: httparse::Request<'_, '_>) -> Result<RequestHead, HeadError> {
    let method = Method::from_bytes(request.method.ok_or(HeadError::InvalidStartLine)?.as_bytes())
        .map_err(|_| HeadError::InvalidMethod)?;
    let target = request
        .path
        .ok_or(HeadError::InvalidStartLine)?
        .parse::<Uri>()
        .map_err(|_| HeadError::InvalidTarget)?;
    let version = version(request.version.ok_or(HeadError::InvalidStartLine)?)?;
    let headers = own_headers(request.headers)?;

    Ok(RequestHead {
        method,
        target,
        version,
        headers,
    })
}

fn build_response_head(response: httparse::Response<'_, '_>) -> Result<ResponseHead, HeadError> {
    let version = version(response.version.ok_or(HeadError::InvalidStartLine)?)?;
    let code = response.code.ok_or(HeadError::InvalidStartLine)?;
    if code > 599 {
        return Err(HeadError::InvalidStatus);
    }
    let status = StatusCode::from_u16(code).map_err(|_| HeadError::InvalidStatus)?;
    let headers = own_headers(response.headers)?;

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

fn expectation(headers: &HeaderMap, version: Version) -> Result<Expectation, HeadError> {
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
    use super::{
        HeadError, HeadLimit, HeadLimits, HeadParser, ParseOutcome, RequestHead, RequestRole, ResponseHead,
        ResponseRole,
    };
    use crate::proto::h1::{BodyMode, Expectation, Persistence, TargetForm, UpgradeKind};
    use http::{Method, StatusCode, Version, header::HOST};

    const LIMITS: HeadLimits = HeadLimits::new(1024, 8);

    #[test]
    fn request_head_is_incremental_at_every_split() {
        let input = b"GET /items?q=rust HTTP/1.1\r\nHost: example.test\r\nX-Test: yes\r\n\r\nbody";

        for split in 0..input.len() - 4 {
            let mut parser = HeadParser::<RequestRole>::request(LIMITS);
            assert!(matches!(parser.parse(&input[..split]).unwrap(), ParseOutcome::NeedMore));
        }

        let mut parser = HeadParser::<RequestRole>::request(LIMITS);
        let ParseOutcome::Complete { head, consumed } = parser.parse(input).unwrap() else {
            panic!("complete request was not parsed");
        };
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
            assert!(matches!(parser.parse(&input[..split]).unwrap(), ParseOutcome::NeedMore));
        }

        let mut parser = HeadParser::<ResponseRole>::response(LIMITS);
        let ParseOutcome::Complete { head, consumed } = parser.parse(input).unwrap() else {
            panic!("complete response was not parsed");
        };
        assert_eq!(head.status, StatusCode::NO_CONTENT);
        assert_eq!(head.version, Version::HTTP_10);
        assert_eq!(&input[consumed..], b"next");
    }

    #[test]
    fn completed_head_owns_all_parsed_data() {
        let mut parser = HeadParser::<RequestRole>::request(LIMITS);
        let head = {
            let input = b"POST /owned HTTP/1.1\r\nHost: owned.test\r\n\r\n".to_vec();
            let ParseOutcome::Complete { head, .. } = parser.parse(&input).unwrap() else {
                panic!("complete request was not parsed");
            };
            head
        };

        assert_eq!(head.target, "/owned");
        assert_eq!(head.headers[HOST], "owned.test");
    }

    #[test]
    fn head_and_header_count_limits_are_enforced_at_boundaries() {
        let input = b"GET / HTTP/1.1\r\nA: 1\r\nB: 2\r\n\r\n";
        let mut one_header = HeadParser::<RequestRole>::request(HeadLimits::new(1024, 1));
        assert_eq!(
            one_header.parse(input).unwrap_err(),
            HeadError::Limit(HeadLimit::Headers)
        );

        let exact = b"GET / HTTP/1.1\r\n\r\n";
        let mut exact_parser = HeadParser::<RequestRole>::request(HeadLimits::new(exact.len(), 1));
        assert!(matches!(
            exact_parser.parse(exact).unwrap(),
            ParseOutcome::Complete { .. }
        ));

        let mut short_parser = HeadParser::<RequestRole>::request(HeadLimits::new(exact.len() - 1, 1));
        assert_eq!(
            short_parser.parse(exact).unwrap_err(),
            HeadError::Limit(HeadLimit::Bytes)
        );
    }

    #[test]
    fn unsupported_versions_and_malformed_start_lines_are_typed() {
        let mut request = HeadParser::<RequestRole>::request(LIMITS);
        assert_eq!(
            request.parse(b"GET / HTTP/2.0\r\n\r\n").unwrap_err(),
            HeadError::UnsupportedVersion
        );
        assert_eq!(
            request.parse(b"GET  HTTP/1.1\r\n\r\n").unwrap_err(),
            HeadError::InvalidStartLine
        );

        let mut response = HeadParser::<ResponseRole>::response(LIMITS);
        assert_eq!(
            response.parse(b"HTTP/1.1 xyz Bad\r\n\r\n").unwrap_err(),
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
                assert!(matches!(
                    parser.parse(&wire.as_bytes()[..end]).unwrap(),
                    ParseOutcome::NeedMore
                ));
            }
            assert!(matches!(
                parser.parse(wire.as_bytes()).unwrap(),
                ParseOutcome::Complete { .. }
            ));
            assert_eq!(parser.parse_calls, 2);
            // A completed parser can immediately read the next head.
            assert!(matches!(
                parser.parse(wire.as_bytes()).unwrap(),
                ParseOutcome::Complete { .. }
            ));
        }
    }

    #[test]
    fn parser_workspace_can_be_reused_after_partial_and_error() {
        let mut parser = HeadParser::<RequestRole>::request(LIMITS);
        assert!(matches!(
            parser.parse(b"GET / HTTP/1.1\r\n").unwrap(),
            ParseOutcome::NeedMore
        ));
        assert!(parser.parse(b"bad start\r\n\r\n").is_err());

        let result = parser.parse(b"GET /ok HTTP/1.1\r\nHost: example.test\r\n\r\n");
        assert!(matches!(result.unwrap(), ParseOutcome::Complete { .. }));
    }

    #[test]
    fn public_error_preserves_head_category_and_source() {
        let error: crate::Error = HeadError::Limit(HeadLimit::Headers).into();
        assert_eq!(error.kind(), crate::ErrorKind::Limit);
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
        assert_eq!(
            parser.parse(b"HTTP/1.1 600 Future\r\n\r\n").unwrap_err(),
            HeadError::InvalidStatus
        );
        let parsed = response(b"HTTP/1.1 599 Edge\r\nContent-Length: 0\r\n\r\n");
        assert_eq!(parsed.status.as_u16(), 599);
    }

    #[test]
    fn obsolete_folding_and_whitespace_before_colon_are_rejected() {
        let mut parser = HeadParser::<RequestRole>::request(LIMITS);
        assert_eq!(
            parser
                .parse(b"GET / HTTP/1.1\r\nHost : example.test\r\n\r\n")
                .unwrap_err(),
            HeadError::InvalidHeader
        );
        assert_eq!(
            parser
                .parse(b"GET / HTTP/1.1\r\nHost: example.test\r\n folded\r\n\r\n")
                .unwrap_err(),
            HeadError::InvalidHeader
        );
    }

    fn request(input: &[u8]) -> RequestHead {
        let mut parser = HeadParser::<RequestRole>::request(HeadLimits::new(4096, 16));
        let ParseOutcome::Complete { head, .. } = parser.parse(input).unwrap() else {
            panic!("request fixture was incomplete");
        };
        head
    }

    fn response(input: &[u8]) -> ResponseHead {
        let mut parser = HeadParser::<ResponseRole>::response(HeadLimits::new(4096, 16));
        let ParseOutcome::Complete { head, .. } = parser.parse(input).unwrap() else {
            panic!("response fixture was incomplete");
        };
        head
    }
}
