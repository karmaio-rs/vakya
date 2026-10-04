//! HTTP field and message semantics, independent of parser storage.
//! Locally generated and received requests keep separate validation entry points.
use super::super::syntax::{is_tchar, quoted_string_len, trim_ows};
use super::super::{BodyMode, Expectation, Persistence, TargetForm, UpgradeKind};
use super::{HeadError, RequestHead, ResponseHead, ValidatedRequestHead, ValidatedResponseHead};
use http::{
    HeaderMap, HeaderValue, Method, StatusCode, Uri, Version,
    header::{CONNECTION, CONTENT_LENGTH, EXPECT, HOST, TRANSFER_ENCODING, UPGRADE},
    uri::Authority,
};

impl RequestHead {
    pub(in crate::proto::h1) fn validate(self) -> Result<ValidatedRequestHead, HeadError> {
        self.validate_for(false)
    }

    /// Apply recipient semantics without weakening locally generated requests.
    #[cfg(any(feature = "server", test))]
    pub(in crate::proto::h1) fn validate_received(self) -> Result<ValidatedRequestHead, HeadError> {
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
    pub(in crate::proto::h1) fn validate(self, request_method: &Method) -> Result<ValidatedResponseHead, HeadError> {
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
            || self.status == StatusCode::RESET_CONTENT
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::proto::h1) enum TransferFraming {
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

pub(in crate::proto::h1) fn parse_content_length(headers: &HeaderMap) -> Result<Option<u64>, HeadError> {
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

pub(in crate::proto::h1) fn parse_transfer_encoding(headers: &HeaderMap) -> Result<Option<TransferFraming>, HeadError> {
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
