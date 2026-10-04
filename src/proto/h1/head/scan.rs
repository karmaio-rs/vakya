//! Incremental head scanning, syntax parsing, and owned materialization.
//! All lifetime-erased httparse workspace slots remain in this module.
use super::super::header_case::HeaderCaseMap;
use super::{HeadError, HeadLimit, HeadLimits, RequestHead, ResponseHead};
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, Version};
use std::{marker::PhantomData, mem::MaybeUninit, slice};

#[derive(Debug)]
pub(in crate::proto::h1) enum RequestRole {}

#[derive(Debug)]
pub(in crate::proto::h1) enum ResponseRole {}

/// Incremental parser over a retained input prefix. After an incomplete scan,
/// the next call must retain the same bytes and append new input. Once
/// `head_len` returns a prefix length, the caller transfers exactly that prefix
/// to `parse_shared`, leaving body or upgraded-protocol bytes untouched.
#[derive(Debug)]
pub(in crate::proto::h1) struct HeadParser<R> {
    limits: HeadLimits,
    workspace: HeaderWorkspace,
    role: PhantomData<fn() -> R>,
    scan: HeadScan,
    preserve_header_case: bool,
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
            preserve_header_case: false,
        }
    }

    pub(in crate::proto::h1) fn with_preserved_header_case(mut self, enabled: bool) -> Self {
        self.preserve_header_case = enabled;
        self
    }

    fn parse_input_len(&self, input: &[u8]) -> usize {
        input.len().min(self.limits.max_bytes)
    }

    /// Find a complete retained head before transferring its backing storage.
    /// Repeated calls return the same length until the caller passes that exact
    /// prefix to the role's `parse_shared`.
    pub(in crate::proto::h1) fn head_len(&mut self, input: &[u8]) -> Result<Option<usize>, HeadError> {
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
    pub(in crate::proto::h1) fn request(limits: HeadLimits) -> Self {
        Self::with_limits(limits)
    }

    pub(in crate::proto::h1) fn parse_shared(&mut self, input: Bytes) -> Result<RequestHead, HeadError> {
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
                    build_shared_request_head(request, &input, self.preserve_header_case)
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
    pub(in crate::proto::h1) fn response(limits: HeadLimits) -> Self {
        Self::with_limits(limits)
    }

    pub(in crate::proto::h1) fn parse_shared(&mut self, input: Bytes) -> Result<ResponseHead, HeadError> {
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
                    build_shared_response_head(response, &input, self.preserve_header_case)
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

#[derive(Debug)]
pub(in crate::proto::h1) struct HeaderWorkspace {
    // The fixed allocation is reused, but no parsed reference is retained
    // after the parsing call that initialized it.
    slots: Box<[MaybeUninit<httparse::Header<'static>>]>,
}

impl HeaderWorkspace {
    pub(in crate::proto::h1) fn new(capacity: usize) -> Self {
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

    pub(in crate::proto::h1) fn for_headers(&mut self) -> &mut [httparse::Header<'_>] {
        self.slots.fill(MaybeUninit::new(httparse::EMPTY_HEADER));
        let pointer = self.slots.as_mut_ptr().cast();
        let length = self.slots.len();

        // SAFETY: every slot was initialized to `EMPTY_HEADER`, and changing
        // only its reference lifetime preserves layout. The returned slice is
        // exclusively borrowed, parsed references are converted to owned
        // `http` values, and `clear` runs before the input can be released.
        unsafe { slice::from_raw_parts_mut(pointer, length) }
    }

    pub(in crate::proto::h1) fn clear(&mut self) {
        self.slots.fill(MaybeUninit::uninit());
    }
}

fn build_shared_request_head(
    request: httparse::Request<'_, '_>,
    input: &Bytes,
    preserve_header_case: bool,
) -> Result<RequestHead, HeadError> {
    let method = Method::from_bytes(request.method.ok_or(HeadError::InvalidStartLine)?.as_bytes())
        .map_err(|_| HeadError::InvalidMethod)?;
    let target = request.path.ok_or(HeadError::InvalidStartLine)?;
    let target = Uri::from_maybe_shared(input.slice_ref(target.as_bytes())).map_err(|_| HeadError::InvalidTarget)?;
    let version = version(request.version.ok_or(HeadError::InvalidStartLine)?)?;
    let (headers, header_case) = shared_headers(request.headers, input, preserve_header_case)?;

    Ok(RequestHead {
        method,
        target,
        version,
        headers,
        header_case,
    })
}

fn build_shared_response_head(
    response: httparse::Response<'_, '_>,
    input: &Bytes,
    preserve_header_case: bool,
) -> Result<ResponseHead, HeadError> {
    let version = version(response.version.ok_or(HeadError::InvalidStartLine)?)?;
    let code = response.code.ok_or(HeadError::InvalidStartLine)?;
    if code > 599 {
        return Err(HeadError::InvalidStatus);
    }
    let status = StatusCode::from_u16(code).map_err(|_| HeadError::InvalidStatus)?;
    let (headers, header_case) = shared_headers(response.headers, input, preserve_header_case)?;

    Ok(ResponseHead {
        version,
        status,
        headers,
        header_case,
    })
}

pub(in crate::proto::h1) fn own_headers(headers: &[httparse::Header<'_>]) -> Result<HeaderMap, HeadError> {
    let mut owned = HeaderMap::try_with_capacity(headers.len()).map_err(|_| HeadError::Limit(HeadLimit::HeaderMap))?;
    for header in headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| HeadError::InvalidHeader)?;
        let value = HeaderValue::from_bytes(header.value).map_err(|_| HeadError::InvalidHeader)?;
        owned.append(name, value);
    }
    Ok(owned)
}

fn shared_headers(
    headers: &[httparse::Header<'_>],
    input: &Bytes,
    preserve_header_case: bool,
) -> Result<(HeaderMap, Option<HeaderCaseMap>), HeadError> {
    let mut owned = HeaderMap::try_with_capacity(headers.len()).map_err(|_| HeadError::Limit(HeadLimit::HeaderMap))?;
    let mut original = preserve_header_case.then(HeaderCaseMap::default);
    for header in headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| HeadError::InvalidHeader)?;
        let value =
            HeaderValue::from_maybe_shared(input.slice_ref(header.value)).map_err(|_| HeadError::InvalidHeader)?;
        if let Some(original) = &mut original {
            original.append(name.clone(), input.slice_ref(header.name.as_bytes()));
        }
        owned.append(name, value);
    }
    Ok((owned, original))
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
