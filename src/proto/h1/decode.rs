use super::syntax::{is_tchar as tchar, quoted_string_len, trim_ows as trim};
use super::{
    BodyMode,
    head::{HeadError, HeaderWorkspace, own_headers},
};
use http::{
    HeaderMap,
    header::{
        AUTHORIZATION, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, EXPECT, HOST,
        PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, TE, TRAILER, TRANSFER_ENCODING, UPGRADE, WWW_AUTHENTICATE,
    },
};
use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecodeLimits {
    max_chunk_line_bytes: usize,
    max_trailer_bytes: usize,
    max_trailers: usize,
}

impl DecodeLimits {
    pub(super) const fn new(chunk_line: usize, trailer_bytes: usize, trailers: usize) -> Self {
        assert!(
            chunk_line > 0 && trailer_bytes > 0 && trailers > 0,
            "decoder limits must be nonzero"
        );
        Self {
            max_chunk_line_bytes: chunk_line,
            max_trailer_bytes: trailer_bytes,
            max_trailers: trailers,
        }
    }
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self::new(8 * 1024, 16 * 1024, 32)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DecodeLimit {
    ChunkLineBytes,
    TrailerBytes,
    Trailers,
    TrailerMap,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DecodeError {
    UnexpectedEof,
    InvalidChunkSize,
    InvalidChunkExtension,
    InvalidChunkTerminator,
    InvalidTrailer,
    ForbiddenTrailer,
    Limit(DecodeLimit),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for DecodeError {}

impl From<DecodeError> for crate::Error {
    fn from(error: DecodeError) -> Self {
        let kind = match error {
            DecodeError::Limit(_) => crate::ErrorKind::Limit,
            _ => crate::ErrorKind::InvalidMessage,
        };
        Self::with_source(kind, "received body framing failed", error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DecodeOutcome {
    NeedMore { consumed: usize },
    Data { payload: Range<usize>, consumed: usize },
    Trailers { trailers: HeaderMap, consumed: usize },
    End { consumed: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChunkState {
    Size { scanned: usize },
    Data { remaining: u64 },
    DataCrlf,
    Trailers { scanned: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DecoderState {
    Empty,
    Fixed { remaining: u64 },
    Chunked(ChunkState),
    UntilEof,
    Done,
    Failed(DecodeError),
}

#[derive(Debug)]
pub(super) struct BodyDecoder {
    state: DecoderState,
    limits: DecodeLimits,
    trailer_workspace: Option<HeaderWorkspace>,
}

impl BodyDecoder {
    #[cfg(test)]
    pub(super) fn new(mode: BodyMode) -> Self {
        Self::with_limits(mode, DecodeLimits::default())
    }

    pub(super) fn with_limits(mode: BodyMode, limits: DecodeLimits) -> Self {
        let state = match mode {
            BodyMode::None => DecoderState::Empty,
            BodyMode::Fixed(remaining) => DecoderState::Fixed { remaining },
            BodyMode::Chunked => DecoderState::Chunked(ChunkState::Size { scanned: 0 }),
            BodyMode::UntilEof => DecoderState::UntilEof,
        };
        Self {
            state,
            limits,
            trailer_workspace: None,
        }
    }

    /// Whether framing is already complete without consuming more input.
    /// This permits closing a delivered fixed body or trailers without asking
    /// the application for another frame solely to acknowledge EOF.
    pub(super) fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DecoderState::Empty | DecoderState::Fixed { remaining: 0 } | DecoderState::Done
        )
    }

    /// Consume only the reported prefix before calling again. Payload ranges
    /// refer to this input and must be transferred or copied before its storage
    /// is reused. EOF applies after all supplied bytes, so a final data range can
    /// precede `End` or an incomplete-framing error on the next call.
    pub(super) fn decode(&mut self, input: &[u8], eof: bool) -> Result<DecodeOutcome, DecodeError> {
        match self.state {
            DecoderState::Empty | DecoderState::Fixed { remaining: 0 } => {
                self.state = DecoderState::Done;
                Ok(DecodeOutcome::End { consumed: 0 })
            }
            DecoderState::Fixed { remaining } if !input.is_empty() => {
                let count64 = u64::try_from(input.len()).unwrap_or(u64::MAX).min(remaining);
                let count = usize::try_from(count64).expect("count is bounded by input length");
                self.state = DecoderState::Fixed {
                    remaining: remaining - count64,
                };
                Ok(DecodeOutcome::Data {
                    payload: 0..count,
                    consumed: count,
                })
            }
            DecoderState::Fixed { .. } if eof => self.fail(DecodeError::UnexpectedEof),
            DecoderState::Fixed { .. } => Ok(DecodeOutcome::NeedMore { consumed: 0 }),
            DecoderState::Chunked(state) => self.decode_chunked(state, input, eof),
            DecoderState::UntilEof if !input.is_empty() => {
                if eof {
                    self.state = DecoderState::Done;
                }
                Ok(DecodeOutcome::Data {
                    payload: 0..input.len(),
                    consumed: input.len(),
                })
            }
            DecoderState::UntilEof if eof => {
                self.state = DecoderState::Done;
                Ok(DecodeOutcome::End { consumed: 0 })
            }
            DecoderState::UntilEof => Ok(DecodeOutcome::NeedMore { consumed: 0 }),
            DecoderState::Done => Ok(DecodeOutcome::End { consumed: 0 }),
            DecoderState::Failed(error) => Err(error),
        }
    }

    fn decode_chunked(&mut self, mut state: ChunkState, input: &[u8], eof: bool) -> Result<DecodeOutcome, DecodeError> {
        let mut cursor = 0;
        loop {
            let before = (state, cursor);
            match state {
                ChunkState::Size { scanned } => {
                    let (parsed, scanned) = match self.parse_chunk_line(&input[cursor..], scanned) {
                        Ok(parsed) => parsed,
                        Err(error) => return self.fail(error),
                    };
                    let Some((size, consumed)) = parsed else {
                        if eof {
                            return self.fail(DecodeError::UnexpectedEof);
                        }
                        self.state = DecoderState::Chunked(ChunkState::Size { scanned });
                        return Ok(DecodeOutcome::NeedMore { consumed: cursor });
                    };
                    cursor += consumed;
                    state = if size == 0 {
                        ChunkState::Trailers { scanned: 0 }
                    } else {
                        ChunkState::Data { remaining: size }
                    };
                }
                ChunkState::Data { remaining } => {
                    if cursor == input.len() {
                        if eof {
                            return self.fail(DecodeError::UnexpectedEof);
                        }
                        self.state = DecoderState::Chunked(state);
                        return Ok(DecodeOutcome::NeedMore { consumed: cursor });
                    }
                    let count64 = u64::try_from(input.len() - cursor).unwrap_or(u64::MAX).min(remaining);
                    let count = usize::try_from(count64).expect("count is bounded by input length");
                    let payload = cursor..cursor + count;
                    cursor += count;
                    let remaining = remaining - count64;
                    self.state = DecoderState::Chunked(if remaining == 0 {
                        ChunkState::DataCrlf
                    } else {
                        ChunkState::Data { remaining }
                    });
                    return Ok(DecodeOutcome::Data {
                        payload,
                        consumed: cursor,
                    });
                }
                ChunkState::DataCrlf => {
                    let remaining = &input[cursor..];
                    if remaining.len() < 2 {
                        if remaining.first().is_some_and(|byte| *byte != b'\r') {
                            return self.fail(DecodeError::InvalidChunkTerminator);
                        }
                        if eof {
                            return self.fail(DecodeError::UnexpectedEof);
                        }
                        self.state = DecoderState::Chunked(state);
                        return Ok(DecodeOutcome::NeedMore { consumed: cursor });
                    }
                    if &remaining[..2] != b"\r\n" {
                        return self.fail(DecodeError::InvalidChunkTerminator);
                    }
                    cursor += 2;
                    state = ChunkState::Size { scanned: 0 };
                }
                ChunkState::Trailers { scanned } => return self.decode_trailers(input, cursor, eof, scanned),
            }
            debug_assert!(state != before.0 || cursor > before.1, "chunk decoder made no progress");
        }
    }

    fn parse_chunk_line(
        &mut self,
        input: &[u8],
        mut scanned: usize,
    ) -> Result<(Option<(u64, usize)>, usize), DecodeError> {
        if input.len() < scanned {
            scanned = 0;
        }
        for index in scanned..input.len() {
            let byte = input[index];
            if byte == b'\n' {
                if index == 0 || input[index - 1] != b'\r' {
                    return Err(DecodeError::InvalidChunkSize);
                }
                let consumed = index + 1;
                if consumed > self.limits.max_chunk_line_bytes {
                    return Err(DecodeError::Limit(DecodeLimit::ChunkLineBytes));
                }
                return parse_chunk_line(&input[..index - 1]).map(|size| (Some((size, consumed)), 0));
            }
            if index > 0 && input[index - 1] == b'\r' {
                return Err(DecodeError::InvalidChunkSize);
            }
        }
        if input.len() > self.limits.max_chunk_line_bytes {
            Err(DecodeError::Limit(DecodeLimit::ChunkLineBytes))
        } else {
            Ok((None, input.len()))
        }
    }

    fn decode_trailers(
        &mut self,
        input: &[u8],
        cursor: usize,
        eof: bool,
        scanned: usize,
    ) -> Result<DecodeOutcome, DecodeError> {
        let available = &input[cursor..];
        let (trailer_bytes, scanned) = match self.trailer_end(available, scanned) {
            Ok(result) => result,
            Err(error) => return self.fail(error),
        };
        let Some(trailer_bytes) = trailer_bytes else {
            if eof {
                return self.fail(DecodeError::UnexpectedEof);
            }
            self.state = DecoderState::Chunked(ChunkState::Trailers { scanned });
            return Ok(DecodeOutcome::NeedMore { consumed: cursor });
        };
        let parsed = {
            let workspace = self
                .trailer_workspace
                .get_or_insert_with(|| HeaderWorkspace::new(self.limits.max_trailers));
            match httparse::parse_headers(&available[..trailer_bytes], workspace.for_headers()) {
                Ok(httparse::Status::Complete((consumed, headers))) => match own_headers(headers) {
                    Ok(trailers) => validate_trailers(&trailers).map(|()| Some((consumed, trailers))),
                    Err(HeadError::Limit(_)) => Err(DecodeError::Limit(DecodeLimit::TrailerMap)),
                    Err(_) => Err(DecodeError::InvalidTrailer),
                },
                Ok(httparse::Status::Partial) => Err(DecodeError::InvalidTrailer),
                Err(httparse::Error::TooManyHeaders) => Err(DecodeError::Limit(DecodeLimit::Trailers)),
                Err(_) => Err(DecodeError::InvalidTrailer),
            }
        };
        self.trailer_workspace
            .as_mut()
            .expect("chunked decoder has trailer workspace")
            .clear();
        match parsed {
            Ok(Some((trailer_bytes, trailers))) => {
                self.state = DecoderState::Done;
                Ok(DecodeOutcome::Trailers {
                    trailers,
                    consumed: cursor + trailer_bytes,
                })
            }
            Ok(None) => unreachable!("a complete trailer block must parse completely"),
            Err(error) => self.fail(error),
        }
    }

    fn trailer_end(&self, input: &[u8], mut scanned: usize) -> Result<(Option<usize>, usize), DecodeError> {
        if input.len() < scanned {
            scanned = 0;
        }
        let bounded = input.len().min(self.limits.max_trailer_bytes);
        for index in scanned.min(bounded)..bounded {
            if input[index] != b'\n' {
                continue;
            }
            let empty_line = index == 0
                || (index == 1 && input[0] == b'\r')
                || input[index - 1] == b'\n'
                || (index >= 2 && input[index - 2] == b'\n' && input[index - 1] == b'\r');
            if empty_line {
                return Ok((Some(index + 1), 0));
            }
        }
        if input.len() > self.limits.max_trailer_bytes {
            Err(DecodeError::Limit(DecodeLimit::TrailerBytes))
        } else {
            Ok((None, bounded))
        }
    }

    fn fail<T>(&mut self, error: DecodeError) -> Result<T, DecodeError> {
        self.state = DecoderState::Failed(error);
        Err(error)
    }
}

fn parse_chunk_line(line: &[u8]) -> Result<u64, DecodeError> {
    let extension = line.iter().position(|byte| *byte == b';').unwrap_or(line.len());
    let size_text = trim_end(&line[..extension]);
    if size_text.is_empty() || !size_text.iter().all(u8::is_ascii_hexdigit) {
        return Err(DecodeError::InvalidChunkSize);
    }
    let size = size_text.iter().try_fold(0_u64, |size, digit| {
        size.checked_mul(16)
            .and_then(|size| size.checked_add(u64::from(hex(*digit))))
            .ok_or(DecodeError::InvalidChunkSize)
    })?;
    validate_extensions(&line[extension..])?;
    Ok(size)
}

fn validate_extensions(mut input: &[u8]) -> Result<(), DecodeError> {
    while !input.is_empty() {
        input = trim(input);
        if input.first() != Some(&b';') {
            return Err(DecodeError::InvalidChunkExtension);
        }
        input = trim(&input[1..]);
        let name = input.iter().take_while(|byte| tchar(**byte)).count();
        if name == 0 {
            return Err(DecodeError::InvalidChunkExtension);
        }
        input = trim(&input[name..]);
        if input.first() == Some(&b'=') {
            input = trim(&input[1..]);
            let value = if input.first() == Some(&b'"') {
                quoted_string_len(input).ok_or(DecodeError::InvalidChunkExtension)?
            } else {
                let value = input.iter().take_while(|byte| tchar(**byte)).count();
                if value == 0 {
                    return Err(DecodeError::InvalidChunkExtension);
                }
                value
            };
            input = trim(&input[value..]);
        }
        if !input.is_empty() && input.first() != Some(&b';') {
            return Err(DecodeError::InvalidChunkExtension);
        }
    }
    Ok(())
}

pub(super) fn validate_trailers(trailers: &HeaderMap) -> Result<(), DecodeError> {
    for name in trailers.keys() {
        if [
            TRANSFER_ENCODING,
            CONTENT_LENGTH,
            HOST,
            CONNECTION,
            TRAILER,
            TE,
            UPGRADE,
            EXPECT,
            AUTHORIZATION,
            PROXY_AUTHORIZATION,
            PROXY_AUTHENTICATE,
            WWW_AUTHENTICATE,
            CONTENT_ENCODING,
            CONTENT_TYPE,
            CONTENT_RANGE,
        ]
        .contains(name)
        {
            return Err(DecodeError::ForbiddenTrailer);
        }
    }
    Ok(())
}

fn trim_end(mut value: &[u8]) -> &[u8] {
    while value.last().is_some_and(|byte| matches!(byte, b' ' | b'\t')) {
        value = &value[..value.len() - 1];
    }
    value
}

fn hex(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::{BodyDecoder, DecodeError, DecodeLimit, DecodeLimits, DecodeOutcome};
    use crate::proto::h1::BodyMode;
    use http::HeaderMap;

    #[test]
    fn non_chunked_modes_preserve_boundaries_and_eof() {
        let mut empty = BodyDecoder::new(BodyMode::None);
        assert_eq!(
            empty.decode(b"next", false).unwrap(),
            DecodeOutcome::End { consumed: 0 }
        );

        let mut fixed = BodyDecoder::new(BodyMode::Fixed(4));
        assert_eq!(
            fixed.decode(b"bodyNEXT", false).unwrap(),
            DecodeOutcome::Data {
                payload: 0..4,
                consumed: 4
            }
        );
        assert_eq!(
            fixed.decode(b"NEXT", false).unwrap(),
            DecodeOutcome::End { consumed: 0 }
        );

        let mut incomplete = BodyDecoder::new(BodyMode::Fixed(1));
        assert_eq!(
            incomplete.decode(&[], false).unwrap(),
            DecodeOutcome::NeedMore { consumed: 0 }
        );
        assert_eq!(incomplete.decode(&[], true).unwrap_err(), DecodeError::UnexpectedEof);
        assert_eq!(incomplete.decode(b"x", false).unwrap_err(), DecodeError::UnexpectedEof);

        let mut eof = BodyDecoder::new(BodyMode::UntilEof);
        assert_eq!(
            eof.decode(b"last", true).unwrap(),
            DecodeOutcome::Data {
                payload: 0..4,
                consumed: 4
            }
        );
        assert_eq!(eof.decode(&[], false).unwrap(), DecodeOutcome::End { consumed: 0 });
    }

    #[test]
    fn fixed_body_decodes_at_every_fragment_size() {
        let body = b"fragmented fixed body";
        for width in 1..=body.len() {
            let mut decoder = BodyDecoder::new(BodyMode::Fixed(body.len() as u64));
            let mut data = Vec::new();
            for input in body.chunks(width) {
                let DecodeOutcome::Data { payload, .. } = decoder.decode(input, false).unwrap() else {
                    panic!("expected data")
                };
                data.extend_from_slice(&input[payload]);
            }
            assert_eq!(decoder.decode(&[], false).unwrap(), DecodeOutcome::End { consumed: 0 });
            assert_eq!(data, body);
        }
    }

    #[test]
    fn chunked_data_trailers_and_excess_survive_every_fragment_size() {
        let wire = b"4;name=token\r\nWiki\r\n5\r\npedia\r\n0;done=\"yes\"\r\nX-Checksum: good\r\n\r\nNEXT";
        for width in 1..=wire.len() {
            let decoded = fragmented(wire, width).unwrap();
            assert_eq!(decoded.data, b"Wikipedia");
            assert_eq!(decoded.trailers["x-checksum"], "good");
            assert_eq!(decoded.remaining, b"NEXT");
        }
    }

    #[test]
    fn chunk_sizes_accept_case_and_empty_trailers() {
        for size in *b"aA" {
            let mut wire = vec![size];
            wire.extend_from_slice(b"\r\n0123456789\r\n0\r\n\r\nafter");
            let decoded = fragmented(&wire, 3).unwrap();
            assert_eq!(decoded.data, b"0123456789");
            assert!(decoded.trailers.is_empty());
            assert_eq!(decoded.remaining, b"after");
        }
    }

    #[test]
    fn fragmented_chunk_metadata_advances_incremental_scans() {
        let mut decoder = BodyDecoder::new(BodyMode::Chunked);
        let mut line = b"1;name=".to_vec();
        line.extend(std::iter::repeat_n(b'a', 256));
        line.push(b'\r');

        let mut pending = Vec::new();
        for byte in line {
            pending.push(byte);
            assert_eq!(
                decoder.decode(&pending, false).unwrap(),
                DecodeOutcome::NeedMore { consumed: 0 }
            );
            assert!(matches!(
                decoder.state,
                super::DecoderState::Chunked(super::ChunkState::Size { scanned })
                    if scanned == pending.len()
            ));
        }
        pending.push(b'\n');
        assert_eq!(
            decoder.decode(&pending, false).unwrap(),
            DecodeOutcome::NeedMore {
                consumed: pending.len()
            }
        );

        let mut decoder = BodyDecoder::new(BodyMode::Chunked);
        assert_eq!(
            decoder.decode(b"0\r\n", false).unwrap(),
            DecodeOutcome::NeedMore { consumed: 3 }
        );
        let mut trailer = b"X-Long: ".to_vec();
        trailer.extend(std::iter::repeat_n(b'a', 256));
        trailer.extend_from_slice(b"\r\n\r");
        pending.clear();
        for byte in trailer {
            pending.push(byte);
            assert_eq!(
                decoder.decode(&pending, false).unwrap(),
                DecodeOutcome::NeedMore { consumed: 0 }
            );
            assert!(matches!(
                decoder.state,
                super::DecoderState::Chunked(super::ChunkState::Trailers { scanned })
                    if scanned == pending.len()
            ));
            assert!(decoder.trailer_workspace.is_none());
        }
        pending.push(b'\n');
        assert!(matches!(
            decoder.decode(&pending, false).unwrap(),
            DecodeOutcome::Trailers { consumed, .. } if consumed == pending.len()
        ));
        assert!(decoder.trailer_workspace.is_some());
    }

    #[test]
    fn malformed_chunks_fail_terminally() {
        for wire in [
            b"10000000000000000\r\n".as_slice(),
            b"x\r\n".as_slice(),
            b"1;=bad\r\n".as_slice(),
            b"1;name=\r\n".as_slice(),
        ] {
            let mut decoder = BodyDecoder::new(BodyMode::Chunked);
            assert!(matches!(
                decoder.decode(wire, false),
                Err(DecodeError::InvalidChunkSize | DecodeError::InvalidChunkExtension)
            ));
        }
        let mut terminator = BodyDecoder::new(BodyMode::Chunked);
        assert!(matches!(
            terminator.decode(b"1\r\naX", false).unwrap(),
            DecodeOutcome::Data { .. }
        ));
        assert_eq!(
            terminator.decode(b"X", false).unwrap_err(),
            DecodeError::InvalidChunkTerminator
        );
        assert_eq!(
            terminator.decode(b"ignored", false).unwrap_err(),
            DecodeError::InvalidChunkTerminator
        );
    }

    #[test]
    fn premature_eof_is_distinct_from_waiting() {
        for wire in [b"1\r\n".as_slice(), b"1\r\na\r".as_slice(), b"0\r\nX".as_slice()] {
            let mut decoder = BodyDecoder::new(BodyMode::Chunked);
            let mut input = wire;
            loop {
                match decoder.decode(input, true) {
                    Ok(DecodeOutcome::Data { consumed, .. }) => input = &input[consumed..],
                    Err(error) => {
                        assert_eq!(error, DecodeError::UnexpectedEof);
                        break;
                    }
                    other => panic!("unexpected EOF outcome: {other:?}"),
                }
            }
        }
    }

    #[test]
    fn decoder_enforces_chunk_and_trailer_limits() {
        let limits = DecodeLimits::new(4, 8, 1);
        let mut line = BodyDecoder::with_limits(BodyMode::Chunked, limits);
        assert_eq!(
            line.decode(b"1;x\r\n", false).unwrap_err(),
            DecodeError::Limit(DecodeLimit::ChunkLineBytes)
        );
        let mut bytes = BodyDecoder::with_limits(BodyMode::Chunked, limits);
        assert_eq!(
            bytes.decode(b"0\r\nLong: value\r\n\r\n", false).unwrap_err(),
            DecodeError::Limit(DecodeLimit::TrailerBytes)
        );
        let count_limits = DecodeLimits::new(4, 64, 1);
        let mut count = BodyDecoder::with_limits(BodyMode::Chunked, count_limits);
        assert_eq!(
            count.decode(b"0\r\nA: 1\r\nB: 2\r\n\r\n", false).unwrap_err(),
            DecodeError::Limit(DecodeLimit::Trailers)
        );

        let exact_limits = DecodeLimits::new(3, 8, 1);
        let mut exact = BodyDecoder::with_limits(BodyMode::Chunked, exact_limits);
        assert!(matches!(
            exact.decode(b"1\r\nx\r\n0\r\nA: 1\r\n\r\n", false).unwrap(),
            DecodeOutcome::Data { .. }
        ));
        assert!(matches!(
            exact.decode(b"\r\n0\r\nA: 1\r\n\r\n", false).unwrap(),
            DecodeOutcome::Trailers { .. }
        ));
    }

    #[test]
    fn forbidden_and_malformed_trailers_are_rejected() {
        let mut forbidden = BodyDecoder::new(BodyMode::Chunked);
        assert_eq!(
            forbidden.decode(b"0\r\nContent-Length: 1\r\n\r\n", false).unwrap_err(),
            DecodeError::ForbiddenTrailer
        );
        let mut malformed = BodyDecoder::new(BodyMode::Chunked);
        assert_eq!(
            malformed.decode(b"0\r\nBad trailer\r\n\r\n", false).unwrap_err(),
            DecodeError::InvalidTrailer
        );
    }

    struct Decoded {
        data: Vec<u8>,
        trailers: HeaderMap,
        remaining: Vec<u8>,
    }

    fn fragmented(wire: &[u8], width: usize) -> Result<Decoded, DecodeError> {
        let mut decoder = BodyDecoder::new(BodyMode::Chunked);
        let (mut pending, mut source, mut data) = (Vec::new(), 0, Vec::new());
        let trailers = loop {
            if source < wire.len() {
                let end = (source + width).min(wire.len());
                pending.extend_from_slice(&wire[source..end]);
                source = end;
            }
            match decoder.decode(&pending, false)? {
                DecodeOutcome::NeedMore { consumed } => {
                    pending.drain(..consumed);
                    assert!(source < wire.len());
                }
                DecodeOutcome::Data { payload, consumed } => {
                    data.extend_from_slice(&pending[payload]);
                    pending.drain(..consumed);
                }
                DecodeOutcome::Trailers { trailers, consumed } => {
                    pending.drain(..consumed);
                    break trailers;
                }
                DecodeOutcome::End { .. } => panic!("body ended before trailer outcome"),
            }
        };
        assert_eq!(decoder.decode(&pending, false)?, DecodeOutcome::End { consumed: 0 });
        pending.extend_from_slice(&wire[source..]);
        Ok(Decoded {
            data,
            trailers,
            remaining: pending,
        })
    }
}
