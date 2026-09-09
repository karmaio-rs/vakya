pub(super) fn trim_ows(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(|byte| matches!(byte, b' ' | b'\t')) {
        value = &value[1..];
    }
    while value.last().is_some_and(|byte| matches!(byte, b' ' | b'\t')) {
        value = &value[..value.len() - 1];
    }
    value
}

pub(super) fn is_tchar(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
        )
}

pub(super) fn quoted_string_len(input: &[u8]) -> Option<usize> {
    let mut escaped = false;
    for (index, byte) in input.iter().copied().enumerate().skip(1) {
        if escaped {
            if byte == b'\r' || byte == b'\n' {
                return None;
            }
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            return Some(index + 1);
        } else if byte == b'\r' || byte == b'\n' || byte == 0x7f || (byte < 0x20 && byte != b'\t') {
            return None;
        }
    }
    None
}
