//! JSONC normalization preserves string bytes and line breaks. Serde remains the
//! JSON grammar authority; only comments and one trailing comma are extensions.
use super::ConfigError;

pub(super) fn parse(text: &str) -> Result<serde_json::Value, ConfigError> {
    let mut bytes = text.as_bytes().to_vec();
    let mut index = 0;
    let mut in_string = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' if in_string => index += 2,
            b'"' => {
                in_string = !in_string;
                index += 1;
            }
            b'/' if !in_string && bytes.get(index + 1) == Some(&b'/') => {
                while index < bytes.len() && !matches!(bytes[index], b'\r' | b'\n') {
                    bytes[index] = b' ';
                    index += 1;
                }
            }
            b'/' if !in_string && bytes.get(index + 1) == Some(&b'*') => {
                bytes[index] = b' ';
                bytes[index + 1] = b' ';
                index += 2;
                loop {
                    if index >= bytes.len() {
                        return Err(ConfigError::with_cause(
                            "configuration",
                            "invalid_format",
                            "unterminated JSONC block comment",
                        ));
                    }
                    if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                        bytes[index] = b' ';
                        bytes[index + 1] = b' ';
                        index += 2;
                        break;
                    }
                    if !matches!(bytes[index], b'\r' | b'\n') {
                        bytes[index] = b' ';
                    }
                    index += 1;
                }
            }
            _ => index += 1,
        }
    }
    // A comma is trailing only after a value token, never after an opener or
    // another comma. This avoids accepting malformed arrays such as [,] or [1,,].
    index = 0;
    in_string = false;
    let mut previous = None;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' if in_string => {
                index += 2;
                continue;
            }
            b'"' => in_string = !in_string,
            b',' if !in_string => {
                let next = bytes[index + 1..]
                    .iter()
                    .copied()
                    .find(|byte| !byte.is_ascii_whitespace());
                if matches!(next, Some(b']' | b'}'))
                    && previous.is_some_and(|byte| !matches!(byte, b'[' | b'{' | b',' | b':'))
                {
                    bytes[index] = b' ';
                }
                previous = Some(b',');
                index += 1;
                continue;
            }
            _ => {}
        }
        if !in_string && !bytes[index].is_ascii_whitespace() {
            previous = Some(bytes[index]);
        }
        index += 1;
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        ConfigError::with_cause(
            "configuration",
            "invalid_format",
            &microcrab_protocol::diagnostic::json_reason(&error),
        )
    })
}
