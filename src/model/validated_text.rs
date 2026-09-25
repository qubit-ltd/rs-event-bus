// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Const-compatible validation helpers for portable text values.

/// Checks nonempty text without surrounding Unicode whitespace or controls.
pub(crate) const fn is_nonblank_without_controls(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let mut index = 0;
    let mut first = 0;
    let mut last = 0;
    while index < bytes.len() {
        let (code_point, width) = decode_utf8_code_point(bytes, index);
        if is_unicode_control(code_point) {
            return false;
        }
        if index == 0 {
            first = code_point;
        }
        last = code_point;
        index += width;
    }
    !is_unicode_whitespace(first) && !is_unicode_whitespace(last)
}

/// Checks topic-name byte bounds and the topic's portable text rules.
pub(crate) const fn is_valid_topic_name(value: &str) -> bool {
    let length = value.len();
    length >= 1 && length <= 255 && is_nonblank_without_controls(value)
}

/// Decodes one code point from a valid UTF-8 string at `index`.
const fn decode_utf8_code_point(bytes: &[u8], index: usize) -> (u32, usize) {
    let first = bytes[index];
    if first < 0x80 {
        (first as u32, 1)
    } else if first < 0xE0 {
        let second = bytes[index + 1];
        ((((first & 0x1F) as u32) << 6) | (second & 0x3F) as u32, 2)
    } else if first < 0xF0 {
        let second = bytes[index + 1];
        let third = bytes[index + 2];
        (
            (((first & 0x0F) as u32) << 12) | (((second & 0x3F) as u32) << 6) | (third & 0x3F) as u32,
            3,
        )
    } else {
        let second = bytes[index + 1];
        let third = bytes[index + 2];
        let fourth = bytes[index + 3];
        (
            (((first & 0x07) as u32) << 18)
                | (((second & 0x3F) as u32) << 12)
                | (((third & 0x3F) as u32) << 6)
                | (fourth & 0x3F) as u32,
            4,
        )
    }
}

/// Matches the Unicode White_Space code points used by `str::trim`.
const fn is_unicode_whitespace(code_point: u32) -> bool {
    matches!(
        code_point,
        0x0009..=0x000D | 0x0020 | 0x0085 | 0x00A0 | 0x1680 | 0x2000..=0x200A | 0x2028 | 0x2029 | 0x202F
            | 0x205F | 0x3000
    )
}

/// Matches Unicode general category Cc control code points.
const fn is_unicode_control(code_point: u32) -> bool {
    matches!(code_point, 0x0000..=0x001F | 0x007F..=0x009F)
}

#[cfg(test)]
mod tests {
    use super::is_nonblank_without_controls;
    use super::is_valid_topic_name;

    #[test]
    fn test_nonblank_text_matches_whitespace_and_control_rules() {
        assert!(!is_nonblank_without_controls(""));
        assert!(!is_nonblank_without_controls(" leading"));
        assert!(!is_nonblank_without_controls("trailing\u{3000}"));
        assert!(!is_nonblank_without_controls("control\u{009F}"));
        assert!(is_nonblank_without_controls("internal space"));
        assert!(is_nonblank_without_controls("事件-v1"));

        let whitespace = [
            '\u{0009}', '\u{000A}', '\u{000B}', '\u{000C}', '\u{000D}', '\u{0020}', '\u{0085}', '\u{00A0}', '\u{1680}',
            '\u{2000}', '\u{2001}', '\u{2002}', '\u{2003}', '\u{2004}', '\u{2005}', '\u{2006}', '\u{2007}', '\u{2008}',
            '\u{2009}', '\u{200A}', '\u{2028}', '\u{2029}', '\u{202F}', '\u{205F}', '\u{3000}',
        ];
        for character in whitespace {
            assert!(character.is_whitespace());
            assert!(!is_nonblank_without_controls(&format!("{character}value")));
            assert!(!is_nonblank_without_controls(&format!("value{character}")));
        }

        for code_point in [0x0000, 0x001F, 0x007F, 0x009F] {
            let character = char::from_u32(code_point).expect("valid control code point");
            assert!(character.is_control());
            assert!(!is_nonblank_without_controls(&format!("value{character}")));
        }
    }

    #[test]
    fn test_topic_name_enforces_byte_limit_and_text_rules() {
        assert!(!is_valid_topic_name(""));
        assert!(!is_valid_topic_name(" events"));
        assert!(is_valid_topic_name("事件.created"));
        assert!(is_valid_topic_name(&"a".repeat(255)));
        assert!(!is_valid_topic_name(&"a".repeat(256)));
    }
}
