// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal validation contracts for portable text.

use crate::util::validated_text::is_nonblank_without_controls;
use crate::util::validated_text::is_valid_topic_name;

#[test]
fn test_nonblank_text_matches_whitespace_and_control_rules() {
    assert!(!is_nonblank_without_controls(""));
    assert!(!is_nonblank_without_controls(" leading"));
    assert!(!is_nonblank_without_controls("trailing\u{3000}"));
    assert!(!is_nonblank_without_controls("control\u{009F}"));
    assert!(is_nonblank_without_controls("internal space"));
    assert!(is_nonblank_without_controls("事件-v1"));

    let whitespace = [
        '\u{0009}', '\u{000A}', '\u{000B}', '\u{000C}', '\u{000D}', '\u{0020}', '\u{0085}',
        '\u{00A0}', '\u{1680}', '\u{2000}', '\u{2001}', '\u{2002}', '\u{2003}', '\u{2004}',
        '\u{2005}', '\u{2006}', '\u{2007}', '\u{2008}', '\u{2009}', '\u{200A}', '\u{2028}',
        '\u{2029}', '\u{202F}', '\u{205F}', '\u{3000}',
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
