// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests of the public capability error variants and their messages.

use std::error::Error;

use qubit_event_bus::error::CapabilityError;

#[test]
fn test_capability_error_codec_required_display_and_error_chain() {
    let error = CapabilityError::CodecRequired;

    assert_eq!(error.to_string(), "a codec is required for this topic");
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_capability_error_unsupported_display_field_and_error_chain() {
    let error = CapabilityError::Unsupported {
        capability: "ordering",
    };

    assert_eq!(
        error.to_string(),
        "unsupported event bus capability: ordering"
    );
    assert!(matches!(
        error,
        CapabilityError::Unsupported {
            capability: "ordering"
        }
    ));
    assert!(Error::source(&error).is_none());
}
