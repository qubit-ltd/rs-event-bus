// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "conformance")]

//! Public formatting behavior for conformance skip reasons.

use qubit_event_bus::spi::conformance::ConformanceSkipReason;

#[test]
fn test_skip_reasons_format_each_explanation_without_losing_context() {
    assert_eq!(
        ConformanceSkipReason::UnsupportedCapability { capability: "replay" }.to_string(),
        "unsupported capability: replay"
    );
    assert_eq!(
        ConformanceSkipReason::MissingFixture {
            detail: "receive hook".into()
        }
        .to_string(),
        "missing fixture: receive hook"
    );
    assert_eq!(
        ConformanceSkipReason::NotApplicable { reason: "sync API" }.to_string(),
        "not applicable: sync API"
    );
}
