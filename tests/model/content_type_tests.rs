// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public MIME content type validation tests.

use qubit_event_bus::model::ContentType;

#[test]
fn test_runtime_content_types_accept_tokens_and_reject_parameters_or_extra_slashes() {
    let custom = ContentType::new("application/vnd.example+json").expect("valid MIME token");
    assert_eq!(
        custom.as_str(),
        "application/vnd.example+json",
        "the custom MIME type should be preserved",
    );
    assert!(
        ContentType::new("application/json; charset=utf-8").is_err(),
        "MIME parameters should be rejected",
    );
    assert!(
        ContentType::new("application/vnd.example/json").is_err(),
        "an extra slash in the subtype should be rejected",
    );
}
