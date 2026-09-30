// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal MIME content type representation tests.

use crate::model::ContentType;

#[test]
fn static_and_runtime_constructors_preserve_their_storage_modes() {
    let static_value = ContentType::new_static("application/x-test");
    let runtime_value = ContentType::new("application/x-test").expect("valid MIME token");

    assert_eq!(static_value.as_str(), "application/x-test");
    assert_eq!(runtime_value.as_str(), "application/x-test");
    assert_eq!(static_value, runtime_value);
}
