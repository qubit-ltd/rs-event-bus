// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for package metadata.

#[test]
fn test_package_version_marks_breaking_api_release() {
    assert_eq!(env!("CARGO_PKG_VERSION"), "0.7.0");
}
