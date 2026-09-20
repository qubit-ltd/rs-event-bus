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
    assert_eq!(env!("CARGO_PKG_VERSION"), "0.11.0");
}

#[test]
fn test_release_documentation_set_exists() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "README.md",
        "README.zh_CN.md",
        "doc/user_guide.md",
        "doc/user_guide.zh_CN.md",
        "doc/design.md",
        "doc/design.zh_CN.md",
        "CHANGELOG.md",
        "CHANGELOG.zh_CN.md",
    ] {
        assert!(root.join(relative).is_file(), "missing release document: {relative}");
    }
}
