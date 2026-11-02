// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for codec panic payload disposal.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;

use crate::codec::call_codec;
use crate::error::CodecError;

struct PanicPayload;

impl Drop for PanicPayload {
    fn drop(&mut self) {
        panic!("panic payload destructor");
    }
}

#[test]
fn test_codec_boundary_contains_panicking_panic_payload_destructor() {
    let result = catch_unwind(AssertUnwindSafe(|| {
        call_codec::<()>("encode", || panic_any(PanicPayload))
    }));
    let error = result
        .expect("payload destructor panic must remain contained")
        .unwrap_err();
    assert!(matches!(
        error,
        CodecError::Panicked {
            operation: "encode",
            ..
        }
    ));
}
