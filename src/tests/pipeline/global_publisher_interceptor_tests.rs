// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal global publisher interceptor contract tests.

use std::panic::panic_any;

use crate::error::PublishError;
use crate::model::PublishMetadata;
use crate::pipeline::GlobalPublisherInterceptor;

#[test]
fn test_interceptor_converts_string_panic_to_structured_error() {
    let interceptor = GlobalPublisherInterceptor::new(|_: &mut PublishMetadata| -> Result<bool, PublishError> {
        panic!("publisher middleware failed")
    });
    let error = interceptor
        .apply(&mut PublishMetadata::default())
        .expect_err("panic should become a structured error");
    assert!(matches!(
        error,
        PublishError::InterceptorPanicked {
            scope: "global",
            message
        } if message.as_ref() == "publisher middleware failed"
    ));
}

#[test]
fn test_interceptor_handles_non_string_panic_payload() {
    let interceptor =
        GlobalPublisherInterceptor::new(|_: &mut PublishMetadata| -> Result<bool, PublishError> { panic_any(17_u8) });
    let error = interceptor
        .apply(&mut PublishMetadata::default())
        .expect_err("panic should become a structured error");
    assert!(matches!(
        error,
        PublishError::InterceptorPanicked { message, .. }
            if message.as_ref() == "non-string panic payload"
    ));
}
