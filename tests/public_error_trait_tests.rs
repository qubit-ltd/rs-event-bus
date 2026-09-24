// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;

use qubit_event_bus::error::PublishAttemptError;
use qubit_event_bus::error::PublishError;
use qubit_retry::Retry;
use qubit_retry::RetryConfig;
use qubit_retry::RetryErrorReason;
use qubit_retry::RetryFallback;

#[test]
fn publish_error_handler_panic_preserves_source_chain() {
    let error = PublishError::ErrorHandlerPanicked {
        message: "observer failed".into(),
        source: Box::new(std::io::Error::other("transport failed")),
    };

    assert!(error.to_string().contains("observer failed"));
    let source = error.source().expect("original error should be retained");
    assert_eq!(source.to_string(), "transport failed");
}

#[test]
fn retry_error_converts_to_publish_error_without_losing_terminal_reason() {
    let config = RetryConfig::<PublishAttemptError>::builder()
        .max_attempts(1)
        .fallback(RetryFallback::Retry)
        .build()
        .unwrap();
    let retry_error = Retry::new(&config)
        .run(|| {
            Err::<(), _>(PublishAttemptError::new(
                "injected",
                Some(false),
                std::io::Error::other("provider unavailable"),
            ))
        })
        .expect_err("single failed attempt should be terminal");

    let publish_error = PublishError::from(retry_error);
    let PublishError::Retry(retry_error) = publish_error else {
        panic!("retry outcome should remain a retry publish error");
    };
    assert!(matches!(retry_error.reason(), RetryErrorReason::Exhausted { .. }));
    assert_eq!(retry_error.last_error().unwrap().kind(), "injected");
}
