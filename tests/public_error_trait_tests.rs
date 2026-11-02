// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests public error source-chain preservation, retry conversion, and cause
//! behavior.

use std::error::Error;
use std::io::Error as IoError;

use qubit_event_bus::error::PublishAttemptError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::error::PublishFailure;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::pipeline::DeadLetterForwardError;
use qubit_retry::Retry;
use qubit_retry::RetryConfig;
use qubit_retry::RetryErrorReason;
use qubit_retry::RetryFallback;

#[test]
fn test_publish_error_handler_panic_preserves_source_chain() {
    let error = PublishError::ErrorHandlerPanicked {
        message: "observer failed".into(),
        source: Box::new(IoError::other("transport failed")),
    };

    assert!(error.to_string().contains("observer failed"));
    let source = error.source().expect("original error should be retained");
    assert_eq!(source.to_string(), "transport failed");
}

#[test]
fn test_retry_error_converts_to_publish_error_without_losing_terminal_reason() {
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
                PublishEffect::NotAccepted,
                IoError::other("provider unavailable"),
            ))
        })
        .expect_err("single failed attempt should be terminal");

    let publish_error = PublishError::from(retry_error);
    let PublishError::Retry(retry_error) = publish_error else {
        panic!("retry outcome should remain a retry publish error");
    };
    assert!(matches!(retry_error.reason(), RetryErrorReason::Exhausted { .. }));
    let last_error = retry_error
        .last_error()
        .expect("exhausted retry should retain the terminal error");
    assert_eq!(last_error.kind(), "injected");
}

#[test]
fn test_dead_letter_forward_error_is_send_sync_and_exposes_admission_evidence() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<DeadLetterForwardError>();

    let error = DeadLetterForwardError::NotAdmitted(AdmissionOutcome::NoDestinations);
    assert!(error.source().is_none());
    assert!(error.to_string().contains("NoDestinations"));
}

#[test]
fn test_dead_letter_publish_variant_exposes_publish_failure_as_direct_source() {
    let publish_failure = PublishFailure::new(
        EventId::new("dead-letter-event").unwrap(),
        PublishEffect::NotAccepted,
        PublishError::Spi(SpiError::Publish {
            provider_id: "scripted".into(),
            resource: None,
            kind: "injected",
            retryable: Some(false),
            effect: PublishEffect::NotAccepted,
            source: Box::new(IoError::other("SPI source")),
        }),
    );
    let error = DeadLetterForwardError::Publish(publish_failure);
    let direct_source = error.source().expect("PublishFailure is directly exposed");
    let failure = direct_source
        .downcast_ref::<PublishFailure>()
        .expect("direct source retains aggregate publish evidence");
    assert_eq!(failure.effect(), PublishEffect::NotAccepted);

    let mut current = Some(direct_source);
    let mut retained_spi_source = false;
    while let Some(source) = current {
        retained_spi_source |= source.to_string() == "SPI source";
        current = source.source();
    }
    assert!(retained_spi_source, "the original SPI source remains in the chain");
}

#[test]
fn test_dead_letter_retry_error_retains_terminal_context_and_application_error() {
    let config = RetryConfig::<DeadLetterForwardError>::builder()
        .max_attempts(1)
        .fallback(RetryFallback::Retry)
        .build()
        .unwrap();
    let retry_error = Retry::new(&config)
        .run(|| Err::<(), _>(DeadLetterForwardError::NotAdmitted(AdmissionOutcome::NoDestinations)))
        .expect_err("one failed attempt exhausts the retry policy");

    assert!(matches!(retry_error.reason(), RetryErrorReason::Exhausted { .. }));
    assert_eq!(retry_error.context().attempts(), 1);
    assert!(matches!(
        retry_error.last_error(),
        Some(DeadLetterForwardError::NotAdmitted(AdmissionOutcome::NoDestinations))
    ));
}
