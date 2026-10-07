// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public construction, display, and source contracts for delivery failures.

use std::error::Error;
use std::io::Error as IoError;

use qubit_event_bus::error::CodecError;
use qubit_event_bus::error::DeliveryAttemptError;
use qubit_event_bus::error::DeliveryError;
use qubit_event_bus::error::SpiError;
use qubit_retry::Retry;
use qubit_retry::RetryConfig;
use qubit_retry::RetryFallback;

#[test]
fn test_delivery_error_handler_preserves_original_source() {
    let error = DeliveryError::Handler {
        source: Box::new(IoError::other("handler rejected event")),
    };

    assert_eq!(
        error.to_string(),
        "event handler failed: handler rejected event"
    );
    assert_eq!(
        Error::source(&error).map(ToString::to_string).as_deref(),
        Some("handler rejected event")
    );
}

#[test]
fn test_delivery_error_codec_conversion_preserves_codec_failure() {
    let codec_error = CodecError::NativeTypeMismatch;
    let error = DeliveryError::from(codec_error);

    assert_eq!(
        error.to_string(),
        "native payload type does not match subscribed topic"
    );
    assert!(matches!(
        &error,
        DeliveryError::Codec(CodecError::NativeTypeMismatch)
    ));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_delivery_error_spi_conversion_preserves_provider_context_and_source() {
    let spi_error = SpiError::Operation {
        provider_id: "memory".into(),
        operation: "receive",
        resource: Some("orders".into()),
        kind: "unavailable",
        retryable: Some(true),
        source: Box::new(IoError::other("provider offline")),
    };
    let error = DeliveryError::from(spi_error);

    assert_eq!(
        error.to_string(),
        "provider memory failed receive (unavailable): provider offline"
    );
    assert!(matches!(
        &error,
        DeliveryError::Spi(SpiError::Operation { provider_id, .. }) if provider_id.as_ref() == "memory"
    ));
    assert_eq!(
        Error::source(&error).map(ToString::to_string).as_deref(),
        Some("provider offline")
    );
}

#[test]
fn test_delivery_error_retry_retains_terminal_report_and_attempt_source() {
    let config = RetryConfig::<DeliveryAttemptError>::builder()
        .max_attempts(1)
        .fallback(RetryFallback::Retry)
        .build()
        .expect("one-attempt retry configuration should be valid");
    let retry_error = Retry::new(&config)
        .run(|| {
            Err::<(), _>(DeliveryAttemptError::new(
                "handler",
                Some(false),
                IoError::other("delivery refused"),
            ))
        })
        .expect_err("the injected delivery attempt should fail");
    let retry_message = retry_error.to_string();
    let error = DeliveryError::Retry(Box::new(retry_error));

    assert_eq!(
        error.to_string(),
        format!("delivery retry policy terminated: {retry_message}")
    );
    let retry_source = Error::source(&error).expect("retry report should be retained as source");
    assert_eq!(retry_source.to_string(), retry_message);
    assert_eq!(
        Error::source(retry_source)
            .map(ToString::to_string)
            .as_deref(),
        Some("delivery attempt failed (handler): delivery refused")
    );
}
