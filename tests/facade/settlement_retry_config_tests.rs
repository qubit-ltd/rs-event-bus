// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public settlement retry policy defaults, validation, and boundary contracts.

use std::num::NonZeroU32;
use std::time::Duration;

use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::facade::SettlementRetryConfig;

#[test]
fn test_settlement_retry_defaults_and_invalid_fields() {
    let config = SettlementRetryConfig::default();
    assert_eq!(config.max_attempts().get(), 5);
    assert_eq!(config.max_elapsed(), Duration::from_secs(5));
    assert_eq!(config.initial_backoff(), Duration::from_millis(10));
    assert_eq!(config.max_backoff(), Duration::from_secs(1));

    let attempts = NonZeroU32::new(1).expect("positive attempt limit");
    let invalid = [
        (
            Duration::ZERO,
            Duration::from_millis(10),
            Duration::from_secs(1),
            "max_elapsed",
        ),
        (
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::from_secs(1),
            "initial_backoff",
        ),
        (
            Duration::from_secs(5),
            Duration::from_secs(2),
            Duration::from_secs(1),
            "max_backoff",
        ),
    ];

    for (max_elapsed, initial_backoff, max_backoff, field) in invalid {
        assert!(matches!(
            SettlementRetryConfig::new(attempts, max_elapsed, initial_backoff, max_backoff),
            Err(ConfigurationError::InvalidField {
                field: actual,
                ..
            }) if actual == field
        ));
    }
}

#[test]
fn test_settlement_retry_accepts_single_attempt_and_equal_backoff_bounds() {
    let attempts = NonZeroU32::new(1).expect("positive attempt limit");
    let backoff = Duration::from_millis(10);
    let config = SettlementRetryConfig::new(attempts, Duration::from_millis(20), backoff, backoff)
        .expect("equal backoff bounds are valid");

    assert_eq!(config.max_attempts(), attempts);
    assert_eq!(config.initial_backoff(), backoff);
    assert_eq!(config.max_backoff(), backoff);
}
