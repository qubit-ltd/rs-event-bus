// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

use qubit_event_bus::SpiError;

#[derive(Debug)]
struct BackendFailure;

impl fmt::Display for BackendFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("backend rejected token")
    }
}

impl Error for BackendFailure {}

#[test]
fn invalid_settlement_token_preserves_spi_context_and_source() {
    let error = SpiError::InvalidSettlementToken {
        provider_id: "local".into(),
        operation: "settle",
        resource: Some("subscription-7".into()),
        reason: "already_settled",
        retryable: Some(false),
        source: Box::new(BackendFailure),
    };

    assert_eq!(error.provider_id(), "local");
    assert_eq!(error.operation(), "settle");
    assert_eq!(error.resource(), Some("subscription-7"));
    assert_eq!(error.kind(), "invalid_settlement_token");
    assert_eq!(error.retryable(), Some(false));
    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("backend rejected token")
    );
}

#[test]
fn operation_failure_exposes_spi_context_and_source() {
    let error = SpiError::Operation {
        provider_id: "remote".into(),
        operation: "publish",
        resource: Some("orders".into()),
        kind: "transport_unavailable",
        retryable: Some(true),
        source: Box::new(BackendFailure),
    };

    assert_eq!(error.provider_id(), "remote");
    assert_eq!(error.operation(), "publish");
    assert_eq!(error.resource(), Some("orders"));
    assert_eq!(error.kind(), "transport_unavailable");
    assert_eq!(error.retryable(), Some(true));
    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("backend rejected token")
    );
}
