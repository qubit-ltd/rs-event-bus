// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Dead-letter forwarding retry contracts requiring crate-private access.

use std::cell::Cell;

use qubit_retry::Retry;
use qubit_retry::RetryPolicy;

use crate::error::PublishError;
use crate::error::PublishFailure;
use crate::error::SpiError;
use crate::model::AdmissionOutcome;
use crate::model::AdmissionSummary;
use crate::model::EventId;
use crate::model::PublishEffect;
use crate::pipeline::DeadLetterForwardError;
use crate::pipeline::dead_letter_retry_config;

#[test]
fn test_unknown_dead_letter_publish_is_not_retried() {
    let config =
        dead_letter_retry_config(&RetryPolicy::builder().max_attempts(3).build().unwrap()).unwrap();
    let calls = Cell::new(0);
    let result: Result<_, _> = Retry::new(&config).run(|| {
        calls.set(calls.get() + 1);
        Err::<(), _>(DeadLetterForwardError::Publish(PublishFailure::new(
            EventId::new("dead-letter").unwrap(),
            PublishEffect::MayHaveBeenAccepted,
            PublishError::Spi(SpiError::Operation {
                provider_id: "script".into(),
                operation: "publish",
                resource: None,
                kind: "lost_response",
                retryable: Some(true),
                source: Box::new(std::io::Error::other("source retained")),
            }),
        )))
    });
    assert!(result.is_err());
    assert_eq!(calls.get(), 1);
}

#[test]
fn test_partially_admitted_dead_letter_is_not_retried() {
    let config =
        dead_letter_retry_config(&RetryPolicy::builder().max_attempts(3).build().unwrap()).unwrap();
    let calls = Cell::new(0);
    let result: Result<_, _> = Retry::new(&config).run(|| {
        calls.set(calls.get() + 1);
        Err::<(), _>(DeadLetterForwardError::NotAdmitted(
            AdmissionOutcome::PartiallyAccepted(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 1,
            }),
        ))
    });
    assert!(result.is_err());
    assert_eq!(calls.get(), 1);
}
