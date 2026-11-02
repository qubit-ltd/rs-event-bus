// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for subscription close error snapshots.

use std::error::Error;
use std::io::Error as IoError;
use std::sync::Arc;

use crate::error::SpiError;
use crate::error::SubscriptionCloseErrors;
use crate::error::SubscriptionCloseFailure;
use crate::model::SubscriberId;

#[test]
fn test_close_error_snapshot_exposes_each_failure_and_its_source() {
    let empty = SubscriptionCloseErrors::from_failures(Vec::new());
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert_eq!(
        empty.to_string(),
        "0 subscription close failure(s) (0 retained, 0 omitted)"
    );
    assert_eq!(empty.total_count(), 0);
    assert!(empty.source().is_none());

    let failure = Arc::new(SubscriptionCloseFailure::new(
        SubscriberId::new("worker-1").expect("valid subscriber ID"),
        SpiError::Operation {
            provider_id: "test-provider".into(),
            operation: "close_subscription",
            resource: Some("worker-1".into()),
            kind: "close_failed",
            retryable: Some(false),
            source: Box::new(IoError::other("close failed")),
        },
    ));
    let errors = SubscriptionCloseErrors::from_failures(vec![failure]);
    assert!(!errors.is_empty());
    assert_eq!(errors.len(), 1);
    assert_eq!(errors.total_count(), 1);
    assert_eq!(errors.omitted_count(), 0);
    assert!(!errors.is_truncated());
    let failure = errors.iter().next().expect("one close failure");
    assert_eq!(failure.subscriber_id().as_str(), "worker-1");
    assert_eq!(failure.error().kind(), "close_failed");
    assert!(failure.to_string().contains("subscription worker-1:"));
    assert_eq!(
        failure.source().unwrap().to_string(),
        "provider test-provider failed close_subscription (close_failed): close failed"
    );
    assert!(
        errors
            .to_string()
            .contains("1 subscription close failure(s) (1 retained, 0 omitted); subscription worker-1:")
    );
    assert_eq!(errors.source().unwrap().to_string(), failure.to_string());

    let summary_failure = Arc::new(SubscriptionCloseFailure::new(
        SubscriberId::new("worker-summary").expect("valid subscriber ID"),
        SpiError::Operation {
            provider_id: "test-provider".into(),
            operation: "close_subscription",
            resource: None,
            kind: "close_failed",
            retryable: Some(false),
            source: Box::new(IoError::other("close failed")),
        },
    ));
    let summary = SubscriptionCloseErrors::from_summary(vec![summary_failure], 300);
    assert_eq!(summary.len(), 1);
    assert_eq!(summary.total_count(), 300);
    assert_eq!(summary.omitted_count(), 299);
    assert!(summary.is_truncated());
}
