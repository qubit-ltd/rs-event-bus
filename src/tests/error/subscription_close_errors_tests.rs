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
    assert_eq!(empty.to_string(), "0 subscription close failure(s)");
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
            .contains("1 subscription close failure(s); subscription worker-1:")
    );
    assert_eq!(errors.source().unwrap().to_string(), failure.to_string());
}
