// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Ordered best-effort batch publication results.

use super::AdmissionStatus;
use super::PublishAcknowledgement;
use super::PublishReceipt;
use crate::error::PublishError;

/// Each result corresponds to the request at the same input position.
pub struct BatchPublishResult {
    items: Vec<Result<PublishReceipt, PublishError>>,
}

impl BatchPublishResult {
    /// Preserves the exact input order of publication results.
    pub fn new(items: Vec<Result<PublishReceipt, PublishError>>) -> Self {
        Self { items }
    }
    /// Returns all per-request results in input order.
    pub fn items(&self) -> &[Result<PublishReceipt, PublishError>] {
        &self.items
    }
    /// Consumes the batch and returns its ordered results.
    pub fn into_items(self) -> Vec<Result<PublishReceipt, PublishError>> {
        self.items
    }
    /// Returns the number of input requests.
    pub fn total_count(&self) -> usize {
        self.items.len()
    }
    /// Counts broker-accepted publications and local publications with at least
    /// one accepted destination. Empty, filtered-only, and rejected-only
    /// destination lists contribute zero; this does not count handler
    /// completion.
    pub fn accepted_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| match item {
                Ok(receipt) => match receipt.acknowledgement() {
                    PublishAcknowledgement::Accepted { .. } => true,
                    PublishAcknowledgement::DestinationAdmissions(destinations) => destinations
                        .iter()
                        .any(|destination| matches!(destination.status(), AdmissionStatus::Accepted)),
                    PublishAcknowledgement::DroppedByInterceptor => false,
                },
                Err(_) => false,
            })
            .count()
    }
    /// Counts events intentionally dropped by a publisher interceptor.
    pub fn dropped_count(&self) -> usize {
        self.items.iter().filter(|item| matches!(item, Ok(receipt) if matches!(receipt.acknowledgement(), PublishAcknowledgement::DroppedByInterceptor))).count()
    }
    /// Counts failed publications and local receipts with at least one rejected
    /// destination. A mixed local receipt contributes to both accepted and
    /// failure counts; neither count describes handler completion.
    pub fn failure_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| match item {
                Err(_) => true,
                Ok(receipt) => match receipt.acknowledgement() {
                    PublishAcknowledgement::DestinationAdmissions(destinations) => destinations
                        .iter()
                        .any(|destination| matches!(destination.status(), AdmissionStatus::Rejected(_))),
                    _ => false,
                },
            })
            .count()
    }
}
