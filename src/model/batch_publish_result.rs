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
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::BatchPublishResult;
///
/// let batch = BatchPublishResult::new(Vec::new());
/// assert_eq!(batch.total_count(), 0);
/// assert!(batch.items().is_empty());
/// ```
#[must_use]
pub struct BatchPublishResult {
    /// Individual outcomes in the same order as their input requests.
    items: Vec<Result<PublishReceipt, PublishError>>,
}

impl BatchPublishResult {
    /// Preserves the exact input order of publication results.
    ///
    /// # Parameters
    /// - `items`: ordered result for each submitted request.
    ///
    /// # Returns
    /// A batch retaining the supplied result order.
    pub fn new(items: Vec<Result<PublishReceipt, PublishError>>) -> Self {
        Self { items }
    }
    /// Returns all per-request results in input order.
    ///
    /// # Returns
    /// The ordered result slice.
    #[inline]
    pub fn items(&self) -> &[Result<PublishReceipt, PublishError>] {
        &self.items
    }
    /// Consumes the batch and returns its ordered results.
    ///
    /// # Returns
    /// The owned result vector in input order.
    #[must_use]
    pub fn into_items(self) -> Vec<Result<PublishReceipt, PublishError>> {
        self.items
    }
    /// Returns the number of input requests.
    ///
    /// # Returns
    /// The number of stored publication outcomes.
    #[must_use]
    #[inline]
    pub fn total_count(&self) -> usize {
        self.items.len()
    }
    /// Counts broker-accepted publications and local publications with at least
    /// one accepted destination. Empty, filtered-only, and rejected-only
    /// destination lists contribute zero; this does not count handler
    /// completion.
    ///
    /// # Returns
    /// The number of results with at least one accepted destination.
    #[must_use]
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
    ///
    /// # Returns
    /// The number of receipts dropped before provider dispatch.
    #[must_use]
    #[inline]
    pub fn dropped_count(&self) -> usize {
        self.items.iter().filter(|item| matches!(item, Ok(receipt) if matches!(receipt.acknowledgement(), PublishAcknowledgement::DroppedByInterceptor))).count()
    }
    /// Counts failed publications and local receipts with at least one rejected
    /// destination. A mixed local receipt contributes to both accepted and
    /// failure counts; neither count describes handler completion.
    ///
    /// # Returns
    /// The number of failed requests or receipts with rejected destinations.
    #[must_use]
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
