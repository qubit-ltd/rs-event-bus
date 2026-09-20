// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Results describing which subscriber deliveries were admitted.

use crate::EventBusError;

/// Result of publishing one event to the local dispatch path.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublishReceipt {
    input_event_id: String,
    dispatched_event_id: Option<String>,
    outcome: PublishOutcome,
}

impl PublishReceipt {
    /// Creates a receipt for a published event.
    pub fn new(input_event_id: String, dispatched_event_id: Option<String>, outcome: PublishOutcome) -> Self {
        Self {
            input_event_id,
            dispatched_event_id,
            outcome,
        }
    }

    /// Returns the identifier supplied by the publisher.
    pub fn input_event_id(&self) -> &str {
        &self.input_event_id
    }

    /// Returns the identifier after publisher interceptors ran.
    pub fn dispatched_event_id(&self) -> Option<&str> {
        self.dispatched_event_id.as_deref()
    }

    /// Returns the dispatch outcome.
    pub fn outcome(&self) -> &PublishOutcome {
        &self.outcome
    }

    /// Returns whether any subscriber delivery was rejected.
    pub fn has_rejections(&self) -> bool {
        matches!(&self.outcome, PublishOutcome::Dispatched(items) if items.iter().any(|item| matches!(item.status(), DispatchStatus::Rejected(_))))
    }
}

/// Outcome of publisher interception and subscriber dispatch.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PublishOutcome {
    /// The publisher interceptor intentionally dropped the event.
    Dropped,
    /// The event was examined against the subscriber snapshot.
    Dispatched(Vec<SubscriberDispatchResult>),
}

/// Result for one subscriber in a publish snapshot.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubscriberDispatchResult {
    subscription_id: usize,
    subscriber_id: String,
    status: DispatchStatus,
}

impl SubscriberDispatchResult {
    /// Creates a subscriber dispatch result.
    pub(crate) fn new(subscription_id: usize, subscriber_id: String, status: DispatchStatus) -> Self {
        Self {
            subscription_id,
            subscriber_id,
            status,
        }
    }

    /// Returns the unique subscription identifier.
    pub fn subscription_id(&self) -> usize {
        self.subscription_id
    }

    /// Returns the application subscriber identifier.
    pub fn subscriber_id(&self) -> &str {
        &self.subscriber_id
    }

    /// Returns the admission status.
    pub fn status(&self) -> &DispatchStatus {
        &self.status
    }
}

/// Admission status for one subscriber delivery.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DispatchStatus {
    /// The subscriber task was accepted.
    Accepted,
    /// The subscriber filter skipped the event.
    Filtered,
    /// The subscriber task could not be accepted.
    Rejected(EventBusError),
}

/// One item in a best-effort batch publish.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchPublishItem {
    index: usize,
    event_id: String,
    result: Result<PublishReceipt, EventBusError>,
}

/// Compatibility view of batch items that failed before admission.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchPublishFailure {
    index: usize,
    event_id: String,
    error: EventBusError,
}
impl BatchPublishFailure {
    /// Returns the input index.
    pub fn index(&self) -> usize {
        self.index
    }
    /// Returns the input event identifier.
    pub fn event_id(&self) -> &str {
        &self.event_id
    }
    /// Returns the global publication error.
    pub fn error(&self) -> &EventBusError {
        &self.error
    }
}

impl BatchPublishItem {
    pub(crate) fn new(index: usize, event_id: String, result: Result<PublishReceipt, EventBusError>) -> Self {
        Self {
            index,
            event_id,
            result,
        }
    }
    /// Returns the input index.
    pub fn index(&self) -> usize {
        self.index
    }
    /// Returns the original event identifier.
    pub fn event_id(&self) -> &str {
        &self.event_id
    }
    /// Returns the per-event publish result.
    pub fn result(&self) -> &Result<PublishReceipt, EventBusError> {
        &self.result
    }
}

/// Summary of best-effort batch publication.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchPublishResult {
    items: Vec<BatchPublishItem>,
    failures: Vec<BatchPublishFailure>,
}

impl BatchPublishResult {
    pub(crate) fn new(items: Vec<BatchPublishItem>) -> Self {
        let failures = items
            .iter()
            .filter_map(|item| match item.result() {
                Err(error) => Some(BatchPublishFailure {
                    index: item.index(),
                    event_id: item.event_id().to_string(),
                    error: error.clone(),
                }),
                Ok(_) => None,
            })
            .collect();
        Self { items, failures }
    }
    /// Returns per-event results in input order.
    pub fn items(&self) -> &[BatchPublishItem] {
        &self.items
    }
    /// Returns the input item count.
    pub fn total_count(&self) -> usize {
        self.items.len()
    }
    /// Returns items whose receipt contains at least one accepted delivery.
    ///
    /// Dropped, filtered-only, rejected-only, and globally failed items are not
    /// counted. This count is not mutually exclusive with `failure_count()`:
    /// one item may have both accepted and rejected subscriber deliveries.
    pub fn accepted_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| {
                matches!(
                    item.result(),
                    Ok(receipt)
                        if matches!(
                            receipt.outcome(),
                            PublishOutcome::Dispatched(items)
                                if items.iter().any(|item| matches!(
                                    item.status(),
                                    DispatchStatus::Accepted
                                ))
                        )
                )
            })
            .count()
    }
    /// Returns publisher-dropped items.
    pub fn dropped_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| matches!(item.result(), Ok(receipt) if matches!(receipt.outcome(), PublishOutcome::Dropped)))
            .count()
    }
    /// Returns items with a global publish error or subscriber rejection.
    pub fn failure_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| item.result().is_err() || matches!(item.result(), Ok(receipt) if receipt.has_rejections()))
            .count()
    }
    /// Returns items that have a global publish error.
    pub fn failures(&self) -> &[BatchPublishFailure] {
        &self.failures
    }
    /// Returns whether no item had a global error or subscriber rejection.
    pub fn is_success(&self) -> bool {
        self.failure_count() == 0
    }
}
