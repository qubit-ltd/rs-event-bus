// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
//! Results describing which subscriber deliveries were admitted.

use crate::EventBusError;

/// Result of publishing one event to the local dispatch path.
///
/// The receipt describes publisher admission and subscriber admission. It
/// does not wait for subscriber handlers to finish.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::{PublishOutcome, PublishReceipt};
///
/// let receipt = PublishReceipt::new(
///     "input-1".to_owned(),
///     Some("dispatched-1".to_owned()),
///     PublishOutcome::Dropped,
/// );
/// assert_eq!(receipt.input_event_id(), "input-1");
/// assert!(!receipt.has_rejections());
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublishReceipt {
    /// Identifier supplied by the publisher before interception.
    input_event_id: String,
    /// Identifier of the envelope after interception, if it was dispatched.
    dispatched_event_id: Option<String>,
    /// Interception and subscriber-admission outcome.
    outcome: PublishOutcome,
}

impl PublishReceipt {
    /// Creates a receipt for a published event.
    #[must_use]
    pub fn new(input_event_id: String, dispatched_event_id: Option<String>, outcome: PublishOutcome) -> Self {
        Self {
            input_event_id,
            dispatched_event_id,
            outcome,
        }
    }

    /// Returns the identifier supplied by the publisher.
    ///
    /// # Returns
    /// The input envelope's identifier, before any publisher interceptor
    /// transforms it.
    #[must_use]
    #[inline]
    pub fn input_event_id(&self) -> &str {
        &self.input_event_id
    }

    /// Returns the identifier after publisher interceptors ran.
    ///
    /// # Returns
    /// `Some` with the dispatched envelope's identifier when interception
    /// produced an envelope, or `None` when the event was dropped.
    #[must_use]
    #[inline]
    pub fn dispatched_event_id(&self) -> Option<&str> {
        self.dispatched_event_id.as_deref()
    }

    /// Returns the dispatch outcome.
    ///
    /// # Returns
    /// [`PublishOutcome::Dropped`] when interception stopped publication, or
    /// [`PublishOutcome::Dispatched`] with one result per matching subscriber.
    #[must_use]
    #[inline]
    pub fn outcome(&self) -> &PublishOutcome {
        &self.outcome
    }

    /// Returns whether any subscriber delivery was rejected.
    ///
    /// # Returns
    /// `true` when at least one dispatched subscriber has a rejected status;
    /// publisher drops and filtered deliveries return `false`.
    #[must_use]
    #[inline]
    pub fn has_rejections(&self) -> bool {
        matches!(&self.outcome, PublishOutcome::Dispatched(items) if items.iter().any(|item| matches!(item.status(), DispatchStatus::Rejected(_))))
    }
}

/// Outcome of publisher interception and subscriber dispatch.
///
/// ```
/// use qubit_event_bus::PublishOutcome;
///
/// let outcome = PublishOutcome::Dropped;
/// assert!(matches!(outcome, PublishOutcome::Dropped));
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PublishOutcome {
    /// The publisher interceptor intentionally dropped the event.
    Dropped,
    /// The event was examined against the subscriber snapshot.
    Dispatched(Vec<SubscriberDispatchResult>),
}

/// Result for one subscriber in a publish snapshot.
///
/// A value is obtained from [`PublishReceipt::outcome`] after a publish.
///
/// ```
/// use qubit_event_bus::{PublishOutcome, PublishReceipt};
///
/// let receipt = PublishReceipt::new("input".into(), None, PublishOutcome::Dropped);
/// if let PublishOutcome::Dispatched(results) = receipt.outcome() {
///     for result in results {
///         let _subscriber_id = result.subscriber_id();
///     }
/// }
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubscriberDispatchResult {
    /// Internal identifier assigned to the subscription.
    subscription_id: usize,
    /// Application subscriber identifier.
    subscriber_id: String,
    /// Admission status for this subscriber.
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
    ///
    /// # Returns
    /// The internal identifier assigned when this subscription was registered.
    #[must_use]
    #[inline]
    pub fn subscription_id(&self) -> usize {
        self.subscription_id
    }

    /// Returns the application subscriber identifier.
    ///
    /// # Returns
    /// The caller-provided identifier for the subscriber, not the subscription
    /// identifier.
    #[must_use]
    #[inline]
    pub fn subscriber_id(&self) -> &str {
        &self.subscriber_id
    }

    /// Returns the admission status.
    ///
    /// # Returns
    /// The subscriber admission result; it does not represent handler
    /// completion.
    #[must_use]
    #[inline]
    pub fn status(&self) -> &DispatchStatus {
        &self.status
    }
}

/// Admission status for one subscriber delivery.
///
/// ```
/// use qubit_event_bus::DispatchStatus;
///
/// assert!(matches!(DispatchStatus::Accepted, DispatchStatus::Accepted));
/// ```
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
///
/// Batch items are returned by [`BatchPublishResult::items`] in input order.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchPublishItem {
    /// Zero-based input position of this envelope.
    index: usize,
    /// Identifier of the input envelope.
    event_id: String,
    /// Per-envelope admission result.
    result: Result<PublishReceipt, EventBusError>,
}

/// Compatibility view of batch items that failed before admission.
///
/// Batch failures are available from [`BatchPublishResult::failures`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchPublishFailure {
    /// Zero-based input position of the failed envelope.
    index: usize,
    /// Identifier of the failed input envelope.
    event_id: String,
    /// Error raised before a publish receipt was produced.
    error: EventBusError,
}
impl BatchPublishFailure {
    /// Returns the input index.
    ///
    /// # Returns
    /// Zero-based position of the event in the batch passed to `publish_all`.
    #[must_use]
    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }
    /// Returns the input event identifier.
    ///
    /// # Returns
    /// Identifier of the input event whose publication failed before a receipt
    /// could be produced.
    #[must_use]
    #[inline]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }
    /// Returns the global publication error.
    ///
    /// # Returns
    /// Error raised before subscriber admission for this batch item.
    #[must_use]
    #[inline]
    pub fn error(&self) -> &EventBusError {
        &self.error
    }
}

impl BatchPublishItem {
    /// Returns an item produced by a best-effort batch publication.
    pub(crate) fn new(index: usize, event_id: String, result: Result<PublishReceipt, EventBusError>) -> Self {
        Self {
            index,
            event_id,
            result,
        }
    }
    /// Returns the input index.
    ///
    /// # Returns
    /// Zero-based position of this event in the original batch.
    #[must_use]
    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }
    /// Returns the original event identifier.
    ///
    /// # Returns
    /// Identifier from the input envelope, before publisher interception.
    #[must_use]
    #[inline]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }
    /// Returns the per-event publish result.
    ///
    /// # Returns
    /// `Ok` with a receipt when publishing reached admission, or `Err` with
    /// the global publication error.
    #[inline]
    pub fn result(&self) -> &Result<PublishReceipt, EventBusError> {
        &self.result
    }
}

/// Summary of best-effort batch publication.
///
/// ```
/// use qubit_event_bus::{EventEnvelope, LocalEventBus, Topic};
///
/// let bus = LocalEventBus::started().unwrap();
/// let topic = Topic::<String>::try_new("batch.docs").unwrap();
/// let result = bus
///     .publish_all(vec![EventEnvelope::create(topic, "payload".to_owned())])
///     .unwrap();
/// assert_eq!(result.total_count(), 1);
/// for item in result.items() {
///     assert_eq!(item.index(), 0);
/// }
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchPublishResult {
    /// Per-envelope results in input order.
    items: Vec<BatchPublishItem>,
    /// Global publication failures derived from `items`.
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
    ///
    /// # Returns
    /// All batch items, including successful, dropped, rejected, and globally
    /// failed publications.
    #[must_use]
    #[inline]
    pub fn items(&self) -> &[BatchPublishItem] {
        &self.items
    }
    /// Returns the input item count.
    ///
    /// # Returns
    /// Number of events supplied to the batch operation.
    #[must_use]
    #[inline]
    pub fn total_count(&self) -> usize {
        self.items.len()
    }
    /// Returns items whose receipt contains at least one accepted delivery.
    ///
    /// Dropped, filtered-only, rejected-only, and globally failed items are not
    /// counted. This count is not mutually exclusive with `failure_count()`:
    /// one item may have both accepted and rejected subscriber deliveries.
    #[must_use]
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
    ///
    /// # Returns
    /// Number of items whose publisher interceptor returned no envelope.
    #[must_use]
    pub fn dropped_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| matches!(item.result(), Ok(receipt) if matches!(receipt.outcome(), PublishOutcome::Dropped)))
            .count()
    }
    /// Returns items with a global publish error or subscriber rejection.
    ///
    /// # Returns
    /// Number of items with either a global error or at least one rejected
    /// subscriber. This count can overlap with `accepted_count()`.
    #[must_use]
    pub fn failure_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| item.result().is_err() || matches!(item.result(), Ok(receipt) if receipt.has_rejections()))
            .count()
    }
    /// Returns items that have a global publish error.
    ///
    /// # Returns
    /// Global publication failures keyed by the original batch index.
    #[must_use]
    #[inline]
    pub fn failures(&self) -> &[BatchPublishFailure] {
        &self.failures
    }
    /// Returns whether no item had a global error or subscriber rejection.
    ///
    /// # Returns
    /// `true` when every item avoided global errors and subscriber rejections.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.failure_count() == 0
    }
}
