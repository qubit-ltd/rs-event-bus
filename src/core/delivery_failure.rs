// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Final subscriber delivery failure reports.

use crate::EventBusError;
use crate::PublishReceipt;

/// Dead-letter result attached to a final delivery failure.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DeadLetterOutcome {
    /// No dead-letter strategy applied.
    NotConfigured,
    /// A strategy intentionally returned no envelope.
    DroppedByStrategy,
    /// A dead-letter publish returned its admission receipt.
    Publication(PublishReceipt),
    /// Dead-letter creation or publication failed before a receipt existed.
    Failed(EventBusError),
}

/// Structured report emitted once after a subscriber delivery reaches a
/// terminal failure.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeliveryFailure {
    event_id: String,
    topic_name: String,
    subscription_id: usize,
    subscriber_id: String,
    error: EventBusError,
    acknowledged_by_error_handler: bool,
    dead_letter: DeadLetterOutcome,
}

impl DeliveryFailure {
    pub(crate) fn new(
        event_id: String,
        topic_name: String,
        subscription_id: usize,
        subscriber_id: String,
        error: EventBusError,
        acknowledged_by_error_handler: bool,
        dead_letter: DeadLetterOutcome,
    ) -> Self {
        Self {
            event_id,
            topic_name,
            subscription_id,
            subscriber_id,
            error,
            acknowledged_by_error_handler,
            dead_letter,
        }
    }
    /// Returns the delivered event identifier.
    pub fn event_id(&self) -> &str {
        &self.event_id
    }
    /// Returns the topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }
    /// Returns the subscription identifier.
    pub fn subscription_id(&self) -> usize {
        self.subscription_id
    }
    /// Returns the subscriber identifier.
    pub fn subscriber_id(&self) -> &str {
        &self.subscriber_id
    }
    /// Returns the terminal processing error.
    pub fn error(&self) -> &EventBusError {
        &self.error
    }
    /// Returns whether an error handler acknowledged the failure.
    pub fn acknowledged_by_error_handler(&self) -> bool {
        self.acknowledged_by_error_handler
    }
    /// Returns the dead-letter result.
    pub fn dead_letter(&self) -> &DeadLetterOutcome {
        &self.dead_letter
    }
}
