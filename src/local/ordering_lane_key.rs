// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Ordering lane key for local subscriber delivery.

use crate::TopicKey;

/// Ordering lane identifier for one subscriber's ordered deliveries.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct OrderingLaneKey {
    topic_key: TopicKey,
    ordering_key: String,
    subscription_id: usize,
}

impl OrderingLaneKey {
    /// Creates an ordering lane key.
    ///
    /// # Parameters
    /// - `topic_key`: Topic receiving the event.
    /// - `ordering_key`: Envelope ordering key.
    /// - `subscription_id`: Subscription receiving the event.
    ///
    /// # Returns
    /// Lane key that serializes work for this subscriber.
    pub(crate) fn new(
        topic_key: TopicKey,
        ordering_key: impl Into<String>,
        subscription_id: usize,
    ) -> Self {
        Self {
            topic_key,
            ordering_key: ordering_key.into(),
            subscription_id,
        }
    }
}
