// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Identity of one per-key ordered lane.

use qubit_id::Id;

/// Identity of one per-key ordered lane.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct OrderingLaneKey {
    /// Topic name that scopes the lane.
    topic: Box<str>,
    /// Optional ordering key within the topic.
    ordering_key: Option<Box<str>>,
    /// Subscription identity that isolates independent receivers.
    subscription_id: Id,
}

impl OrderingLaneKey {
    /// Creates a lane identity from stable event and subscription metadata.
    ///
    /// # Parameters
    /// - `topic`: destination topic name.
    /// - `ordering_key`: optional per-key ordering value.
    /// - `subscription_id`: receiver whose lane is being identified.
    ///
    /// # Returns
    /// A stable key that scopes ordering to the subscription and topic.
    #[must_use]
    pub(crate) fn new(topic: &str, ordering_key: Option<&str>, subscription_id: Id) -> Self {
        Self {
            topic: topic.into(),
            ordering_key: ordering_key.map(Into::into),
            subscription_id,
        }
    }
}
