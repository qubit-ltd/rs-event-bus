// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider-wide routing and shutdown state.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use qubit_id::Id;

use super::local_queue::LocalQueue;
use super::topic_subscriptions::TopicSubscriptions;
use crate::spi::ShutdownOutcome;
use crate::spi::TopicAddress;

/// Provider-wide routing table and shutdown state.
#[derive(Default)]
pub(in crate::local) struct BusState {
    /// Topic-local queues and payload type bindings.
    pub(in crate::local) topics: HashMap<TopicAddress, TopicSubscriptions>,
    /// Live subscription identifiers are unique across the provider.
    pub(in crate::local) subscription_ids: HashSet<Id>,
    /// Rejects new provider operations after shutdown begins.
    pub(in crate::local) closed: bool,
    /// Stable outcome returned by all later shutdown calls.
    pub(in crate::local) shutdown_outcome: Option<ShutdownOutcome>,
    /// Change counter that prevents missed graceful-shutdown notifications.
    pub(in crate::local) change_version: u64,
}

impl BusState {
    /// Removes dead identifiers in one topic and returns its ordered live
    /// queues.
    ///
    /// # Parameters
    /// - `topic`: topic bucket to prune.
    ///
    /// # Returns
    /// Strong owners of currently live queues for the topic.
    #[must_use]
    pub(in crate::local) fn live_queues_for_topic(&mut self, topic: &TopicAddress) -> Vec<Arc<LocalQueue>> {
        let Some(bucket) = self.topics.get_mut(topic) else {
            return Vec::new();
        };
        let dead_ids = bucket
            .queues
            .iter()
            .filter_map(|(id, queue)| (queue.strong_count() == 0).then_some(*id))
            .collect::<Vec<_>>();
        let queues = bucket.live_queues();
        for id in dead_ids {
            self.subscription_ids.remove(&id);
        }
        if bucket.queues.is_empty() {
            self.topics.remove(topic);
        }
        queues
    }

    /// Collects all live queues while pruning stale topic entries and IDs.
    ///
    /// # Returns
    /// Strong owners of all currently live subscription queues.
    #[must_use]
    pub(in crate::local) fn live_queues(&mut self) -> Vec<Arc<LocalQueue>> {
        let topics = self.topics.keys().cloned().collect::<Vec<_>>();
        let mut queues = Vec::new();
        for topic in topics {
            queues.extend(self.live_queues_for_topic(&topic));
        }
        queues
    }

    /// Removes one stale provider-wide identifier from whichever bucket owns
    /// it.
    ///
    /// # Parameters
    /// - `id`: stale subscription identifier to remove.
    pub(in crate::local) fn remove_stale_id(&mut self, id: Id) {
        for bucket in self.topics.values_mut() {
            if bucket.queues.get(&id).is_some_and(|queue| queue.strong_count() == 0) {
                bucket.queues.remove(&id);
                if bucket.queues.is_empty() {
                    bucket.payload_type_id = None;
                }
                break;
            }
        }
        self.subscription_ids.remove(&id);
        self.topics.retain(|_, bucket| !bucket.queues.is_empty());
    }
}
