// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Live subscription queues grouped by topic.

use std::any::TypeId;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Weak;

use qubit_id::Id;

use super::local_queue::LocalQueue;

/// Live subscriptions sharing one topic and native payload type.
#[derive(Default)]
pub(in crate::local) struct TopicSubscriptions {
    /// Live queues indexed in stable subscription identifier order.
    pub(in crate::local) queues: BTreeMap<Id, Weak<LocalQueue>>,
    /// Payload type bound while this bucket has live subscriptions.
    pub(in crate::local) payload_type_id: Option<TypeId>,
}

impl TopicSubscriptions {
    /// Removes dead subscriptions and returns live queues in identifier order.
    ///
    /// # Returns
    /// Strong owners of live queues ordered by subscription ID.
    pub(in crate::local) fn live_queues(&mut self) -> Vec<Arc<LocalQueue>> {
        self.queues.retain(|_, queue| queue.strong_count() > 0);
        let queues = self
            .queues
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        if queues.is_empty() {
            self.payload_type_id = None;
        }
        queues
    }

    /// Removes an entry only when it still points to the closing queue.
    ///
    /// # Parameters
    /// - `id`: subscription identifier to remove.
    /// - `queue`: queue expected at that identifier.
    ///
    /// # Returns
    /// `true` when the stored entry matched and was removed.
    pub(in crate::local) fn remove(&mut self, id: Id, queue: &Arc<LocalQueue>) -> bool {
        let matches = self
            .queues
            .get(&id)
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, queue));
        if matches {
            self.queues.remove(&id);
        }
        if self.queues.is_empty() {
            self.payload_type_id = None;
        }
        matches
    }
}
