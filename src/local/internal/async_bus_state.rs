// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Registry state shared by asynchronous local bus operations.

use std::any::TypeId;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use qubit_id::Id;

use super::MailboxKey;
use super::async_mailbox::AsyncMailbox;
use crate::spi::ShutdownOutcome;
use crate::spi::TopicAddress;

/// Registry and lifecycle state shared by asynchronous local bus calls.
#[derive(Default)]
pub(in crate::local) struct AsyncBusState {
    /// Rejects new subscriptions and publications after shutdown starts.
    pub(in crate::local) closed: bool,
    /// Stable outcome returned after provider shutdown completes.
    pub(in crate::local) outcome: Option<ShutdownOutcome>,
    /// Mailboxes keyed by their bus-local subscription IDs.
    pub(in crate::local) mailboxes: HashMap<MailboxKey, Arc<AsyncMailbox>>,
    /// Subscription IDs grouped by topic in deterministic order for routing.
    topic_members: HashMap<TopicAddress, BTreeSet<Id>>,
    /// Native payload type bound to each active topic.
    pub(in crate::local) payload_types: HashMap<TopicAddress, TypeId>,
}

impl AsyncBusState {
    /// Returns live mailboxes for a topic in subscription-ID order.
    ///
    /// # Parameters
    /// - `topic`: destination to look up.
    ///
    /// # Returns
    /// Strong mailbox references for active subscriptions on the topic.
    ///
    /// # Panics
    /// Panics in debug builds if an indexed mailbox belongs to another topic.
    #[must_use = "use the mailboxes indexed for this topic"]
    pub(in crate::local) fn mailboxes_for_topic(&self, topic: &TopicAddress) -> Vec<Arc<AsyncMailbox>> {
        let mailboxes = self
            .topic_members
            .get(topic)
            .into_iter()
            .flatten()
            .filter_map(|id| self.mailboxes.get(&MailboxKey { subscription_id: *id }).cloned())
            .collect::<Vec<_>>();
        debug_assert!(mailboxes.iter().all(|mailbox| mailbox.queue.topic == *topic));
        mailboxes
    }

    /// Returns whether a topic has any indexed subscribers.
    ///
    /// # Parameters
    /// - `topic`: destination to check.
    ///
    /// # Returns
    /// `true` when at least one subscription ID is indexed.
    #[must_use = "Use the returned query result."]
    #[inline]
    pub(in crate::local) fn has_topic(&self, topic: &TopicAddress) -> bool {
        self.topic_members.get(topic).is_some_and(|members| !members.is_empty())
    }

    /// Inserts a mailbox and indexes it under its topic.
    ///
    /// # Parameters
    /// - `key`: unique provider mailbox identity.
    /// - `mailbox`: queue and wake state to register.
    ///
    /// # Returns
    /// `true` when inserted, or `false` if the key already exists.
    ///
    /// # Panics
    /// Panics in debug builds if the subscription ID is already present in the
    /// topic index or the inserted mailbox is indexed under another topic.
    pub(in crate::local) fn insert_mailbox(&mut self, key: MailboxKey, mailbox: Arc<AsyncMailbox>) -> bool {
        if self.mailboxes.contains_key(&key) {
            return false;
        }
        let topic = mailbox.queue.topic.clone();
        let id = mailbox.queue.id;
        self.mailboxes.insert(key, mailbox);
        let inserted = self.topic_members.entry(topic.clone()).or_default().insert(id);
        debug_assert!(inserted, "subscription ID must appear once in the topic index");
        debug_assert!(self.mailboxes.get(&key).is_some_and(|entry| entry.queue.topic == topic));
        true
    }

    /// Removes a mailbox only when the indexed `Arc` still matches.
    ///
    /// # Parameters
    /// - `key`: mailbox identity to remove.
    /// - `mailbox`: expected mailbox allocation.
    ///
    /// # Returns
    /// `true` when the mailbox was current and removed.
    ///
    /// # Panics
    /// Panics in debug builds if the primary mailbox entry has no matching
    /// topic-index entry.
    pub(in crate::local) fn remove_mailbox_if_same(&mut self, key: MailboxKey, mailbox: &Arc<AsyncMailbox>) -> bool {
        let is_current = self
            .mailboxes
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, mailbox));
        if !is_current {
            return false;
        }
        self.mailboxes.remove(&key);
        let topic = &mailbox.queue.topic;
        if let Some(members) = self.topic_members.get_mut(topic) {
            let removed = members.remove(&mailbox.queue.id);
            debug_assert!(removed, "primary mailbox entry must appear in its topic index");
            if members.is_empty() {
                self.topic_members.remove(topic);
            }
        }
        true
    }

    /// Removes all mailboxes and clears the topic index.
    ///
    /// # Returns
    /// Strong owners of every previously registered mailbox.
    pub(in crate::local) fn drain_mailboxes(&mut self) -> Vec<Arc<AsyncMailbox>> {
        self.topic_members.clear();
        std::mem::take(&mut self.mailboxes).into_values().collect()
    }
}
