// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscription storage and processing-tracker delegation.

use std::cmp::Reverse;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::LocalEventBusInner;
use super::lifecycle::LifecycleState;
use crate::EventBusError;
use crate::EventBusResult;
use crate::TopicKey;
use crate::local::erased_subscription::ErasedSubscription;

impl LocalEventBusInner {
    /// Allocates a new subscription ID.
    ///
    /// # Returns
    /// Process-local subscription ID.
    pub(crate) fn next_subscription_id(&self) -> usize {
        self.next_subscription_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Adds a subscription entry.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key.
    /// - `subscription`: Type-erased subscription entry.
    ///
    /// # Returns
    /// `Ok(())` when the entry is stored.
    ///
    /// # Errors
    /// Returns a lock-poisoning error when subscription storage is unavailable.
    pub(crate) fn add_subscription(
        &self,
        topic_key: TopicKey,
        subscription: Arc<dyn ErasedSubscription>,
    ) -> EventBusResult<()> {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriptions"))?;
        let id = subscription.id();
        let inserted =
            subscriptions
                .entry(topic_key)
                .or_default()
                .try_insert(id, Reverse(subscription.priority()), subscription);
        assert!(inserted.is_ok(), "subscription ID must be unique");
        Ok(())
    }

    /// Registers a subscription atomically with the lifecycle start check.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key.
    /// - `subscription`: Type-erased subscription entry.
    ///
    /// # Returns
    /// `Ok(())` when the started bus stores the subscription.
    ///
    /// # Errors
    /// Returns `NotStarted` after shutdown begins or a lock error when
    /// lifecycle or subscription state is unavailable.
    pub(crate) fn register_subscription_if_started(
        &self,
        topic_key: TopicKey,
        subscription: Arc<dyn ErasedSubscription>,
    ) -> EventBusResult<()> {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
        if lifecycle.state != LifecycleState::Started {
            return Err(EventBusError::not_started());
        }
        self.add_subscription(topic_key, subscription)
    }

    /// Returns subscriptions for a topic key.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to look up.
    ///
    /// # Returns
    /// A cloned list of subscription entries in dispatch order.
    ///
    /// # Errors
    /// Returns a lock-poisoning error when subscription storage is unavailable.
    pub(crate) fn subscriptions_for(&self, topic_key: &TopicKey) -> EventBusResult<Vec<Arc<dyn ErasedSubscription>>> {
        Ok(self
            .subscriptions
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriptions"))?
            .get(topic_key)
            .map(|entries| entries.values_ordered().map(Arc::clone).collect())
            .unwrap_or_default())
    }

    /// Removes and deactivates a subscription entry.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key containing the subscription.
    /// - `id`: Subscription ID.
    ///
    /// # Returns
    /// `Ok(())` after removal, including when the subscription is absent.
    ///
    /// # Errors
    /// Returns a lock-poisoning error when subscription storage is unavailable.
    pub(crate) fn unsubscribe(&self, topic_key: &TopicKey, id: usize) -> EventBusResult<()> {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriptions"))?;
        let removed = if let Some(entries) = subscriptions.get_mut(topic_key) {
            let removed = entries.remove(&id);
            if entries.is_empty() {
                subscriptions.remove(topic_key);
            }
            removed
        } else {
            None
        };
        if let Some(entry) = removed {
            entry.into_value().deactivate();
        }
        Ok(())
    }

    /// Deactivates and clears all subscriptions.
    ///
    /// A poisoned subscription lock suppresses cleanup because the stored
    /// entries cannot be accessed safely.
    pub(crate) fn clear_subscriptions(&self) {
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            for entries in subscriptions.values() {
                for entry in entries.values_ordered() {
                    entry.deactivate();
                }
            }
            subscriptions.clear();
        }
    }

    /// Increments active work for a topic.
    ///
    /// # Parameters
    /// - `topic_key`: Topic receiving new handler work.
    ///
    /// # Returns
    /// `Ok(())` after incrementing the count.
    ///
    /// # Errors
    /// Returns a lock-poisoning error when processing state is unavailable.
    pub(crate) fn start_processing(&self, topic_key: &TopicKey) -> EventBusResult<()> {
        self.processing_tracker.start(topic_key)
    }

    /// Decrements active work for a topic.
    ///
    /// # Parameters
    /// - `topic_key`: Topic whose handler work finished.
    pub(crate) fn finish_processing(&self, topic_key: &TopicKey) {
        self.processing_tracker.finish(topic_key);
    }

    /// Waits until a topic has zero active work.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to wait for.
    ///
    /// # Returns
    /// `Ok(())` once the topic is idle.
    ///
    /// # Errors
    /// Returns a lock-poisoning error when processing state is unavailable.
    pub(crate) fn wait_for_idle(&self, topic_key: &TopicKey) -> EventBusResult<()> {
        self.processing_tracker.wait_for_idle(topic_key)
    }

    /// Waits until a topic has zero active work or the timeout elapses.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to wait for.
    /// - `timeout`: Maximum duration to wait.
    ///
    /// # Returns
    /// `Ok(true)` once the topic is idle, or `Ok(false)` when the timeout
    /// elapses first.
    ///
    /// # Errors
    /// Returns a lock-poisoning error when processing state is unavailable.
    pub(crate) fn wait_for_idle_timeout(&self, topic_key: &TopicKey, timeout: Duration) -> EventBusResult<bool> {
        self.processing_tracker.wait_for_idle_timeout(topic_key, timeout)
    }

    /// Waits until all topics have zero active work.
    ///
    /// # Returns
    /// `Ok(())` once all tracked topics are idle.
    ///
    /// # Errors
    /// Returns a lock-poisoning error when processing state is unavailable.
    pub(crate) fn wait_for_all_idle(&self) -> EventBusResult<()> {
        self.processing_tracker.wait_for_all_idle()
    }

    /// Waits until all topics have zero active work or the timeout elapses.
    ///
    /// # Parameters
    /// - `timeout`: Maximum duration to wait.
    ///
    /// # Returns
    /// `Ok(true)` once all tracked topics are idle, or `Ok(false)` when the
    /// timeout elapses first.
    ///
    /// # Errors
    /// Returns a lock-poisoning error when processing state is unavailable.
    pub(crate) fn wait_for_all_idle_timeout(&self, timeout: Duration) -> EventBusResult<bool> {
        self.processing_tracker.wait_for_all_idle_timeout(timeout)
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;
    use std::thread;

    use super::LocalEventBusInner;
    use crate::EventBusError;
    use crate::EventBusResult;
    use crate::LocalEventBus;
    use crate::Topic;
    use crate::local::erased_subscription::DispatchAdmission;
    use crate::local::erased_subscription::ErasedSubscription;

    struct TestSubscription;

    impl ErasedSubscription for TestSubscription {
        fn id(&self) -> usize {
            1
        }

        fn subscriber_id(&self) -> &str {
            "test"
        }

        fn priority(&self) -> i32 {
            0
        }

        fn deactivate(&self) {}

        fn dispatch(
            &self,
            _envelope: Box<dyn Any + Send>,
            _bus: Arc<LocalEventBusInner>,
            _allow_stopping: bool,
        ) -> EventBusResult<DispatchAdmission> {
            Ok(DispatchAdmission::Accepted)
        }
    }

    /// Registration after the shutdown transition must not revive a cleared
    /// subscription.
    #[test]
    fn test_registration_rejects_after_shutdown_clears_subscriptions() {
        let bus = LocalEventBus::started().expect("bus should start");
        let topic = Topic::<String>::try_new("shutdown-registration").expect("topic should build");
        assert!(bus.inner.is_started());
        assert!(bus.inner.mark_stopping());
        bus.inner.clear_subscriptions();
        let result = bus
            .inner
            .register_subscription_if_started(topic.key(), Arc::new(TestSubscription));
        assert!(matches!(result, Err(EventBusError::NotStarted)));
        assert!(
            bus.inner
                .subscriptions_for(&topic.key())
                .expect("lookup should work")
                .is_empty()
        );
    }

    /// A cancellation that fails to remove storage must remain retryable.
    #[test]
    fn test_failed_unsubscribe_keeps_handle_active() {
        let bus = LocalEventBus::started().expect("bus should start");
        let topic = Topic::<String>::try_new("poisoned-unsubscribe").expect("topic should build");
        let handle = bus
            .subscribe("sub", &topic, |_| ())
            .expect("subscription should register");
        let inner = Arc::clone(&bus.inner);
        assert!(
            thread::spawn(move || {
                let _guard = inner
                    .subscriptions
                    .lock()
                    .expect("subscriptions lock should be available");
                panic!("poison subscriptions lock for cancellation test");
            })
            .join()
            .is_err()
        );
        assert!(handle.cancel().is_err());
        assert!(handle.is_active(), "failed cancellation must remain retryable");
    }
}
