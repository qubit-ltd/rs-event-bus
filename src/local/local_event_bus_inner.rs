// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
//! Shared state for the local event bus.

mod admission;
mod lifecycle;
mod ordering_lane;
mod processing_tracker;
mod scheduling;
use std::any::Any;
use std::any::TypeId;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

pub(crate) use admission::DeliveryPermit;
use lifecycle::LifecycleState;
use lifecycle::LocalEventBusLifecycle;
use ordering_lane::OrderedProcessingLane;
use processing_tracker::ProcessingTracker;
use qubit_collections::map::OrderedIndexMap;

use super::erased_subscription::ErasedSubscription;
use super::local_event_bus::PublisherInterceptorAny;
use super::local_event_bus::SubscriberInterceptorAny;
use super::ordering_lane_key::OrderingLaneKey;
use super::publisher_interceptor_entry::PublisherInterceptorEntry;
use super::subscriber_interceptor_entry::SubscriberInterceptorEntry;
use crate::DeliveryFailure;
use crate::EventBusError;
use crate::EventBusResult;
use crate::PublishOptions;
use crate::SubscribeOptions;
use crate::TopicKey;
use crate::core::delivery_limits::DeliveryLimits;
use crate::core::subscribe_options::DeadLetterStrategyAnyFn;

type ErrorObserverFn = dyn Fn(&EventBusError) + Send + Sync + 'static;
type DeliveryFailureObserverFn = dyn Fn(&DeliveryFailure) + Send + Sync + 'static;
type TypeErasedDefaults = HashMap<TypeId, Arc<dyn Any + Send + Sync>>;
type TopicSubscriptions = OrderedIndexMap<usize, Reverse<i32>, Arc<dyn ErasedSubscription>>;

/// Runtime options used to construct a local event bus.
pub(crate) struct LocalEventBusRuntimeOptions {
    pub(crate) default_publish_options: TypeErasedDefaults,
    pub(crate) default_subscribe_options: TypeErasedDefaults,
    pub(crate) default_dead_letter_strategies: TypeErasedDefaults,
    pub(crate) global_default_dead_letter_strategy: Option<Arc<DeadLetterStrategyAnyFn>>,
    pub(crate) global_publisher_interceptors: Vec<Arc<dyn PublisherInterceptorAny>>,
    pub(crate) global_subscriber_interceptors: Vec<Arc<dyn SubscriberInterceptorAny>>,
    pub(crate) publisher_interceptors: Vec<Arc<dyn PublisherInterceptorEntry>>,
    pub(crate) subscriber_interceptors: Vec<Arc<dyn SubscriberInterceptorEntry>>,
    pub(crate) subscription_handler_pool_size: usize,
    pub(crate) delivery_limits: DeliveryLimits,
}
/// Shared mutable state for [`crate::LocalEventBus`].
pub(crate) struct LocalEventBusInner {
    lifecycle: Mutex<LocalEventBusLifecycle>,
    subscriptions: Mutex<HashMap<TopicKey, TopicSubscriptions>>,
    global_publisher_interceptors: Mutex<Vec<Arc<dyn PublisherInterceptorAny>>>,
    global_subscriber_interceptors: Mutex<Vec<Arc<dyn SubscriberInterceptorAny>>>,
    publisher_interceptors: Mutex<Vec<Arc<dyn PublisherInterceptorEntry>>>,
    subscriber_interceptors: Mutex<Vec<Arc<dyn SubscriberInterceptorEntry>>>,
    error_observers: Mutex<Vec<Arc<ErrorObserverFn>>>,
    delivery_failure_observers: Mutex<Vec<Arc<DeliveryFailureObserverFn>>>,
    ordering_lanes: Mutex<HashMap<OrderingLaneKey, OrderedProcessingLane>>,
    ordered_queued_task_count: AtomicUsize,
    pub(super) in_flight_delivery_count: Arc<AtomicUsize>,
    processing_tracker: ProcessingTracker,
    next_subscription_id: AtomicUsize,
    default_publish_options: TypeErasedDefaults,
    default_subscribe_options: TypeErasedDefaults,
    default_dead_letter_strategies: TypeErasedDefaults,
    global_default_dead_letter_strategy: Option<Arc<DeadLetterStrategyAnyFn>>,
    subscription_handler_pool_size: usize,
    pub(super) delivery_limits: DeliveryLimits,
}

impl LocalEventBusInner {
    /// Creates shared local event bus state.
    ///
    /// # Parameters
    /// - `default_subscribe_options`: Typed default subscription options.
    /// - `subscription_handler_pool_size`: Worker count for subscriber
    ///   handlers.
    /// - `delivery_limits`: Independent admission and executor queue limits.
    ///
    /// # Returns
    /// Shared state initialized in the stopped lifecycle state.
    pub(crate) fn new(options: LocalEventBusRuntimeOptions) -> Self {
        Self {
            lifecycle: Mutex::new(LocalEventBusLifecycle::stopped()),
            subscriptions: Mutex::new(HashMap::new()),
            global_publisher_interceptors: Mutex::new(options.global_publisher_interceptors),
            global_subscriber_interceptors: Mutex::new(options.global_subscriber_interceptors),
            publisher_interceptors: Mutex::new(options.publisher_interceptors),
            subscriber_interceptors: Mutex::new(options.subscriber_interceptors),
            error_observers: Mutex::new(Vec::new()),
            delivery_failure_observers: Mutex::new(Vec::new()),
            ordering_lanes: Mutex::new(HashMap::new()),
            ordered_queued_task_count: AtomicUsize::new(0),
            in_flight_delivery_count: Arc::new(AtomicUsize::new(0)),
            processing_tracker: ProcessingTracker::new(),
            next_subscription_id: AtomicUsize::new(1),
            default_publish_options: options.default_publish_options,
            default_subscribe_options: options.default_subscribe_options,
            default_dead_letter_strategies: options.default_dead_letter_strategies,
            global_default_dead_letter_strategy: options.global_default_dead_letter_strategy,
            subscription_handler_pool_size: options.subscription_handler_pool_size,
            delivery_limits: options.delivery_limits,
        }
    }

    /// Allocates a new subscription ID.
    ///
    /// # Returns
    /// Process-local subscription ID.
    pub(crate) fn next_subscription_id(&self) -> usize {
        self.next_subscription_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Returns typed default publish options.
    ///
    /// # Returns
    /// Type-specific default options if configured.
    pub(crate) fn default_publish_options<T>(&self) -> Option<PublishOptions<T>>
    where
        T: 'static,
    {
        self.default_publish_options
            .get(&TypeId::of::<T>())
            .and_then(|options| options.downcast_ref::<PublishOptions<T>>())
            .cloned()
    }

    /// Returns typed default subscribe options.
    ///
    /// # Returns
    /// Type-specific default options if configured.
    pub(crate) fn default_subscribe_options<T>(&self) -> Option<SubscribeOptions<T>>
    where
        T: 'static,
    {
        self.default_subscribe_options
            .get(&TypeId::of::<T>())
            .and_then(|options| options.downcast_ref::<SubscribeOptions<T>>())
            .cloned()
    }

    /// Returns a typed default dead-letter strategy.
    ///
    /// # Returns
    /// Type-specific strategy if configured.
    pub(crate) fn default_dead_letter_strategy<T>(
        &self,
    ) -> Option<Arc<crate::core::subscribe_options::DeadLetterStrategyFn<T>>>
    where
        T: 'static,
    {
        self.default_dead_letter_strategies
            .get(&TypeId::of::<T>())
            .and_then(|strategy| {
                strategy.downcast_ref::<Arc<crate::core::subscribe_options::DeadLetterStrategyFn<T>>>()
            })
            .cloned()
    }

    /// Returns the global default dead-letter strategy.
    ///
    /// # Returns
    /// Type-erased strategy if configured.
    pub(crate) fn global_default_dead_letter_strategy(&self) -> Option<Arc<DeadLetterStrategyAnyFn>> {
        self.global_default_dead_letter_strategy.clone()
    }

    /// Returns registered global publisher interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn global_publisher_interceptors(&self) -> EventBusResult<Vec<Arc<dyn PublisherInterceptorAny>>> {
        Ok(self
            .global_publisher_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("global_publisher_interceptors"))?
            .clone())
    }

    /// Returns registered publisher interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn publisher_interceptors(&self) -> EventBusResult<Vec<Arc<dyn PublisherInterceptorEntry>>> {
        Ok(self
            .publisher_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("publisher_interceptors"))?
            .clone())
    }

    /// Returns registered subscriber interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn subscriber_interceptors(&self) -> EventBusResult<Vec<Arc<dyn SubscriberInterceptorEntry>>> {
        Ok(self
            .subscriber_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriber_interceptors"))?
            .clone())
    }

    /// Returns registered global subscriber interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn global_subscriber_interceptors(&self) -> EventBusResult<Vec<Arc<dyn SubscriberInterceptorAny>>> {
        Ok(self
            .global_subscriber_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("global_subscriber_interceptors"))?
            .clone())
    }

    /// Adds an error observer.
    ///
    /// # Parameters
    /// - `observer`: Callback notified about internal callback failures.
    ///
    /// # Returns
    /// `Ok(())` when the observer is stored.
    pub(crate) fn add_error_observer(&self, observer: Arc<ErrorObserverFn>) -> EventBusResult<()> {
        self.error_observers
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("error_observers"))?
            .push(observer);
        Ok(())
    }

    pub(crate) fn add_delivery_failure_observer(&self, observer: Arc<DeliveryFailureObserverFn>) -> EventBusResult<()> {
        self.delivery_failure_observers
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("delivery_failure_observers"))?
            .push(observer);
        Ok(())
    }

    pub(crate) fn observe_delivery_failure(&self, failure: &DeliveryFailure) {
        let Ok(observers) = self
            .delivery_failure_observers
            .lock()
            .map(|observers| observers.clone())
        else {
            return;
        };
        for observer in observers {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(failure)));
        }
    }

    /// Notifies registered error observers.
    ///
    /// # Parameters
    /// - `error`: Internal failure to observe.
    pub(crate) fn observe_error(&self, error: &EventBusError) {
        let Ok(observers) = self.error_observers.lock().map(|observers| observers.clone()) else {
            return;
        };
        for observer in observers {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(error)));
        }
    }

    /// Adds a subscription entry.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key.
    /// - `subscription`: Type-erased subscription entry.
    ///
    /// # Returns
    /// `Ok(())` when the entry is stored.
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
    /// # Errors
    /// Returns `NotStarted` after shutdown begins or a lock error when state is
    /// unavailable.
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
    /// A cloned list of subscription entries.
    pub(crate) fn subscriptions_for(&self, topic_key: &TopicKey) -> EventBusResult<Vec<Arc<dyn ErasedSubscription>>> {
        Ok(self
            .subscriptions
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriptions"))?
            .get(topic_key)
            .map(|entries| entries.values_ordered().map(Arc::clone).collect())
            .unwrap_or_default())
    }

    /// Removes a subscription entry.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key containing the subscription.
    /// - `id`: Subscription ID.
    ///
    /// # Returns
    /// `Ok(())` after removal.
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

    /// Clears all subscriptions.
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
    pub(crate) fn wait_for_idle_timeout(&self, topic_key: &TopicKey, timeout: Duration) -> EventBusResult<bool> {
        self.processing_tracker.wait_for_idle_timeout(topic_key, timeout)
    }

    /// Waits until all topics have zero active work.
    ///
    /// # Returns
    /// `Ok(())` once all tracked topics are idle.
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
    pub(crate) fn wait_for_all_idle_timeout(&self, timeout: Duration) -> EventBusResult<bool> {
        self.processing_tracker.wait_for_all_idle_timeout(timeout)
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::thread;

    use super::LocalEventBusInner;
    use super::LocalEventBusRuntimeOptions;
    use crate::EventBusError;
    use crate::EventBusResult;
    use crate::LocalEventBus;
    use crate::Topic;
    use crate::core::delivery_limits::DeliveryLimits;
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

    /// Restart is forbidden until shutdown finalization clears the state.
    #[test]
    fn test_start_rejects_stopping_state() {
        let bus = LocalEventBus::started().expect("bus should start");
        assert!(bus.inner.mark_stopping());
        assert!(matches!(bus.start(), Err(EventBusError::StartFailed { .. })));
    }

    #[test]
    fn test_unbounded_delivery_permit_preserves_counter() {
        let inner = LocalEventBusInner::new(LocalEventBusRuntimeOptions {
            default_publish_options: HashMap::new(),
            default_subscribe_options: HashMap::new(),
            default_dead_letter_strategies: HashMap::new(),
            global_default_dead_letter_strategy: None,
            global_publisher_interceptors: Vec::new(),
            global_subscriber_interceptors: Vec::new(),
            publisher_interceptors: Vec::new(),
            subscriber_interceptors: Vec::new(),
            subscription_handler_pool_size: 1,
            delivery_limits: DeliveryLimits::default(),
        });

        let first = inner
            .try_acquire_delivery_permit()
            .expect("unbounded permit should be available");
        drop(first);
        let second = inner
            .try_acquire_delivery_permit()
            .expect("unbounded permit should remain available");
        drop(second);

        assert_eq!(
            inner.in_flight_delivery_count.load(Ordering::SeqCst),
            0,
            "unbounded permits must not mutate the bounded counter"
        );
    }
}
