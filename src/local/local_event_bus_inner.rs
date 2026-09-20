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
mod observers;
mod ordering_lane;
mod processing_tracker;
mod scheduling;
mod subscriptions;
use std::any::Any;
use std::any::TypeId;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

pub(crate) use admission::DeliveryPermit;
use lifecycle::LocalEventBusLifecycle;
use observers::DeliveryFailureObserverFn;
use observers::ErrorObserverFn;
use ordering_lane::OrderedProcessingLane;
use processing_tracker::ProcessingTracker;
use qubit_collections::map::OrderedIndexMap;

use super::erased_subscription::ErasedSubscription;
use super::local_event_bus::PublisherInterceptorAny;
use super::local_event_bus::SubscriberInterceptorAny;
use super::ordering_lane_key::OrderingLaneKey;
use super::publisher_interceptor_entry::PublisherInterceptorEntry;
use super::subscriber_interceptor_entry::SubscriberInterceptorEntry;
use crate::EventBusError;
use crate::EventBusResult;
use crate::PublishOptions;
use crate::SubscribeOptions;
use crate::TopicKey;
use crate::core::delivery_limits::DeliveryLimits;
use crate::core::subscribe_options::DeadLetterStrategyAnyFn;

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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    use super::LocalEventBusInner;
    use super::LocalEventBusRuntimeOptions;
    use crate::EventBusError;
    use crate::LocalEventBus;
    use crate::core::delivery_limits::DeliveryLimits;

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
