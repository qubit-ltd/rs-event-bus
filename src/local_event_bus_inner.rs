/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Shared state for the local event bus.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use crate::erased_subscription::ErasedSubscription;
use crate::publisher_interceptor_entry::PublisherInterceptorEntry;
use crate::{EventBusError, EventBusResult, SubscribeOptions, TopicKey};

/// Shared mutable state for [`crate::LocalEventBus`].
pub(crate) struct LocalEventBusInner {
    started: AtomicBool,
    subscriptions: Mutex<HashMap<TopicKey, Vec<Arc<dyn ErasedSubscription>>>>,
    publisher_interceptors: Mutex<Vec<Arc<dyn PublisherInterceptorEntry>>>,
    processing_tracker: ProcessingTracker,
    next_subscription_id: AtomicUsize,
    default_subscribe_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl LocalEventBusInner {
    /// Creates shared local event bus state.
    ///
    /// # Parameters
    /// - `default_subscribe_options`: Typed default subscription options.
    ///
    /// # Returns
    /// Shared state initialized in the stopped lifecycle state.
    pub(crate) fn new(
        default_subscribe_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    ) -> Self {
        Self {
            started: AtomicBool::new(false),
            subscriptions: Mutex::new(HashMap::new()),
            publisher_interceptors: Mutex::new(Vec::new()),
            processing_tracker: ProcessingTracker::new(),
            next_subscription_id: AtomicUsize::new(1),
            default_subscribe_options,
        }
    }

    /// Marks the bus as started.
    ///
    /// # Returns
    /// `true` when this call changed state from stopped to started.
    pub(crate) fn mark_started(&self) -> bool {
        self.started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Marks the bus as stopped.
    ///
    /// # Returns
    /// `true` when this call changed state from started to stopped.
    pub(crate) fn mark_stopped(&self) -> bool {
        self.started
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Returns whether the bus is currently started.
    ///
    /// # Returns
    /// `true` if publishing and subscribing are allowed.
    pub(crate) fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Allocates a new subscription ID.
    ///
    /// # Returns
    /// Process-local subscription ID.
    pub(crate) fn next_subscription_id(&self) -> usize {
        self.next_subscription_id.fetch_add(1, Ordering::SeqCst)
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

    /// Adds a publisher interceptor.
    ///
    /// # Parameters
    /// - `interceptor`: Type-erased interceptor entry.
    ///
    /// # Returns
    /// `Ok(())` when the entry is stored.
    pub(crate) fn add_publisher_interceptor(
        &self,
        interceptor: Arc<dyn PublisherInterceptorEntry>,
    ) -> EventBusResult<()> {
        self.publisher_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("publisher_interceptors"))?
            .push(interceptor);
        Ok(())
    }

    /// Returns registered publisher interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn publisher_interceptors(
        &self,
    ) -> EventBusResult<Vec<Arc<dyn PublisherInterceptorEntry>>> {
        Ok(self
            .publisher_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("publisher_interceptors"))?
            .clone())
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
        let entries = subscriptions.entry(topic_key).or_default();
        entries.push(subscription);
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.priority()));
        Ok(())
    }

    /// Returns subscriptions for a topic key.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to look up.
    ///
    /// # Returns
    /// A cloned list of subscription entries.
    pub(crate) fn subscriptions_for(
        &self,
        topic_key: &TopicKey,
    ) -> EventBusResult<Vec<Arc<dyn ErasedSubscription>>> {
        Ok(self
            .subscriptions
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriptions"))?
            .get(topic_key)
            .cloned()
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
        if let Some(entries) = subscriptions.get_mut(topic_key) {
            entries.retain(|entry| entry.id() != id);
            if entries.is_empty() {
                subscriptions.remove(topic_key);
            }
        }
        Ok(())
    }

    /// Clears all subscriptions.
    pub(crate) fn clear_subscriptions(&self) {
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
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

    /// Waits until all topics have zero active work.
    ///
    /// # Returns
    /// `Ok(())` once all tracked topics are idle.
    pub(crate) fn wait_for_all_idle(&self) -> EventBusResult<()> {
        self.processing_tracker.wait_for_all_idle()
    }
}

/// Tracks active handler work per topic.
struct ProcessingTracker {
    counts: Mutex<HashMap<TopicKey, usize>>,
    condvar: Condvar,
}

impl ProcessingTracker {
    /// Creates an empty processing tracker.
    ///
    /// # Returns
    /// Tracker with zero active work.
    fn new() -> Self {
        Self {
            counts: Mutex::new(HashMap::new()),
            condvar: Condvar::new(),
        }
    }

    /// Increments active work for a topic.
    ///
    /// # Parameters
    /// - `topic_key`: Topic receiving new handler work.
    ///
    /// # Returns
    /// `Ok(())` after incrementing the count.
    fn start(&self, topic_key: &TopicKey) -> EventBusResult<()> {
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        *counts.entry(topic_key.clone()).or_insert(0) += 1;
        Ok(())
    }

    /// Decrements active work for a topic.
    ///
    /// # Parameters
    /// - `topic_key`: Topic whose handler work finished.
    fn finish(&self, topic_key: &TopicKey) {
        if let Ok(mut counts) = self.counts.lock() {
            if let Some(count) = counts.get_mut(topic_key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(topic_key);
                }
            }
            self.condvar.notify_all();
        }
    }

    /// Waits until a topic has zero active work.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to wait for.
    ///
    /// # Returns
    /// `Ok(())` once the topic is idle.
    fn wait_for_idle(&self, topic_key: &TopicKey) -> EventBusResult<()> {
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        while counts.get(topic_key).copied().unwrap_or(0) > 0 {
            counts = self
                .condvar
                .wait(counts)
                .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        }
        Ok(())
    }

    /// Waits until all topics have zero active work.
    ///
    /// # Returns
    /// `Ok(())` once all tracked topics are idle.
    fn wait_for_all_idle(&self) -> EventBusResult<()> {
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        while !counts.is_empty() {
            counts = self
                .condvar
                .wait(counts)
                .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        }
        Ok(())
    }
}

/// Exercises lock-poisoning branches for coverage-oriented tests.
///
/// # Returns
/// Diagnostic strings collected from the covered error paths.
pub(crate) fn coverage_exercise_inner_poison_paths() -> Vec<String> {
    let mut diagnostics = Vec::new();
    let topic_key = TopicKey::new("coverage.inner".to_string(), TypeId::of::<String>());

    let mut defaults = HashMap::new();
    defaults.insert(
        TypeId::of::<String>(),
        Arc::new(SubscribeOptions::<String>::empty()) as Arc<dyn Any + Send + Sync>,
    );
    let defaults_inner = LocalEventBusInner::new(defaults);
    diagnostics.push(
        defaults_inner
            .default_subscribe_options::<u32>()
            .is_none()
            .to_string(),
    );
    diagnostics.push(coverage_error_message(Ok(()), "coverage ok branch"));
    diagnostics.push(
        CoverageInnerPublisherInterceptor
            .payload_type_id()
            .eq(&TypeId::of::<String>())
            .to_string(),
    );
    diagnostics.push(
        CoverageInnerPublisherInterceptor
            .intercept(Box::new("coverage".to_string()))
            .expect("coverage interceptor should pass through")
            .is_some()
            .to_string(),
    );
    diagnostics.push(ErasedSubscription::id(&CoverageInnerSubscription).to_string());
    diagnostics.push(ErasedSubscription::priority(&CoverageInnerSubscription).to_string());
    diagnostics.push(
        ErasedSubscription::dispatch(
            &CoverageInnerSubscription,
            Box::new("coverage".to_string()),
            Arc::new(LocalEventBusInner::new(HashMap::new())),
        )
        .is_ok()
        .to_string(),
    );

    let interceptor_inner = LocalEventBusInner::new(HashMap::new());
    coverage_poison_mutex(&interceptor_inner.publisher_interceptors);
    diagnostics.push(coverage_error_message(
        interceptor_inner.add_publisher_interceptor(Arc::new(CoverageInnerPublisherInterceptor)),
        "poisoned interceptor lock should reject writes",
    ));
    diagnostics.push(coverage_error_message(
        interceptor_inner.publisher_interceptors(),
        "poisoned interceptor lock should reject reads",
    ));

    let subscription_inner = LocalEventBusInner::new(HashMap::new());
    coverage_poison_mutex(&subscription_inner.subscriptions);
    diagnostics.push(coverage_error_message(
        subscription_inner.add_subscription(topic_key.clone(), Arc::new(CoverageInnerSubscription)),
        "poisoned subscription lock should reject writes",
    ));
    diagnostics.push(coverage_error_message(
        subscription_inner.subscriptions_for(&topic_key),
        "poisoned subscription lock should reject reads",
    ));
    diagnostics.push(coverage_error_message(
        subscription_inner.unsubscribe(&topic_key, 1),
        "poisoned subscription lock should reject removals",
    ));
    subscription_inner.clear_subscriptions();

    let tracker_inner = LocalEventBusInner::new(HashMap::new());
    coverage_poison_mutex(&tracker_inner.processing_tracker.counts);
    diagnostics.push(
        tracker_inner
            .start_processing(&topic_key)
            .expect_err("poisoned processing lock should reject start")
            .to_string(),
    );
    tracker_inner.finish_processing(&topic_key);
    diagnostics.push(
        tracker_inner
            .wait_for_idle(&topic_key)
            .expect_err("poisoned processing lock should reject topic wait")
            .to_string(),
    );
    diagnostics.push(
        tracker_inner
            .wait_for_all_idle()
            .expect_err("poisoned processing lock should reject global wait")
            .to_string(),
    );
    diagnostics.push(coverage_wait_for_idle_poison_message());
    diagnostics.push(coverage_wait_for_all_idle_poison_message());

    diagnostics
}

/// Converts an expected error result into its diagnostic message.
fn coverage_error_message<T>(result: EventBusResult<T>, message: &str) -> String {
    match result {
        Ok(_) => message.to_string(),
        Err(error) => error.to_string(),
    }
}

/// Poisons a mutex while suppressing the expected panic output.
fn coverage_poison_mutex<T>(mutex: &Mutex<T>) {
    let previous_hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        let _guard = mutex.lock().expect("coverage lock should be available");
        panic!("coverage poison mutex");
    }));
    panic::set_hook(previous_hook);
}

/// Exercises a poisoned lock returned from `Condvar::wait` for one topic.
fn coverage_wait_for_idle_poison_message() -> String {
    let tracker = Arc::new(ProcessingTracker::new());
    let topic_key = TopicKey::new("coverage.wait-for-idle".to_string(), TypeId::of::<String>());
    tracker
        .start(&topic_key)
        .expect("coverage tracker should start");
    let waiting_tracker = Arc::clone(&tracker);
    let waiting_topic_key = topic_key.clone();
    let waiter = std::thread::spawn(move || {
        waiting_tracker
            .wait_for_idle(&waiting_topic_key)
            .expect_err("poisoned condvar wait should fail")
            .to_string()
    });
    std::thread::sleep(std::time::Duration::from_millis(20));
    coverage_poison_mutex(&tracker.counts);
    tracker.condvar.notify_all();
    waiter.join().expect("coverage waiter should finish")
}

/// Exercises a poisoned lock returned from `Condvar::wait` for all topics.
fn coverage_wait_for_all_idle_poison_message() -> String {
    let tracker = Arc::new(ProcessingTracker::new());
    let topic_key = TopicKey::new(
        "coverage.wait-for-all-idle".to_string(),
        TypeId::of::<String>(),
    );
    tracker
        .start(&topic_key)
        .expect("coverage tracker should start");
    let waiting_tracker = Arc::clone(&tracker);
    let waiter = std::thread::spawn(move || {
        waiting_tracker
            .wait_for_all_idle()
            .expect_err("poisoned condvar wait should fail")
            .to_string()
    });
    std::thread::sleep(std::time::Duration::from_millis(20));
    coverage_poison_mutex(&tracker.counts);
    tracker.condvar.notify_all();
    waiter.join().expect("coverage waiter should finish")
}

/// Publisher interceptor used for poisoned-lock coverage paths.
struct CoverageInnerPublisherInterceptor;

impl PublisherInterceptorEntry for CoverageInnerPublisherInterceptor {
    /// Returns a fixed coverage payload type.
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<String>()
    }

    /// Passes the erased envelope through unchanged.
    fn intercept(
        &self,
        envelope: Box<dyn Any + Send>,
    ) -> EventBusResult<Option<Box<dyn Any + Send>>> {
        Ok(Some(envelope))
    }
}

/// Subscription used for poisoned-lock coverage paths.
struct CoverageInnerSubscription;

impl ErasedSubscription for CoverageInnerSubscription {
    /// Returns a fixed coverage subscription ID.
    fn id(&self) -> usize {
        1
    }

    /// Returns neutral priority.
    fn priority(&self) -> i32 {
        0
    }

    /// Accepts the erased envelope without work.
    fn dispatch(
        &self,
        _envelope: Box<dyn Any + Send>,
        _bus: Arc<LocalEventBusInner>,
    ) -> EventBusResult<()> {
        Ok(())
    }
}
