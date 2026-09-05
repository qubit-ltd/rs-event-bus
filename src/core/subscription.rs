// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscription handle.
// qubit-style: allow coverage-cfg
// qubit-style: allow multiple-public-types

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use crate::EventBusResult;
use crate::SubscribeOptions;
use crate::Topic;
use crate::TopicKey;
use crate::local::local_event_bus_inner::LocalEventBusInner;

#[cfg_attr(not(coverage), allow(dead_code))]
const MAX_DELAY_WAIT_SLICE: Duration = Duration::from_secs(60 * 60);

/// Handle returned from a successful subscription.
///
/// Dropping the handle does not automatically cancel the subscription. Call
/// [`cancel`](Self::cancel) to unsubscribe.
pub struct Subscription<T: 'static> {
    pub(crate) id: usize,
    pub(crate) subscriber_id: String,
    pub(crate) topic: Topic<T>,
    pub(crate) topic_key: TopicKey,
    pub(crate) options: SubscribeOptions<T>,
    pub(crate) active: Arc<SubscriptionState>,
    pub(crate) bus: Weak<LocalEventBusInner>,
}

impl<T: 'static> Subscription<T> {
    /// Returns subscriber ID.
    ///
    /// # Returns
    /// ID supplied when subscribing.
    pub fn subscriber_id(&self) -> &str {
        &self.subscriber_id
    }

    /// Returns subscribed topic.
    ///
    /// # Returns
    /// Type-safe topic metadata.
    pub fn topic(&self) -> &Topic<T> {
        &self.topic
    }

    /// Returns subscription options.
    ///
    /// # Returns
    /// Immutable options captured at subscription time.
    pub const fn options(&self) -> &SubscribeOptions<T> {
        &self.options
    }

    /// Returns whether the subscription is active.
    ///
    /// # Returns
    /// `true` until [`cancel`](Self::cancel) succeeds.
    pub fn is_active(&self) -> bool {
        self.active.is_active()
    }

    /// Cancels this subscription.
    ///
    /// # Returns
    /// `Ok(())` when the subscription is cancelled or was already inactive.
    pub fn cancel(&self) -> EventBusResult<()> {
        if self.active.deactivate()
            && let Some(bus) = self.bus.upgrade()
        {
            bus.unsubscribe(&self.topic_key, self.id)?;
        }
        Ok(())
    }
}

/// Shared active/cancelled state for one subscription.
pub(crate) struct SubscriptionState {
    active: AtomicBool,
    next_delay_cancellation_id: AtomicUsize,
    delay_cancellations: Mutex<HashMap<usize, Box<dyn Fn() + Send + Sync + 'static>>>,
    delay_mutex: Mutex<()>,
    delay_condvar: Condvar,
}

impl SubscriptionState {
    /// Creates active subscription state.
    ///
    /// # Returns
    /// State initialized as active.
    pub(crate) fn active() -> Self {
        Self {
            active: AtomicBool::new(true),
            next_delay_cancellation_id: AtomicUsize::new(1),
            delay_cancellations: Mutex::new(HashMap::new()),
            delay_mutex: Mutex::new(()),
            delay_condvar: Condvar::new(),
        }
    }

    /// Returns whether the subscription is active.
    ///
    /// # Returns
    /// `true` until deactivation.
    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Marks the subscription inactive and wakes pending delayed deliveries.
    ///
    /// # Returns
    /// `true` when this call changed the state.
    pub(crate) fn deactivate(&self) -> bool {
        let mut cancellations = self.delay_cancellations_guard();
        let guard = self.delay_mutex_guard();
        let was_active = self.active.swap(false, Ordering::SeqCst);
        let cancellation_callbacks = cancellations.drain().map(|(_id, cancel)| cancel).collect::<Vec<_>>();
        drop(cancellations);
        drop(guard);
        if was_active {
            for cancel in cancellation_callbacks {
                cancel();
            }
            self.delay_condvar.notify_all();
        }
        was_active
    }

    /// Registers cancellation for a delayed delivery.
    ///
    /// # Parameters
    /// - `cancel`: Callback that cancels the delayed delivery and releases its
    ///   active processing accounting.
    ///
    /// # Returns
    /// Cancellation registration ID, or `None` if the subscription is already
    /// inactive.
    pub(crate) fn register_delay_cancellation<F>(&self, cancel: F) -> Option<usize>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let mut cancellations = self.delay_cancellations_guard();
        if !self.is_active() {
            drop(cancellations);
            cancel();
            return None;
        }
        let id = self.next_delay_cancellation_id.fetch_add(1, Ordering::SeqCst);
        cancellations.insert(id, Box::new(cancel));
        Some(id)
    }

    /// Removes a delayed-delivery cancellation registration.
    ///
    /// # Parameters
    /// - `id`: Registration ID returned by
    ///   [`register_delay_cancellation`](Self::register_delay_cancellation).
    pub(crate) fn unregister_delay_cancellation(&self, id: usize) {
        let mut cancellations = self.delay_cancellations_guard();
        cancellations.remove(&id);
    }

    /// Waits until a delay elapses or the subscription becomes inactive.
    ///
    /// # Parameters
    /// - `delay`: Delay duration to wait.
    ///
    /// # Returns
    /// `true` if the delay elapsed while the subscription stayed active.
    #[cfg_attr(not(coverage), allow(dead_code))]
    pub(crate) fn wait_until_delay_elapsed_or_inactive(&self, delay: Duration) -> bool {
        if delay.is_zero() {
            return self.is_active();
        }
        let started_at = Instant::now();
        let mut guard = self.delay_mutex_guard();
        while self.is_active() {
            let Some(remaining) = delay.checked_sub(started_at.elapsed()) else {
                return self.is_active();
            };
            let wait_duration = remaining.min(MAX_DELAY_WAIT_SLICE);
            let (next_guard, timeout_result) = match self.delay_condvar.wait_timeout(guard, wait_duration) {
                Ok(result) => result,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard = next_guard;
            if timeout_result.timed_out() && remaining <= wait_duration {
                return self.is_active();
            }
        }
        false
    }

    fn delay_mutex_guard(&self) -> MutexGuard<'_, ()> {
        match self.delay_mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn delay_cancellations_guard(&self) -> MutexGuard<'_, HashMap<usize, Box<dyn Fn() + Send + Sync + 'static>>> {
        match self.delay_cancellations.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[cfg(coverage)]
    pub(crate) fn coverage_poison_delay_mutex(&self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = self.delay_mutex.lock().expect("delay mutex should lock");
            panic!("coverage poison");
        }));
    }

    #[cfg(coverage)]
    pub(crate) fn coverage_poison_delay_cancellations(&self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = self
                .delay_cancellations
                .lock()
                .expect("delay cancellations should lock");
            panic!("coverage poison");
        }));
    }
}
