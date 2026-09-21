// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
//! Subscription handle.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::EventBusResult;
use crate::SubscribeOptions;
use crate::Topic;
use crate::TopicKey;
use crate::local::local_event_bus_inner::LocalEventBusInner;

/// Handle returned from a successful subscription.
///
/// Dropping the handle does not automatically cancel the subscription. Call
/// [`cancel`](Self::cancel) to unsubscribe.
///
/// ```
/// use qubit_event_bus::{LocalEventBus, Topic};
///
/// let bus = LocalEventBus::started().unwrap();
/// let topic = Topic::<String>::try_new("orders.created").unwrap();
/// let subscription = bus.subscribe("audit", &topic, |_event| ());
/// let subscription = subscription.unwrap();
/// assert_eq!(subscription.subscriber_id(), "audit");
/// subscription.cancel().unwrap();
/// ```
pub struct Subscription<T: 'static> {
    /// Internal subscription identifier.
    pub(crate) id: usize,
    /// Application-provided subscriber identifier.
    pub(crate) subscriber_id: String,
    /// Type-safe topic subscribed to by this handle.
    pub(crate) topic: Topic<T>,
    /// Type-erased key used by the local subscription map.
    pub(crate) topic_key: TopicKey,
    /// Effective options captured at registration time.
    pub(crate) options: SubscribeOptions<T>,
    /// Shared active state used by delayed deliveries.
    pub(crate) active: Arc<SubscriptionState>,
    /// Weak reference to the owning local bus.
    pub(crate) bus: Weak<LocalEventBusInner>,
}

/// Backend-independent operations available on a subscription handle.
///
/// Backends implementing [`crate::EventBus`] may return their own handle type.
/// Dropping a handle does not cancel a subscription; call [`Self::cancel`].
pub trait SubscriptionHandle<T: Clone + Send + Sync + 'static>: Send + Sync {
    /// Returns the subscriber identifier supplied at registration.
    fn subscriber_id(&self) -> &str;

    /// Returns the subscribed topic.
    fn topic(&self) -> &Topic<T>;

    /// Returns the effective subscription options.
    fn options(&self) -> &SubscribeOptions<T>;

    /// Returns whether the subscription remains active.
    fn is_active(&self) -> bool;

    /// Cancels this subscription; repeated successful calls are harmless.
    ///
    /// # Errors
    /// Returns a backend-specific error when cancellation cannot complete.
    fn cancel(&self) -> EventBusResult<()>;
}

impl<T: Clone + Send + Sync + 'static> SubscriptionHandle<T> for Subscription<T> {
    fn subscriber_id(&self) -> &str {
        Self::subscriber_id(self)
    }
    fn topic(&self) -> &Topic<T> {
        Self::topic(self)
    }
    fn options(&self) -> &SubscribeOptions<T> {
        Self::options(self)
    }
    fn is_active(&self) -> bool {
        Self::is_active(self)
    }
    fn cancel(&self) -> EventBusResult<()> {
        Self::cancel(self)
    }
}

impl<T: 'static> Subscription<T> {
    /// Returns subscriber ID.
    ///
    /// # Returns
    /// ID supplied when subscribing.
    #[must_use]
    #[inline]
    pub fn subscriber_id(&self) -> &str {
        &self.subscriber_id
    }

    /// Returns subscribed topic.
    ///
    /// # Returns
    /// Type-safe topic metadata.
    #[must_use]
    #[inline]
    pub fn topic(&self) -> &Topic<T> {
        &self.topic
    }

    /// Returns subscription options.
    ///
    /// # Returns
    /// Immutable options captured at subscription time.
    #[must_use]
    #[inline]
    pub const fn options(&self) -> &SubscribeOptions<T> {
        &self.options
    }

    /// Returns whether the subscription is active.
    ///
    /// # Returns
    /// `true` until [`cancel`](Self::cancel) succeeds.
    #[must_use]
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.is_active()
    }

    /// Cancels this subscription.
    ///
    /// # Returns
    /// `Ok(())` when the subscription is cancelled or was already inactive.
    pub fn cancel(&self) -> EventBusResult<()> {
        if !self.active.is_active() {
            return Ok(());
        }
        if let Some(bus) = self.bus.upgrade() {
            bus.unsubscribe(&self.topic_key, self.id)?;
        }
        self.active.deactivate();
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
}
