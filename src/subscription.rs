/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Subscription handle.

use std::sync::atomic::{
    AtomicBool,
    Ordering,
};
use std::sync::{
    Arc,
    Weak,
};

use crate::local_event_bus_inner::LocalEventBusInner;
use crate::{
    EventBusResult,
    SubscribeOptions,
    Topic,
    TopicKey,
};

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
    pub(crate) active: Arc<AtomicBool>,
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
        self.active.load(Ordering::SeqCst)
    }

    /// Cancels this subscription.
    ///
    /// # Returns
    /// `Ok(())` when the subscription is cancelled or was already inactive.
    pub fn cancel(&self) -> EventBusResult<()> {
        if self.active.swap(false, Ordering::SeqCst)
            && let Some(bus) = self.bus.upgrade()
        {
            bus.unsubscribe(&self.topic_key, self.id)?;
        }
        Ok(())
    }
}
