// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscription identity and its delivery lifecycle snapshot.

use qubit_id::Id;

use crate::facade::DeliveryMetricsSnapshot;
use crate::model::SubscriberId;

/// A subscription snapshot retained by its handle after closing.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::facade::DeliveryMetricsSnapshot;
/// use qubit_event_bus::facade::SubscriptionDeliveryMetricsSnapshot;
/// use qubit_event_bus::model::SubscriberId;
/// use qubit_id::Id;
///
/// let snapshot = SubscriptionDeliveryMetricsSnapshot {
///     subscription_id: Id::new(42),
///     subscriber_id: SubscriberId::new("audit").expect("valid subscriber ID"),
///     metrics: DeliveryMetricsSnapshot::default(),
/// };
/// assert_eq!(snapshot.metrics.running_handlers, 0);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionDeliveryMetricsSnapshot {
    /// Identifies the subscription within the event bus that created it.
    pub subscription_id: Id,
    /// Identifies the logical subscriber associated with this subscription.
    pub subscriber_id: SubscriberId,
    /// Captures this subscription's delivery gauges and cumulative counters
    /// at the time the snapshot was taken.
    pub metrics: DeliveryMetricsSnapshot,
}
