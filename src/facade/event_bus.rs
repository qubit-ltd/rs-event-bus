// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Synchronous type-safe event-bus facade over an object-safe provider SPI.

use std::sync::Arc;

pub(in crate::facade) use internal::CoordinatorMessage;
pub(in crate::facade) use internal::EventBusInner;

use self::internal::OperationGate;
use self::internal::OwnerSettlementRouter;
use self::internal::ShutdownState;
use self::internal::SubscriptionWorkerBudget;

// Implements local-provider construction and facade configuration.
mod construction;
// Decodes messages and runs subscriber handlers.
mod delivery;
// Registers and emits bus diagnostics.
mod diagnostics;
// Applies terminal delivery failure actions.
mod failure;
// Owns shared provider, operation, and worker state.
mod internal;
// Implements wait and shutdown lifecycle operations.
mod lifecycle;
// Implements publish operations.
mod publishing;
// Implements subscription operations.
mod subscribing;
// Owns and runs provider coordinator workers.
mod worker;
#[cfg(test)]
pub(in crate::facade) use subscribing::cleanup_failed_worker_spawn;

/// A cloneable synchronous facade with one provider coordinator per
/// subscription and a shared bounded handler pool.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::local::LocalEventBusConfig;
/// use qubit_event_bus::model::PublishRequest;
/// use qubit_event_bus::model::SubscribeRequest;
/// use qubit_event_bus::model::Topic;
/// use qubit_event_bus::DeliveryError;
/// use qubit_event_bus::EventBus;
///
/// let bus = EventBus::local(LocalEventBusConfig::default())?;
/// let topic = Topic::<String>::new("orders.created")?;
/// let request = SubscribeRequest::new("audit", topic.clone())?;
/// let subscription = bus.subscribe(request, |_| Ok::<(), DeliveryError>(()))?;
/// let receipt = bus.publish(PublishRequest::new(topic.clone(), "order-1".to_owned())?)?;
/// assert_eq!(receipt.provider_id().as_str(), "local");
/// bus.wait_for_idle(&topic, None)?;
/// subscription.cancel()?;
/// bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct EventBus {
    /// Shared provider, subscription, scheduler, and lifecycle state.
    pub(super) inner: Arc<EventBusInner>,
}

#[cfg(test)]
#[path = "../../tests/support/spawn_failure_tests.rs"]
mod spawn_failure_tests;
