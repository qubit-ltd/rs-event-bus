// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

// qubit-style: allow multiple-public-types
//! Synchronous type-safe event-bus facade over an object-safe provider SPI.

use std::any::Any;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

pub(in crate::facade) use internal::CoordinatorMessage;
pub(in crate::facade) use internal::EventBusInner;
use internal::OperationGate;
use internal::OwnerSettlementRouter;
use internal::ShutdownState;
use internal::SubscriptionWorkerBudget;
use qubit_id::Id;
use qubit_retry::AttemptFailure;
use qubit_retry::Retry;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryConfig;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryFallback;
use qubit_retry::RetryPolicy;

use crate::codec::resolve_codec;
use crate::error::DeliveryAttemptError;
use crate::error::SubscriptionCloseErrors;
use crate::error::SubscriptionCloseFailure;
use crate::pipeline::terminal_directive as choose_terminal_directive;

mod construction;
mod delivery;
mod diagnostics;
mod failure;
mod internal;
mod lifecycle;
mod publishing;
mod subscribing;
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
/// use qubit_event_bus::model::{PublishRequest, SubscribeRequest, Topic};
/// use qubit_event_bus::{DeliveryError, EventBus};
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
