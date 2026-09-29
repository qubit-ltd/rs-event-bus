// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Runtime-neutral asynchronous event-bus facade.

use std::sync::Arc;

pub(super) use internal::AsyncDeliveryGuard;
pub(super) use internal::AsyncEventBusInner;
pub(super) use internal::AsyncRunnerGuard;
pub(super) use internal::AsyncShutdownDriver;
pub(super) use internal::AsyncSignal;
pub(super) use internal::AsyncTracker;
pub(super) use internal::BusState;
pub(in crate::facade::async_event_bus) use internal::ShutdownLeaderGuard;
pub(in crate::facade::async_event_bus) use internal::ShutdownWait;
pub(super) use internal::SignalRegistration;
pub(in crate::facade) use internal::catch_spi_future_fn as catch_spi_future;

// Owns shared async provider and lifecycle state.
mod internal;

// Implements async facade construction and provider setup.
mod construction;
// Registers and emits bus diagnostics.
mod diagnostics;
// Implements async wait and shutdown lifecycle operations.
mod lifecycle;
// Implements async publish operations.
pub(in crate::facade) mod publishing;
// Implements async subscription creation.
mod subscribing;
// Implements waits for deliveries received by the facade.
pub(in crate::facade) mod waiting;

/// A cloneable runtime-neutral asynchronous facade over one provider SPI.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::SubscribeRequest;
/// use qubit_event_bus::model::Topic;
/// use qubit_event_bus::AsyncEventBus;
/// use qubit_event_bus::DeliveryError;
///
/// async fn consume(bus: &AsyncEventBus) -> Result<(), Box<dyn std::error::Error>> {
///     let topic = Topic::<String>::new("orders.created")?;
///     let request = SubscribeRequest::new("audit", topic)?;
///     let mut subscription = bus.subscribe(request).await?;
///     subscription.run(|delivery| async move {
///         audit(delivery.payload()).await?;
///         Ok::<(), DeliveryError>(())
///     }).await?;
///     Ok(())
/// }
///
/// async fn audit(_order: &str) -> Result<(), DeliveryError> { Ok(()) }
/// ```
///
/// The caller supplies a facade created by an async provider and chooses how
/// to drive this function. The event bus does not spawn a runtime task.
#[derive(Clone)]
pub struct AsyncEventBus {
    /// Shared provider, lifecycle, and pipeline state.
    pub(super) inner: Arc<AsyncEventBusInner>,
}
