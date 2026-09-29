// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

// qubit-style: allow multiple-public-types

//! Runtime-neutral asynchronous event-bus facade.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

pub(super) use internal::AsyncDeliveryGuard;
pub(super) use internal::AsyncEventBusInner;
pub(super) use internal::AsyncRunnerGuard;
pub(super) use internal::AsyncShutdownDriver;
pub(super) use internal::AsyncSignal;
pub(super) use internal::AsyncTracker;
pub(super) use internal::BusState;
pub(super) use internal::ShutdownLeaderGuard;
pub(super) use internal::ShutdownWait;
pub(super) use internal::SignalRegistration;
pub(super) use internal::catch_spi_future;
use qubit_clock::StdMonotonicClock;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_id::Id;

use super::async_admission::AsyncAdmission;
use crate::codec::resolve_codec;
use crate::error::SubscriptionCloseErrors;
use crate::error::SubscriptionCloseFailure;

mod internal;

mod construction;
mod diagnostics;
mod lifecycle;
mod publishing;
mod subscribing;
mod waiting;

/// A cloneable runtime-neutral asynchronous facade over one provider SPI.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::{SubscribeRequest, Topic};
/// use qubit_event_bus::{AsyncEventBus, DeliveryError};
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
