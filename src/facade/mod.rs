// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Type-safe synchronous and asynchronous event bus facades.

mod async_event_bus;
mod async_subscription;
mod event_bus;
mod event_bus_facade_config;
mod lifecycle;
mod subscription;
mod sync_delivery_scheduler;
mod tracker;

pub use async_event_bus::AsyncEventBus;
pub use async_subscription::AsyncSubscription;
pub use event_bus::DiagnosticObserverHandle;
pub use event_bus::EventBus;
pub use event_bus::IntoHandlerResult;
pub use event_bus_facade_config::DeliveryAdmissionConfig;
pub use event_bus_facade_config::EventBusFacadeConfig;
pub use event_bus_facade_config::SyncDeliverySchedulerConfig;
pub(crate) use lifecycle::BusContextGuard;
pub(crate) use lifecycle::LifecycleState;
pub use lifecycle::WaitOutcome;
pub(crate) use lifecycle::is_current_bus_context;
pub(crate) use lifecycle::receive_poll_interval;
pub use subscription::Subscription;
pub(crate) use subscription::SubscriptionControl;
pub(crate) use tracker::DeliveryTrackerGuard;
pub(crate) use tracker::LifecycleTracker;
