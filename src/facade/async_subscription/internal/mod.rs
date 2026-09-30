// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal ownership types for the asynchronous subscription runner.

mod async_session;
mod async_subscription_control;
mod bus_context_future;
mod delivery_task_context;
mod owned_delivery_task;
mod pending_delivery;
mod session_lease;
mod session_signals;
mod session_slot;

pub(in crate::facade::async_subscription) use async_session::AsyncSession;
pub(in crate::facade) use async_subscription_control::AsyncSubscriptionControl;
pub(super) use bus_context_future::BusContextFuture;
pub(in crate::facade) use bus_context_future::is_current_bus_poll;
pub(super) use owned_delivery_task::OwnedDeliveryTask;
pub(super) use pending_delivery::PendingDelivery;
pub(in crate::facade::async_subscription) use session_lease::SessionLease;
pub(super) use session_signals::SessionSignals;
pub(in crate::facade::async_subscription) use session_slot::SessionSlot;

mod owned_delivery_lease;
mod settlement_progress;

mod handler_duration_guard;

mod settlement_attempt_guard;
