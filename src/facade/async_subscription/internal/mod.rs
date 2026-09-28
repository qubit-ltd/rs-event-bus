// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal ownership types for the asynchronous subscription runner.

mod bus_context_future;
mod delivery_task_context;
mod owned_delivery_task;
mod pending_delivery;
mod session_signals;

pub(super) use bus_context_future::BusContextFuture;
pub(in crate::facade) use bus_context_future::is_current_bus_poll;
pub(super) use delivery_task_context::DeliveryTaskContext;
pub(super) use owned_delivery_task::OwnedDeliveryTask;
pub(super) use owned_delivery_task::discard_unstarted_tasks;
pub(super) use pending_delivery::PendingDelivery;
pub(super) use session_signals::SessionSignals;
