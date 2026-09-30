// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private state, operation admission, and receiver-owner messages for the
//! facade.

mod close_spi_subscription;
mod coordinator_message;
mod event_bus_inner;
mod operation_gate;
mod operation_gate_state;
mod operation_permit;
mod owner_settlement_router;
mod shared_spi_error;
mod shutdown_state;
mod spi_error_clone;
mod subscription_worker_budget;
mod subscription_worker_permit;

mod owned_sync_delivery;
#[cfg(test)]
mod settlement_clock_tests;

mod handler_completion_guard;
mod handler_start_rejected;
mod owner_lifecycle_guard;
mod receive_lease_guard;

pub(in crate::facade) use close_spi_subscription::close_spi_subscription;
pub(in crate::facade) use coordinator_message::CoordinatorMessage;
pub(in crate::facade) use event_bus_inner::EventBusInner;
pub(in crate::facade) use handler_completion_guard::HandlerCompletionGuard;
pub(in crate::facade) use handler_start_rejected::HandlerStartRejected;
pub(in crate::facade) use operation_gate::OperationGate;
pub(in crate::facade) use owned_sync_delivery::OwnedSyncDelivery;
pub(in crate::facade) use owner_lifecycle_guard::OwnerLifecycleGuard;
pub(in crate::facade) use owner_settlement_router::OwnerSettlementRouter;
pub(in crate::facade) use receive_lease_guard::ReceiveLeaseGuard;
pub(in crate::facade) use shutdown_state::ShutdownState;
pub(in crate::facade) use spi_error_clone::clone_spi_error;
pub(in crate::facade) use subscription_worker_budget::SubscriptionWorkerBudget;
