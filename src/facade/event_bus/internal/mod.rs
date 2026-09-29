// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal synchronous facade ownership types.

pub(in crate::facade) use coordinator_message::CoordinatorMessage;
pub(in crate::facade) use event_bus_inner::EventBusInner;
pub(in crate::facade) use event_bus_inner::clone_spi_error;
pub(in crate::facade) use event_bus_inner::close_spi_subscription;
pub(in crate::facade) use operation_gate::OperationGate;
pub(in crate::facade) use operation_gate_state::OperationGateState;
pub(in crate::facade) use operation_permit::OperationPermit;
pub(in crate::facade) use owner_settlement_router::OwnerSettlementRouter;
pub(in crate::facade) use shutdown_state::ShutdownState;
pub(in crate::facade) use subscription_worker_budget::SubscriptionWorkerBudget;
pub(in crate::facade) use subscription_worker_permit::SubscriptionWorkerPermit;

mod coordinator_message;
mod event_bus_inner;
mod operation_gate;
mod operation_gate_state;
mod operation_permit;
mod owner_settlement_router;
mod shutdown_state;
mod subscription_worker_budget;
mod subscription_worker_permit;
