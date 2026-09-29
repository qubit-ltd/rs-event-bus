// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal asynchronous facade lifecycle primitives.

pub(in crate::facade) use async_close_guard::AsyncCloseGuard;
pub(in crate::facade) use async_delivery_guard::AsyncDeliveryGuard;
pub(in crate::facade) use async_event_bus_inner::AsyncEventBusInner;
pub(in crate::facade) use async_publish_guard::AsyncPublishGuard;
pub(in crate::facade) use async_runner_guard::AsyncRunnerGuard;
pub(in crate::facade) use async_shutdown_driver::AsyncShutdownDriver;
pub(in crate::facade) use async_signal::AsyncSignal;
pub(in crate::facade) use async_subscribe_guard::AsyncSubscribeGuard;
pub(in crate::facade) use async_tracker::AsyncTracker;
pub(in crate::facade) use bus_state::BusState;
pub(in crate::facade) use catch_spi_future::catch_spi_future;
pub(in crate::facade) use shutdown_leader_guard::ShutdownLeaderGuard;
pub(in crate::facade) use shutdown_wait::ShutdownWait;
pub(in crate::facade) use signal_registration::SignalRegistration;

mod async_close_guard;
mod async_delivery_guard;
mod async_event_bus_inner;
mod async_publish_guard;
mod async_runner_guard;
mod async_shutdown_driver;
mod async_signal;
mod async_subscribe_guard;
mod async_tracker;
mod bus_state;
mod catch_spi_future;
mod shutdown_leader_guard;
mod shutdown_wait;
mod signal_registration;
mod tracker_state;
