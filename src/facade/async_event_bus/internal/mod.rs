// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal asynchronous facade lifecycle primitives.

mod async_close_guard;
mod async_delivery_guard;
mod async_publish_guard;
mod async_runner_guard;
mod async_signal;
mod async_subscribe_guard;
mod async_tracker;
mod tracker_state;

pub(in crate::facade) use async_close_guard::AsyncCloseGuard;
pub(in crate::facade) use async_delivery_guard::AsyncDeliveryGuard;
pub(in crate::facade) use async_publish_guard::AsyncPublishGuard;
pub(in crate::facade) use async_runner_guard::AsyncRunnerGuard;
pub(in crate::facade) use async_signal::AsyncSignal;
pub(in crate::facade) use async_subscribe_guard::AsyncSubscribeGuard;
pub(in crate::facade) use async_tracker::AsyncTracker;
