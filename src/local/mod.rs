// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Built-in local event bus provider.

pub use async_local_event_bus_provider::AsyncLocalEventBusProvider;
pub use async_local_event_bus_spi::AsyncLocalEventBusSpi;
pub use local_capacity_monitor::LocalCapacityMonitor;
pub use local_capacity_snapshot::LocalCapacitySnapshot;
pub use local_event_bus_config::LocalEventBusConfig;
pub use local_event_bus_provider::LocalEventBusProvider;
pub(crate) use local_event_bus_spi::LocalEventBusSpi;

mod async_local_event_bus_provider;
mod async_local_event_bus_spi;
mod async_local_event_subscription;
pub(super) mod async_signal;
mod internal;
mod local_capacity_monitor;
mod local_capacity_snapshot;
mod local_event_bus_config;
mod local_event_bus_provider;
mod local_event_bus_spi;
mod local_event_subscription;
pub(super) mod outstanding_budget;
mod state;

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;

use crate::internal::panic_payload::dispose_panic_payload;

/// Releases every event in a detached batch, even when an event destructor
/// panics. The first panic is resumed after the batch unless this runs during
/// an existing unwind, in which case it is safely disposed.
///
/// # Type Parameters
/// - `T`: event type in the detached batch.
///
/// # Parameters
/// - `events`: detached events to release.
pub(super) fn release_events<T>(events: Vec<T>) {
    let mut first_panic: Option<Box<dyn Any + Send>> = None;
    for event in events {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(event))) {
            if first_panic.is_none() {
                first_panic = Some(payload);
            } else {
                dispose_panic_payload(payload);
            }
        }
    }
    if let Some(payload) = first_panic {
        if std::thread::panicking() {
            dispose_panic_payload(payload);
        } else {
            resume_unwind(payload);
        }
    }
}
