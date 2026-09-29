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
pub use local_event_bus_config::LocalEventBusConfig;
pub use local_event_bus_provider::LocalEventBusProvider;

mod async_local_event_bus_provider;
mod async_local_event_bus_spi;
mod async_local_event_subscription;
pub(super) mod async_signal;
mod internal;
mod local_event_bus_config;
mod local_event_bus_provider;
mod local_event_bus_spi;
mod local_event_subscription;
pub(super) mod outstanding_budget;
mod state;
