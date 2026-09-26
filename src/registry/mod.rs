// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider selection and event bus creation.

mod async_event_bus_provider_adapter;
mod async_event_bus_registry;
mod event_bus_config;
mod event_bus_provider_adapter;
mod event_bus_provider_error;
mod event_bus_registry;
mod event_bus_spec;
mod identified_async_event_bus_spi;
mod identified_event_bus_spi;
mod required_capabilities;

#[cfg(feature = "discovery")]
qubit_spi::declare_sync_provider_inventory! {
    pub mod sync_provider_inventory { spec = crate::registry::EventBusSpec; }
}

#[cfg(feature = "discovery")]
qubit_spi::declare_async_provider_inventory! {
    pub mod async_provider_inventory { spec = crate::registry::EventBusSpec; }
}

pub use async_event_bus_registry::AsyncEventBusRegistry;
pub use event_bus_config::EventBusConfig;
pub use event_bus_provider_error::EventBusProviderError;
pub use event_bus_registry::EventBusRegistry;
pub use event_bus_spec::AsyncEventBusProvider;
pub use event_bus_spec::EventBusProvider;
pub use event_bus_spec::EventBusSpec;
pub use required_capabilities::RequiredCapabilities;
