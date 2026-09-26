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
mod provider_inventory;
mod required_capabilities;

pub use async_event_bus_registry::AsyncEventBusRegistry;
pub use event_bus_config::EventBusConfig;
pub use event_bus_provider_error::EventBusProviderError;
pub use event_bus_registry::EventBusRegistry;
pub use event_bus_spec::AsyncEventBusProvider;
pub use event_bus_spec::EventBusProvider;
pub use event_bus_spec::EventBusSpec;
#[cfg(feature = "discovery")]
pub use provider_inventory::async_provider_inventory;
#[cfg(feature = "discovery")]
pub use provider_inventory::sync_provider_inventory;
pub use required_capabilities::RequiredCapabilities;
