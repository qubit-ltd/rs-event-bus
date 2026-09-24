// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! `qubit-spi` service specification for event-bus providers.

use std::sync::Arc;

use qubit_spi::AsyncProviderDefinition;
use qubit_spi::AsyncServiceSpec;
use qubit_spi::ProviderDefinition;
use qubit_spi::ServiceSpec;
use qubit_spi::SyncServiceSpec;

use super::EventBusConfig;
use super::EventBusProviderError;
use crate::spi::AsyncEventBusSpi;
use crate::spi::EventBusSpi;

/// Type-level binding between event-bus provider configuration and SPI outputs.
///
/// The specification is shared by synchronous and asynchronous provider
/// catalogs; each catalog uses its own output capability.
///
/// # Examples
///
/// ```rust
/// use qubit_event_bus::EventBusSpec;
/// use qubit_spi::ServiceSpec;
///
/// let _: Option<<EventBusSpec as ServiceSpec>::Config> = None;
/// ```
pub struct EventBusSpec;

/// Object-safe registration contract for synchronous event-bus providers.
pub type EventBusProvider = dyn ProviderDefinition<EventBusSpec>;

/// Object-safe registration contract for asynchronous event-bus providers.
pub type AsyncEventBusProvider = dyn AsyncProviderDefinition<EventBusSpec>;

impl ServiceSpec for EventBusSpec {
    type Config = EventBusConfig;
    type Error = EventBusProviderError;
}

impl SyncServiceSpec for EventBusSpec {
    type Output = Arc<dyn EventBusSpi>;
}

impl AsyncServiceSpec for EventBusSpec {
    type Output = Arc<dyn AsyncEventBusSpi>;
}
