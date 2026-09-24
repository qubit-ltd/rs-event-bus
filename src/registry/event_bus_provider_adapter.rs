// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Creation-time validator for synchronous event-bus providers.

use std::sync::Arc;

use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;

use super::EventBusConfig;
use super::EventBusProvider;
use super::EventBusProviderError;
use super::EventBusSpec;
use super::identified_event_bus_spi::IdentifiedEventBusSpi;
use crate::model::ProviderId;
use crate::spi::EventBusSpi;

/// Wraps a provider to snapshot metadata, validate capabilities and attach
/// identity.
pub(crate) struct EventBusProviderAdapter {
    /// Original provider factory whose classification is preserved on failure.
    provider: Arc<EventBusProvider>,
    /// Immutable registration metadata captured before catalog insertion.
    descriptor: ProviderDescriptor,
    /// Facade identifier corresponding to the captured canonical descriptor.
    provider_id: ProviderId,
}

impl EventBusProviderAdapter {
    /// Captures the provider descriptor once and validates its facade identity.
    pub(crate) fn new(provider: Arc<EventBusProvider>) -> Self {
        let descriptor = provider.descriptor();
        let provider_id =
            ProviderId::new(descriptor.id().as_str()).expect("qubit-spi provider IDs satisfy the facade ID invariants");
        Self {
            provider,
            descriptor,
            provider_id,
        }
    }
}

impl ProviderMetadata for EventBusProviderAdapter {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }
}

impl ServiceProvider<EventBusSpec> for EventBusProviderAdapter {
    fn create_configured(
        &self,
        config: &EventBusConfig,
    ) -> Result<Arc<dyn EventBusSpi>, ProviderFailure<EventBusProviderError>> {
        let spi = self.provider.create_configured(config)?;
        let missing = config.required_capabilities().missing_from(spi.capabilities());
        if !missing.is_empty() {
            return Err(ProviderFailure::unsupported(
                EventBusProviderError::UnsupportedCapabilities { missing },
            ));
        }
        let identified: Arc<dyn EventBusSpi> = Arc::new(IdentifiedEventBusSpi::new(self.provider_id.clone(), spi));
        Ok(identified)
    }
}
