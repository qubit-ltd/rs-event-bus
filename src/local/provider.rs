// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! `qubit-spi` provider definition for the built-in local backend.

use std::sync::Arc;

use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderId as SpiProviderId;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;

use super::LocalEventBusConfig;
use super::spi::LocalEventBusSpi;
use crate::registry::EventBusConfig;
use crate::registry::EventBusProviderError;
use crate::registry::EventBusSpec;
use crate::spi::EventBusSpi;

/// Built-in synchronous in-process event-bus provider.
///
/// # Examples
///
/// Register the built-in provider through the public registry rather than
/// constructing its internal SPI directly:
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::registry::{EventBusConfig, EventBusRegistry};
///
/// let registry = EventBusRegistry::with_local()?;
/// let bus = registry.create(&EventBusConfig::default())?;
/// let _bus = bus;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalEventBusProvider;

impl ProviderMetadata for LocalEventBusProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(SpiProviderId::new("local").expect("static provider ID is valid"))
            .with_aliases(["memory", "in-process"])
            .expect("static provider aliases are valid")
    }
}

impl ServiceProvider<EventBusSpec> for LocalEventBusProvider {
    fn create_configured(
        &self,
        config: &EventBusConfig,
    ) -> Result<Arc<dyn EventBusSpi>, ProviderFailure<EventBusProviderError>> {
        let local = LocalEventBusConfig::from_provider_options(config)
            .map_err(|error| ProviderFailure::invalid_configuration(EventBusProviderError::provider(error)))?;
        let spi = LocalEventBusSpi::new(&local)
            .map_err(|error| ProviderFailure::invalid_configuration(EventBusProviderError::provider(error)))?;
        Ok(Arc::new(spi))
    }
}

#[cfg(test)]
mod tests {
    use qubit_spi::ProviderMetadata;
    use qubit_spi::ServiceProvider;

    use super::LocalEventBusProvider;
    use crate::registry::EventBusConfig;
    use crate::spi::EventBusSpi;
    use crate::spi::PayloadModes;
    use crate::spi::ShutdownMode;

    #[test]
    fn test_default_configuration_creates_a_native_local_spi() {
        assert_eq!("local", LocalEventBusProvider.descriptor().id().as_str());
        let spi: std::sync::Arc<dyn EventBusSpi> = LocalEventBusProvider
            .create_configured(&EventBusConfig::default())
            .expect("default provider config is valid");

        assert_eq!(PayloadModes::Native, spi.capabilities().payload_modes());
        spi.shutdown(ShutdownMode::Immediate).expect("local SPI closes");
    }
}
