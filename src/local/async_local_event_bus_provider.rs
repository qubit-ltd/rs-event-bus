// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! `qubit-spi` provider definition for the asynchronous local backend.

use std::sync::Arc;

use qubit_clock::StdTimer;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderFuture;
use qubit_spi::ProviderId;
use qubit_spi::ProviderMetadata;
use qubit_spi::error::ProviderFailure;

use super::LocalEventBusConfig;
use super::async_local_event_bus_spi::AsyncLocalEventBusSpi;
use crate::registry::EventBusConfig;
use crate::registry::EventBusProviderError;
use crate::registry::EventBusSpec;
use crate::spi::AsyncEventBusSpi;

/// Built-in runtime-neutral asynchronous in-process event-bus provider.
///
/// # Examples
///
/// ```
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::registry::AsyncEventBusRegistry;
/// use qubit_event_bus::registry::EventBusConfig;
///
/// let registry = AsyncEventBusRegistry::with_local()?;
/// let bus = registry.create(&EventBusConfig::default()).await?;
/// bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct AsyncLocalEventBusProvider;

impl ProviderMetadata for AsyncLocalEventBusProvider {
    /// Returns the stable local provider identity and its selection aliases.
    ///
    /// # Returns
    /// A descriptor for the built-in in-process provider.
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(ProviderId::new("local").expect("static provider ID is valid"))
            .with_aliases(["memory", "in-process"])
            .expect("static provider aliases are valid")
    }
}

impl AsyncServiceProvider<EventBusSpec> for AsyncLocalEventBusProvider {
    /// Builds an asynchronous local SPI using the standard timer.
    ///
    /// # Parameters
    /// - `config`: facade and provider configuration supplied by the registry.
    ///
    /// # Returns
    /// A future resolving to the constructed provider SPI.
    ///
    /// # Errors
    /// Returns an invalid-configuration provider failure when local options
    /// cannot be parsed or validated.
    fn create_configured<'a>(
        &'a self,
        config: &'a EventBusConfig,
    ) -> ProviderFuture<'a, Result<Arc<dyn AsyncEventBusSpi>, ProviderFailure<EventBusProviderError>>> {
        Box::pin(async move {
            let local = LocalEventBusConfig::from_provider_options(config)
                .map_err(|error| ProviderFailure::invalid_configuration(EventBusProviderError::provider(error)))?;
            let spi = AsyncLocalEventBusSpi::with_timer(&local, Arc::new(StdTimer::new()))
                .map_err(|error| ProviderFailure::invalid_configuration(EventBusProviderError::provider(error)))?;
            Ok(Arc::new(spi) as Arc<dyn AsyncEventBusSpi>)
        })
    }
}
