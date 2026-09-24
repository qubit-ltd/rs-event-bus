// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Creation-time validator for asynchronous event-bus providers.

use std::sync::Arc;

use qubit_spi::AsyncServiceProvider;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderFuture;
use qubit_spi::ProviderMetadata;
use qubit_spi::error::ProviderFailure;

use super::AsyncEventBusProvider;
use super::EventBusConfig;
use super::EventBusProviderError;
use super::EventBusSpec;
use super::identified_async_event_bus_spi::IdentifiedAsyncEventBusSpi;
use crate::model::ProviderId;
use crate::spi::AsyncEventBusSpi;

/// Validates async provider capabilities and attaches snapshotted identity.
pub(crate) struct AsyncEventBusProviderAdapter {
    /// Original asynchronous provider factory.
    provider: Arc<AsyncEventBusProvider>,
    /// Immutable metadata captured before insertion in the registry.
    descriptor: ProviderDescriptor,
    /// Facade ID corresponding to the captured canonical descriptor.
    provider_id: ProviderId,
}

impl AsyncEventBusProviderAdapter {
    /// Captures the provider descriptor once and validates its facade identity.
    pub(crate) fn new(provider: Arc<AsyncEventBusProvider>) -> Self {
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

impl ProviderMetadata for AsyncEventBusProviderAdapter {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }
}

impl AsyncServiceProvider<EventBusSpec> for AsyncEventBusProviderAdapter {
    fn create_configured<'a>(
        &'a self,
        config: &'a EventBusConfig,
    ) -> ProviderFuture<'a, Result<Arc<dyn AsyncEventBusSpi>, ProviderFailure<EventBusProviderError>>> {
        Box::pin(async move {
            let spi = self.provider.create_configured(config).await?;
            let missing = config.required_capabilities().missing_from(spi.capabilities());
            if !missing.is_empty() {
                return Err(ProviderFailure::unsupported(
                    EventBusProviderError::UnsupportedCapabilities { missing },
                ));
            }
            let identified: Arc<dyn AsyncEventBusSpi> =
                Arc::new(IdentifiedAsyncEventBusSpi::new(self.provider_id.clone(), spi));
            Ok(identified)
        })
    }
}
