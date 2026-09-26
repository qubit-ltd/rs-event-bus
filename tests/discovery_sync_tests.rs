// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "discovery")]

use std::sync::Arc;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::EventBusSpec;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::EventBusSpi;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderId;
use qubit_spi::ProviderMetadata;
use qubit_spi::ProviderSelection;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;

struct DiscoveredSyncProvider;

impl ProviderMetadata for DiscoveredSyncProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(ProviderId::new("test-sync-discovered").unwrap())
    }
}

impl ServiceProvider<EventBusSpec> for DiscoveredSyncProvider {
    fn create_configured(
        &self,
        config: &EventBusConfig,
    ) -> Result<Arc<dyn EventBusSpi>, ProviderFailure<EventBusProviderError>> {
        LocalEventBusProvider.create_configured(config)
    }
}

qubit_spi::submit_sync_provider! {
    inventory_entry = qubit_event_bus::registry::sync_provider_inventory::Entry;
    spec = EventBusSpec;
    provider = DiscoveredSyncProvider;
}

#[test]
fn discovered_sync_provider_is_creatable() {
    let registry = EventBusRegistry::discover().unwrap();
    assert!(registry.provider_ids().iter().any(|id| id.as_str() == "local"));
    assert!(
        registry
            .provider_ids()
            .iter()
            .any(|id| id.as_str() == "test-sync-discovered")
    );
    let config = EventBusConfig::default().with_selection(ProviderSelection::named("test-sync-discovered").unwrap());
    let bus = registry.create(&config).unwrap();
    let request = PublishRequest::new(Topic::<String>::new("discovery.test").unwrap(), "hello".to_owned()).unwrap();
    let receipt = bus.publish(request).unwrap();
    assert_eq!(receipt.provider_id().as_str(), "test-sync-discovered");
}
