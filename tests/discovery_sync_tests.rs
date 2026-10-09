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
use qubit_event_bus::registry::sync_provider_inventory::Entry;
use qubit_event_bus::spi::EventBusSpi;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderId;
use qubit_spi::ProviderMetadata;
use qubit_spi::ProviderSelection;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;
use qubit_spi::submit_sync_provider;

/// Test provider submitted to sync provider inventory.
struct DiscoveredSyncProvider;

impl ProviderMetadata for DiscoveredSyncProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(ProviderId::new("test-sync-discovered").expect("valid provider ID"))
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

submit_sync_provider! {
    inventory_entry = Entry;
    spec = EventBusSpec;
    provider = DiscoveredSyncProvider;
}

#[test]
fn test_discovered_sync_provider_is_creatable() {
    let registry = EventBusRegistry::discover().expect("discover linked providers");
    let provider_ids = registry.provider_ids();
    assert!(
        provider_ids.iter().any(|id| id.as_str() == "local"),
        "the local provider should be discovered"
    );
    assert!(
        provider_ids.iter().any(|id| id.as_str() == "test-sync-discovered"),
        "the submitted sync provider should be discovered"
    );
    let selection = ProviderSelection::named("test-sync-discovered").expect("valid provider selection");
    let config = EventBusConfig::default().with_selection(selection);
    let bus = registry.create(&config).expect("create bus from discovered provider");
    let topic = Topic::<String>::new("discovery.test").expect("valid topic");
    let request = PublishRequest::new(topic, "hello".to_owned()).expect("valid publish request");
    let receipt = bus.publish(request).expect("publish through discovered provider");
    assert_eq!(
        receipt.provider_id().as_str(),
        "test-sync-discovered",
        "the publish receipt should identify the selected provider"
    );
}
