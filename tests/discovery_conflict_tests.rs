#![cfg(feature = "discovery")]

use std::sync::Arc;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::EventBusSpec;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::spi::EventBusSpi;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderId;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;
use qubit_spi::error::RegistryMutationError;

struct DuplicateProvider;

impl ProviderMetadata for DuplicateProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(ProviderId::new("test-duplicate-discovered").unwrap())
    }
}

impl ServiceProvider<EventBusSpec> for DuplicateProvider {
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
    provider = DuplicateProvider;
}

qubit_spi::submit_sync_provider! {
    inventory_entry = qubit_event_bus::registry::sync_provider_inventory::Entry;
    spec = EventBusSpec;
    provider = DuplicateProvider;
}

#[test]
fn duplicate_discovered_id_reports_source() {
    let error = match EventBusRegistry::discover() {
        Ok(_) => panic!("duplicate provider ID must fail"),
        Err(error) => error,
    };
    assert!(error.source_location().line() > 0);
    assert!(
        error
            .source_location()
            .file()
            .ends_with("discovery_conflict_tests.rs")
    );
    assert!(matches!(
        error.registration_error(),
        RegistryMutationError::DuplicateSelector { .. }
    ));
    assert_eq!(
        error.registration_error().selector(),
        Some("test-duplicate-discovered")
    );
}
