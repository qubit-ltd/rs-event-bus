use std::sync::Arc;
use qubit_event_bus::{EventBusConfig, EventBusProviderError, EventBusSpec};
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::spi::EventBusSpi;
use qubit_spi::{ProviderDescriptor, ProviderId, ProviderMetadata, ServiceProvider};
use qubit_spi::error::ProviderFailure;

pub struct FixtureProvider;
impl ProviderMetadata for FixtureProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(ProviderId::new("fixture-provider").unwrap())
    }
}
impl ServiceProvider<EventBusSpec> for FixtureProvider {
    fn create_configured(&self, config: &EventBusConfig)
        -> Result<Arc<dyn EventBusSpi>, ProviderFailure<EventBusProviderError>> {
        LocalEventBusProvider.create_configured(config)
    }
}
qubit_spi::submit_sync_provider! {
    inventory_entry = qubit_event_bus::registry::sync_provider_inventory::Entry;
    spec = EventBusSpec;
    provider = FixtureProvider;
}
