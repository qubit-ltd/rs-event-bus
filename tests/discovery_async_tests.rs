#![cfg(feature = "discovery")]

mod support;

use std::sync::Arc;

use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::EventBusSpec;
use qubit_event_bus::RequiredCapabilities;
use qubit_spi::error::{ProviderCreationError, ProviderFailure, ProviderFailureKind};
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderFuture;
use qubit_spi::ProviderId;
use qubit_spi::ProviderMetadata;
use qubit_spi::ProviderSelection;

use support::fake_spi::FakeAsyncEventBusSpi;
use support::manual_async::block_on;

struct DiscoveredAsyncProvider;

impl ProviderMetadata for DiscoveredAsyncProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(ProviderId::new("test-async-discovered").unwrap())
    }
}

impl AsyncServiceProvider<EventBusSpec> for DiscoveredAsyncProvider {
    fn create_configured<'a>(
        &'a self,
        _: &'a EventBusConfig,
    ) -> ProviderFuture<'a, Result<Arc<dyn AsyncEventBusSpi>, ProviderFailure<EventBusProviderError>>> {
        Box::pin(async { Ok(Arc::new(FakeAsyncEventBusSpi::new()) as Arc<dyn AsyncEventBusSpi>) })
    }
}

qubit_spi::submit_async_provider! {
    inventory_entry = qubit_event_bus::registry::async_provider_inventory::Entry;
    spec = EventBusSpec;
    provider = DiscoveredAsyncProvider;
}

#[test]
fn discovered_async_provider_is_creatable() {
    let registry = AsyncEventBusRegistry::discover().unwrap();
    assert!(
        registry
            .provider_ids()
            .iter()
            .any(|id| id.as_str() == "test-async-discovered")
    );
    let config = EventBusConfig::default().with_selection(ProviderSelection::named("test-async-discovered").unwrap());
    let _bus = block_on(registry.create(&config)).unwrap();
}

#[test]
fn discovered_async_provider_rejects_missing_capability() {
    let registry = AsyncEventBusRegistry::discover().unwrap();
    let config = EventBusConfig::default()
        .with_selection(ProviderSelection::named("test-async-discovered").unwrap())
        .with_required_capabilities(RequiredCapabilities::new().durable());
    let result = block_on(registry.create(&config));
    let Err(qubit_event_bus::ProviderError::Creation { source }) = result else {
        panic!("missing capability must fail during provider creation");
    };
    let creation_error = source
        .downcast_ref::<ProviderCreationError<EventBusProviderError>>()
        .expect("creation source must preserve classified provider attempts");
    assert_eq!(
        creation_error.decisive_attempt().failure().kind(),
        ProviderFailureKind::Unsupported
    );
}
