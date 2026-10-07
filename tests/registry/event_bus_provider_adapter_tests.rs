// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Behavior tests for synchronous provider adaptation.

use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::error::ProviderError;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::registry::EventBusProviderError;
use qubit_event_bus::registry::RequiredCapabilities;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::ShutdownMode;
use qubit_spi::error::ProviderCreationError;
use qubit_spi::error::ProviderFailureKind;

#[test]
fn test_sync_provider_registry_attaches_successful_provider_identity() {
    let registry = EventBusRegistry::with_local().expect("local provider registers");
    let bus = registry
        .create(&EventBusConfig::default())
        .expect("local provider creates");
    let topic = Topic::<String>::new("adapter.sync").expect("topic is valid");
    let request = PublishRequest::new(topic, "payload".to_owned()).expect("request is valid");
    let receipt = bus
        .publish(request)
        .expect("local provider accepts publication");

    assert_eq!("local", receipt.provider_id().as_str());
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .expect("event bus shuts down");
}

#[test]
fn test_sync_provider_registry_rejects_missing_capabilities_during_creation() {
    let registry = EventBusRegistry::with_local().expect("local provider registers");
    let config = EventBusConfig::default().with_required_capabilities(
        RequiredCapabilities::new().with_durability(DurabilityCapability::Durable),
    );

    let error = match registry.create(&config) {
        Ok(_) => panic!("ephemeral local provider does not satisfy durable retention"),
        Err(error) => error,
    };
    assert_unsupported_durability(error);
}

/// Checks that registry creation preserves unsupported-capability
/// classification.
fn assert_unsupported_durability(error: ProviderError) {
    let ProviderError::Creation { source } = error else {
        panic!("capability mismatch is a provider creation failure");
    };
    let creation = source
        .downcast_ref::<ProviderCreationError<EventBusProviderError>>()
        .expect("creation error retains provider attempt details");

    assert!(creation.is_absence());
    assert_eq!(
        ProviderFailureKind::Unsupported,
        creation.decisive_attempt().failure().kind()
    );
    assert!(matches!(
        creation.decisive_attempt().failure().error(),
        EventBusProviderError::UnsupportedCapabilities { missing }
            if missing.as_slice() == ["durability"]
    ));
}
