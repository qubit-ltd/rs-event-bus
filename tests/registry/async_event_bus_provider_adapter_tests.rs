// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Behavior tests for asynchronous provider adaptation.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use qubit_event_bus::AsyncEventBusRegistry;
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
fn test_async_provider_registry_attaches_successful_provider_identity() {
    let registry = AsyncEventBusRegistry::with_local().expect("local provider registers");
    let bus = block_on(registry.create(&EventBusConfig::default()))
        .expect("local async provider creates");
    let receipt = block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("adapter.async").expect("valid topic"),
                "payload".to_owned(),
            )
            .expect("valid publish request"),
        ),
    )
    .expect("local provider accepts publication");

    assert_eq!("local", receipt.provider_id().as_str());
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("event bus shuts down");
}

#[test]
fn test_async_provider_registry_rejects_missing_capabilities_during_creation() {
    let registry = AsyncEventBusRegistry::with_local().expect("local provider registers");
    let config = EventBusConfig::default().with_required_capabilities(
        RequiredCapabilities::new().with_durability(DurabilityCapability::Durable),
    );

    let error = match block_on(registry.create(&config)) {
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

/// Drives a runtime-neutral future until provider work completes.
fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(thread::Thread);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}
