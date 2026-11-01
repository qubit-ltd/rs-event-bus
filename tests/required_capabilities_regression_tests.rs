// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Regression tests for minimum ordering-capability matching.

use std::future::Future;
use std::io::Error;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::EventBusSpec;
use qubit_event_bus::RequiredCapabilities;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_spi::FallbackPolicy;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderId as SpiProviderId;
use qubit_spi::ProviderMetadata;
use qubit_spi::ProviderSelection;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;

struct OrderingProvider {
    id: &'static str,
    ordering: OrderingCapability,
}

impl ProviderMetadata for OrderingProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(SpiProviderId::new(self.id).expect("test provider ID is valid"))
    }
}

impl ServiceProvider<EventBusSpec> for OrderingProvider {
    fn create_configured(
        &self,
        _: &EventBusConfig,
    ) -> Result<Arc<dyn EventBusSpi>, ProviderFailure<EventBusProviderError>> {
        Ok(Arc::new(OrderingSpi {
            ordering: self.ordering,
        }))
    }
}

struct OrderingSpi {
    ordering: OrderingCapability,
}

impl EventBusSpi for OrderingSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        capabilities(self.ordering)
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Err(unused_spi_error("subscribe"))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

fn capabilities(ordering: OrderingCapability) -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::AcceptOnly,
        ordering,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        SubscriptionModes::EPHEMERAL,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    )
}

fn unused_spi_error(operation: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: "ordering-test".into(),
        operation,
        resource: None,
        kind: "unused",
        retryable: Some(false),
        source: Box::new(Error::other("unused by ordering capability test")),
    }
}

#[test]
fn test_explicit_none_ordering_allows_local_provider() {
    let registry = EventBusRegistry::with_local().expect("local provider registers");
    let config = EventBusConfig::default()
        .with_required_capabilities(RequiredCapabilities::new().with_ordering(OrderingCapability::None));

    let bus = registry.create(&config).expect("None ordering imposes no minimum");
    let _report = bus.shutdown(ShutdownMode::Immediate).expect("local bus shuts down");
}

#[test]
fn test_async_explicit_none_ordering_allows_local_provider() {
    block_on(async {
        let registry = AsyncEventBusRegistry::with_local().expect("local provider registers");
        let config = EventBusConfig::default()
            .with_required_capabilities(RequiredCapabilities::new().with_ordering(OrderingCapability::None));

        let bus = registry
            .create(&config)
            .await
            .expect("None ordering imposes no minimum");
        let _report = bus
            .shutdown(ShutdownMode::Immediate)
            .await
            .expect("local async bus shuts down");
    });
}

#[test]
fn test_ordering_requirement_matrix_preserves_existing_capability_relationships() {
    for actual in [
        OrderingCapability::None,
        OrderingCapability::PerSubscription,
        OrderingCapability::PerKey,
        OrderingCapability::PerPartition,
    ] {
        let registry = EventBusRegistry::new();
        registry
            .register(OrderingProvider {
                id: "matrix",
                ordering: actual,
            })
            .expect("provider registers");
        let selection = ProviderSelection::named("matrix").expect("selection is valid");

        for (required, accepted) in [
            (None, true),
            (Some(OrderingCapability::None), true),
            (Some(OrderingCapability::PerKey), actual.supports_per_key()),
            (
                Some(OrderingCapability::PerSubscription),
                actual == OrderingCapability::PerSubscription,
            ),
            (
                Some(OrderingCapability::PerPartition),
                actual == OrderingCapability::PerPartition,
            ),
        ] {
            let mut config = EventBusConfig::default();
            if let Some(required) = required {
                config = config.with_required_capabilities(RequiredCapabilities::new().with_ordering(required));
            }
            let result = registry.create_selected(&selection, &config);
            assert_eq!(accepted, result.is_ok(), "actual {actual:?}, required {required:?}");
            if let Ok(bus) = result {
                let _report = bus.shutdown(ShutdownMode::Immediate).expect("test bus shuts down");
            }
        }
    }
}

#[test]
fn test_fallback_keeps_candidate_that_satisfies_per_key_requirement() {
    let registry = EventBusRegistry::new();
    registry
        .register(OrderingProvider {
            id: "partition-only",
            ordering: OrderingCapability::PerPartition,
        })
        .expect("first provider registers");
    registry
        .register(OrderingProvider {
            id: "per-subscription",
            ordering: OrderingCapability::PerSubscription,
        })
        .expect("second provider registers");
    let selection = ProviderSelection::chain(["partition-only", "per-subscription"])
        .expect("selection is valid")
        .with_fallback_policy(FallbackPolicy::OnAbsence);
    let config = EventBusConfig::default()
        .with_required_capabilities(RequiredCapabilities::new().with_ordering(OrderingCapability::PerKey));

    let bus = registry
        .create_selected(&selection, &config)
        .expect("fallback selects a provider satisfying per-key ordering");
    assert_eq!("per-subscription", bus.provider_id().as_str());
    let _report = bus.shutdown(ShutdownMode::Immediate).expect("selected bus shuts down");
}

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
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        thread::park();
    }
}
