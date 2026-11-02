// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
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
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::EventBusSpec;
use qubit_event_bus::RequiredCapabilities;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
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
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::FallbackPolicy;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderFuture;
use qubit_spi::ProviderId as SpiProviderId;
use qubit_spi::ProviderMetadata;
use qubit_spi::ProviderSelection;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;

struct OrderingProvider {
    id: &'static str,
    ordering: OrderingCapability,
    subscription_modes: SubscriptionModes,
    durability: DurabilityCapability,
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
            subscription_modes: self.subscription_modes,
            durability: self.durability,
        }))
    }
}

struct AsyncOrderingProvider {
    id: &'static str,
    ordering: OrderingCapability,
    subscription_modes: SubscriptionModes,
    durability: DurabilityCapability,
}

impl ProviderMetadata for AsyncOrderingProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(SpiProviderId::new(self.id).expect("test provider ID is valid"))
    }
}

impl AsyncServiceProvider<EventBusSpec> for AsyncOrderingProvider {
    fn create_configured<'a>(
        &'a self,
        _: &'a EventBusConfig,
    ) -> ProviderFuture<'a, Result<Arc<dyn AsyncEventBusSpi>, ProviderFailure<EventBusProviderError>>> {
        let ordering = self.ordering;
        let subscription_modes = self.subscription_modes;
        let durability = self.durability;
        Box::pin(async move {
            Ok(Arc::new(AsyncOrderingSpi {
                ordering,
                subscription_modes,
                durability,
            }) as Arc<dyn AsyncEventBusSpi>)
        })
    }
}

struct AsyncOrderingSpi {
    ordering: OrderingCapability,
    subscription_modes: SubscriptionModes,
    durability: DurabilityCapability,
}

impl AsyncEventBusSpi for AsyncOrderingSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        capabilities(self.ordering, self.subscription_modes, self.durability)
    }

    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async {
            Ok(PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            })
        })
    }

    fn subscribe<'a>(
        &'a self,
        _: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async { Err(unused_spi_error("subscribe")) })
    }

    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

struct OrderingSpi {
    ordering: OrderingCapability,
    subscription_modes: SubscriptionModes,
    durability: DurabilityCapability,
}

impl EventBusSpi for OrderingSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        capabilities(self.ordering, self.subscription_modes, self.durability)
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

fn capabilities(
    ordering: OrderingCapability,
    subscription_modes: SubscriptionModes,
    durability: DurabilityCapability,
) -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::AcceptOnly,
        ordering,
        DelayedDeliveryCapability::None,
        durability,
        subscription_modes,
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
fn test_subscription_mode_requirement_matrix_and_replacement() {
    let modes = [
        SubscriptionModes::EPHEMERAL,
        SubscriptionModes::DURABLE,
        SubscriptionModes::BOTH,
    ];
    for actual in modes {
        let registry = EventBusRegistry::new();
        registry
            .register(OrderingProvider {
                id: "mode-matrix",
                ordering: OrderingCapability::None,
                subscription_modes: actual,
                durability: DurabilityCapability::Ephemeral,
            })
            .expect("provider registers");
        let selection = ProviderSelection::named("mode-matrix").expect("selection is valid");
        for required in modes {
            let config = EventBusConfig::default()
                .with_required_capabilities(RequiredCapabilities::new().with_subscription_modes(required));
            let result = registry.create_selected(&selection, &config);
            let accepted = (required == SubscriptionModes::EPHEMERAL
                && actual.supports(qubit_event_bus::model::SubscriptionDurability::Ephemeral))
                || (required == SubscriptionModes::DURABLE
                    && actual.supports(qubit_event_bus::model::SubscriptionDurability::Durable))
                || (required == SubscriptionModes::BOTH && actual == SubscriptionModes::BOTH);
            assert_eq!(accepted, result.is_ok(), "actual {actual:?}, required {required:?}");
            if let Ok(bus) = result {
                let _report = bus.shutdown(ShutdownMode::Immediate).expect("test bus shuts down");
            } else {
                match result {
                    Err(error) => assert!(format!("{error:?}").contains("subscription_modes"), "{error:?}"),
                    Ok(_) => unreachable!("matrix cell was expected to fail"),
                }
            }
        }
    }

    let registry = EventBusRegistry::new();
    registry
        .register(OrderingProvider {
            id: "mode-none",
            ordering: OrderingCapability::None,
            subscription_modes: SubscriptionModes::EPHEMERAL,
            durability: DurabilityCapability::Ephemeral,
        })
        .expect("provider registers");
    let selection = ProviderSelection::named("mode-none").expect("selection is valid");
    let unconstrained = registry
        .create_selected(&selection, &EventBusConfig::default())
        .expect("None imposes no subscription mode requirement");
    let _report = unconstrained
        .shutdown(ShutdownMode::Immediate)
        .expect("test bus shuts down");
    let overwritten = RequiredCapabilities::new()
        .with_subscription_modes(SubscriptionModes::DURABLE)
        .with_subscription_modes(SubscriptionModes::EPHEMERAL);
    let bus = registry
        .create_selected(
            &selection,
            &EventBusConfig::default().with_required_capabilities(overwritten),
        )
        .expect("the last subscription mode requirement replaces the first");
    let _report = bus.shutdown(ShutdownMode::Immediate).expect("test bus shuts down");
}

#[test]
fn test_async_subscription_mode_requirement_matrix() {
    block_on(async {
        let modes = [
            SubscriptionModes::EPHEMERAL,
            SubscriptionModes::DURABLE,
            SubscriptionModes::BOTH,
        ];
        for actual in modes {
            let registry = AsyncEventBusRegistry::new();
            registry
                .register(AsyncOrderingProvider {
                    id: "async-mode-matrix",
                    ordering: OrderingCapability::None,
                    subscription_modes: actual,
                    durability: DurabilityCapability::Ephemeral,
                })
                .expect("provider registers");
            let selection = ProviderSelection::named("async-mode-matrix").expect("selection is valid");
            for required in modes {
                let config = EventBusConfig::default()
                    .with_required_capabilities(RequiredCapabilities::new().with_subscription_modes(required));
                let result = registry.create_selected(&selection, &config).await;
                let accepted = match required {
                    SubscriptionModes::EPHEMERAL => {
                        actual.supports(qubit_event_bus::model::SubscriptionDurability::Ephemeral)
                    }
                    SubscriptionModes::DURABLE => {
                        actual.supports(qubit_event_bus::model::SubscriptionDurability::Durable)
                    }
                    SubscriptionModes::BOTH => actual == SubscriptionModes::BOTH,
                    _ => unreachable!(),
                };
                assert_eq!(accepted, result.is_ok(), "actual {actual:?}, required {required:?}");
                if let Ok(bus) = result {
                    let _report = bus
                        .shutdown(ShutdownMode::Immediate)
                        .await
                        .expect("test bus shuts down");
                } else {
                    match result {
                        Err(error) => assert!(format!("{error:?}").contains("subscription_modes"), "{error:?}"),
                        Ok(_) => unreachable!("matrix cell was expected to fail"),
                    }
                }
            }
        }
    });
}

#[test]
fn test_subscription_mode_requirement_is_independent_of_durability() {
    let registry = EventBusRegistry::new();
    registry
        .register(OrderingProvider {
            id: "retention-mode",
            ordering: OrderingCapability::None,
            subscription_modes: SubscriptionModes::EPHEMERAL,
            durability: DurabilityCapability::Durable,
        })
        .expect("provider registers");
    let selection = ProviderSelection::named("retention-mode").expect("selection is valid");
    let retained = registry
        .create_selected(
            &selection,
            &EventBusConfig::default().with_required_capabilities(RequiredCapabilities::new().durable()),
        )
        .expect("durability requirement remains independent of subscription modes");
    let _report = retained.shutdown(ShutdownMode::Immediate).expect("test bus shuts down");
    let required_mode = registry.create_selected(
        &selection,
        &EventBusConfig::default().with_required_capabilities(
            RequiredCapabilities::new()
                .durable()
                .with_subscription_modes(SubscriptionModes::DURABLE),
        ),
    );
    match required_mode {
        Err(error) => assert!(format!("{error:?}").contains("subscription_modes"), "{error:?}"),
        Ok(_) => unreachable!("durable mode requirement was expected to fail"),
    }
}

#[test]
fn test_fallback_skips_candidate_missing_subscription_mode() {
    let registry = EventBusRegistry::new();
    for (id, subscription_modes) in [
        ("ephemeral-first", SubscriptionModes::EPHEMERAL),
        ("durable-second", SubscriptionModes::DURABLE),
    ] {
        registry
            .register(OrderingProvider {
                id,
                ordering: OrderingCapability::None,
                subscription_modes,
                durability: DurabilityCapability::Ephemeral,
            })
            .expect("provider registers");
    }
    let selection = ProviderSelection::chain(["ephemeral-first", "durable-second"])
        .expect("selection is valid")
        .with_fallback_policy(FallbackPolicy::OnAbsence);
    let config = EventBusConfig::default()
        .with_required_capabilities(RequiredCapabilities::new().with_subscription_modes(SubscriptionModes::DURABLE));
    let bus = registry
        .create_selected(&selection, &config)
        .expect("second candidate satisfies mode requirement");
    assert_eq!("durable-second", bus.provider_id().as_str());
    let _report = bus.shutdown(ShutdownMode::Immediate).expect("test bus shuts down");
}

#[test]
fn test_async_fallback_skips_candidate_missing_subscription_mode() {
    block_on(async {
        let registry = AsyncEventBusRegistry::new();
        for (id, subscription_modes) in [
            ("async-ephemeral-first", SubscriptionModes::EPHEMERAL),
            ("async-durable-second", SubscriptionModes::DURABLE),
        ] {
            registry
                .register(AsyncOrderingProvider {
                    id,
                    ordering: OrderingCapability::None,
                    subscription_modes,
                    durability: DurabilityCapability::Ephemeral,
                })
                .expect("provider registers");
        }
        let selection = ProviderSelection::chain(["async-ephemeral-first", "async-durable-second"])
            .expect("selection is valid")
            .with_fallback_policy(FallbackPolicy::OnAbsence);
        let config = EventBusConfig::default().with_required_capabilities(
            RequiredCapabilities::new().with_subscription_modes(SubscriptionModes::DURABLE),
        );
        let bus = registry
            .create_selected(&selection, &config)
            .await
            .expect("second candidate satisfies mode requirement");
        assert_eq!("async-durable-second", bus.provider_id().as_str());
        let _report = bus
            .shutdown(ShutdownMode::Immediate)
            .await
            .expect("test bus shuts down");
    });
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
                subscription_modes: SubscriptionModes::EPHEMERAL,
                durability: DurabilityCapability::Ephemeral,
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
            subscription_modes: SubscriptionModes::EPHEMERAL,
            durability: DurabilityCapability::Ephemeral,
        })
        .expect("first provider registers");
    registry
        .register(OrderingProvider {
            id: "per-subscription",
            ordering: OrderingCapability::PerSubscription,
            subscription_modes: SubscriptionModes::EPHEMERAL,
            durability: DurabilityCapability::Ephemeral,
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

#[test]
fn test_async_fallback_keeps_candidate_that_satisfies_per_key_requirement() {
    block_on(async {
        let registry = AsyncEventBusRegistry::new();
        registry
            .register(AsyncOrderingProvider {
                id: "async-partition-only",
                ordering: OrderingCapability::PerPartition,
                subscription_modes: SubscriptionModes::EPHEMERAL,
                durability: DurabilityCapability::Ephemeral,
            })
            .expect("first async provider registers");
        registry
            .register(AsyncOrderingProvider {
                id: "async-per-subscription",
                ordering: OrderingCapability::PerSubscription,
                subscription_modes: SubscriptionModes::EPHEMERAL,
                durability: DurabilityCapability::Ephemeral,
            })
            .expect("second async provider registers");
        let selection = ProviderSelection::chain(["async-partition-only", "async-per-subscription"])
            .expect("selection is valid")
            .with_fallback_policy(FallbackPolicy::OnAbsence);
        let config = EventBusConfig::default()
            .with_required_capabilities(RequiredCapabilities::new().with_ordering(OrderingCapability::PerKey));

        let bus = registry
            .create_selected(&selection, &config)
            .await
            .expect("fallback selects an async provider satisfying per-key ordering");
        assert_eq!("async-per-subscription", bus.provider_id().as_str());
        let _report = bus
            .shutdown(ShutdownMode::Immediate)
            .await
            .expect("selected async bus shuts down");
    });
}

#[test]
fn test_public_missing_from_reports_stable_capability_labels() {
    let bus = EventBus::local(Default::default()).expect("local bus is created");
    let actual = bus.capabilities();

    assert!(RequiredCapabilities::new().missing_from(actual).is_empty());

    let weaker_capabilities = EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::AcceptOnly,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        SubscriptionModes::EPHEMERAL,
        false,
        ReplayCapability::None,
        PublishGuarantee::FireAndForget,
        PublishVisibility::Opaque,
    );
    let requirements = RequiredCapabilities::new()
        .with_payload(PayloadModes::Encoded)
        .with_settlement(SettlementCapabilities::AcceptRetryReject)
        .with_ordering(OrderingCapability::PerKey)
        .with_durability(DurabilityCapability::Durable)
        .with_publish_guarantee(PublishGuarantee::DurablyStored);

    assert_eq!(
        requirements.missing_from(weaker_capabilities),
        vec!["payload", "settlement", "ordering", "durability", "publish_guarantee"],
    );

    let _report = bus.shutdown(ShutdownMode::Immediate).expect("local bus shuts down");
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
