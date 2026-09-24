// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Contract tests for event-bus provider registration and creation.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::EventBusSpec;
use qubit_event_bus::ProviderError;
use qubit_event_bus::RequiredCapabilities;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::Topic;
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
use qubit_event_bus::spi::TransportPayload;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::FallbackPolicy;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderFuture;
use qubit_spi::ProviderId as SpiProviderId;
use qubit_spi::ProviderMetadata;
use qubit_spi::ProviderSelection;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;

struct StubProvider {
    id: &'static str,
    aliases: &'static [&'static str],
    capabilities: EventBusCapabilities,
    creates: Arc<AtomicUsize>,
    publish_fails: bool,
    create_unavailable: bool,
}

impl ProviderMetadata for StubProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(SpiProviderId::new(self.id).expect("test ID is valid"))
            .with_aliases(self.aliases.iter().copied())
            .expect("test aliases are valid")
    }
}

impl ServiceProvider<EventBusSpec> for StubProvider {
    fn create_configured(
        &self,
        _: &EventBusConfig,
    ) -> Result<Arc<dyn EventBusSpi>, ProviderFailure<EventBusProviderError>> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        if self.create_unavailable {
            return Err(ProviderFailure::unavailable(EventBusProviderError::provider(
                std::io::Error::other("backend unavailable"),
            )));
        }
        Ok(Arc::new(StubSpi {
            capabilities: self.capabilities,
            publish_fails: self.publish_fails,
        }))
    }
}

struct StubSpi {
    capabilities: EventBusCapabilities,
    publish_fails: bool,
}

impl EventBusSpi for StubSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        if self.publish_fails {
            return Err(SpiError::Operation {
                provider_id: "first".into(),
                operation: "publish",
                resource: None,
                kind: "injected_failure",
                retryable: Some(false),
                source: Box::new(std::io::Error::other("publish failed")),
            });
        }
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Err(SpiError::Operation {
            provider_id: "stub".into(),
            operation: "subscribe",
            resource: None,
            kind: "unused",
            retryable: Some(false),
            source: Box::new(std::io::Error::other("unused")),
        })
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

fn capabilities(durability: DurabilityCapability) -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::AcceptOnly,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        durability,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    )
}

#[test]
fn registry_per_key_capability_accepts_per_subscription_and_rejects_per_partition() {
    for (ordering, accepted) in [
        (OrderingCapability::PerSubscription, true),
        (OrderingCapability::PerPartition, false),
    ] {
        let registry = EventBusRegistry::new();
        registry
            .register(StubProvider {
                id: "ordering-test",
                aliases: &[],
                capabilities: EventBusCapabilities::new(
                    PayloadModes::Native,
                    SettlementCapabilities::AcceptOnly,
                    ordering,
                    DelayedDeliveryCapability::None,
                    DurabilityCapability::Ephemeral,
                    false,
                    ReplayCapability::None,
                    PublishGuarantee::Accepted,
                    PublishVisibility::Opaque,
                ),
                creates: Arc::new(AtomicUsize::new(0)),
                publish_fails: false,
                create_unavailable: false,
            })
            .expect("provider registration succeeds");
        let result = registry.create_selected(
            &ProviderSelection::named("ordering-test").expect("valid selection"),
            &EventBusConfig::default()
                .with_required_capabilities(RequiredCapabilities::new().with_ordering(OrderingCapability::PerKey)),
        );
        if accepted {
            assert!(
                result.is_ok(),
                "per-subscription ordering satisfies per-key requirement"
            );
        } else {
            assert!(matches!(result, Err(ProviderError::Creation { .. })));
        }
    }
}

#[test]
fn registry_resolves_alias_and_snapshots_provider_descriptor() {
    let registry = EventBusRegistry::new();
    let creates = Arc::new(AtomicUsize::new(0));
    let creates_for_provider = creates.clone();
    registry
        .register(StubProvider {
            id: "memory",
            aliases: &["local", "in-process"],
            capabilities: capabilities(DurabilityCapability::Ephemeral),
            creates: creates_for_provider,
            publish_fails: false,
            create_unavailable: false,
        })
        .expect("provider registration succeeds");

    let descriptors = registry.descriptors();
    assert_eq!("memory", descriptors[0].id().as_str());
    assert_eq!(2, descriptors[0].aliases().len());
    assert_eq!(
        vec!["memory"],
        registry
            .provider_ids()
            .iter()
            .map(SpiProviderId::as_str)
            .collect::<Vec<_>>()
    );

    let config = EventBusConfig::default();
    let _bus = registry
        .create_selected(
            &ProviderSelection::named(" LOCAL ").expect("selector is valid"),
            &config,
        )
        .expect("alias resolves to registered provider");
    assert_eq!(1, creates.load(Ordering::SeqCst));
}

#[test]
fn registry_falls_back_when_created_spi_lacks_required_capability() {
    let registry = EventBusRegistry::new();
    let ephemeral_creates = Arc::new(AtomicUsize::new(0));
    let durable_creates = Arc::new(AtomicUsize::new(0));
    registry
        .register(StubProvider {
            id: "ephemeral",
            aliases: &[],
            capabilities: capabilities(DurabilityCapability::Ephemeral),
            creates: ephemeral_creates.clone(),
            publish_fails: false,
            create_unavailable: false,
        })
        .expect("first provider registration succeeds");
    registry
        .register(StubProvider {
            id: "durable",
            aliases: &[],
            capabilities: capabilities(DurabilityCapability::Durable),
            creates: durable_creates.clone(),
            publish_fails: false,
            create_unavailable: false,
        })
        .expect("second provider registration succeeds");

    let selection = ProviderSelection::chain(["ephemeral", "durable"])
        .expect("selection is valid")
        .with_fallback_policy(FallbackPolicy::OnAbsence);
    let config = EventBusConfig::default().with_required_capabilities(RequiredCapabilities::new().durable());
    let bus = registry
        .create_selected(&selection, &config)
        .expect("capability mismatch advances to the next provider");
    let request = PublishRequest::new(Topic::<u32>::new("test.topic").expect("topic is valid"), 42)
        .expect("event ID generation succeeds");
    let receipt = bus.publish(request).expect("selected SPI accepts publication");
    assert_eq!(1, ephemeral_creates.load(Ordering::SeqCst));
    assert_eq!(1, durable_creates.load(Ordering::SeqCst));
    assert_eq!("durable", receipt.provider_id().as_str());
}

#[test]
fn operation_failure_does_not_resolve_a_fallback_provider() {
    let registry = EventBusRegistry::new();
    let first_creates = Arc::new(AtomicUsize::new(0));
    let second_creates = Arc::new(AtomicUsize::new(0));
    registry
        .register(StubProvider {
            id: "first",
            aliases: &[],
            capabilities: capabilities(DurabilityCapability::Ephemeral),
            creates: first_creates.clone(),
            publish_fails: true,
            create_unavailable: false,
        })
        .expect("first provider registration succeeds");
    registry
        .register(StubProvider {
            id: "second",
            aliases: &[],
            capabilities: capabilities(DurabilityCapability::Ephemeral),
            creates: second_creates.clone(),
            publish_fails: false,
            create_unavailable: false,
        })
        .expect("second provider registration succeeds");

    let selection = ProviderSelection::chain(["first", "second"])
        .expect("selection is valid")
        .with_fallback_policy(FallbackPolicy::OnAbsence);
    let bus = registry
        .create_selected(&selection, &EventBusConfig::default())
        .expect("first provider creates the facade");
    let request = PublishRequest::new(Topic::<u32>::new("test.topic").expect("topic is valid"), 42)
        .expect("event ID generation succeeds");

    assert!(bus.publish(request).is_err());
    assert_eq!(1, first_creates.load(Ordering::SeqCst));
    assert_eq!(0, second_creates.load(Ordering::SeqCst));
}

#[test]
fn empty_registry_is_mutable_until_sealed() {
    let registry = EventBusRegistry::default();
    assert!(registry.provider_ids().is_empty());
    assert!(!registry.is_sealed());
    registry.seal();
    assert!(registry.is_sealed());
    let creates = Arc::new(AtomicUsize::new(0));
    assert!(
        registry
            .register(StubProvider {
                id: "late",
                aliases: &[],
                capabilities: capabilities(DurabilityCapability::Ephemeral),
                creates,
                publish_fails: false,
                create_unavailable: false,
            })
            .is_err()
    );
}

#[test]
fn unavailable_provider_falls_back_only_during_creation() {
    let registry = EventBusRegistry::new();
    let unavailable_creates = Arc::new(AtomicUsize::new(0));
    let success_creates = Arc::new(AtomicUsize::new(0));
    registry
        .register(StubProvider {
            id: "offline",
            aliases: &[],
            capabilities: capabilities(DurabilityCapability::Ephemeral),
            creates: unavailable_creates.clone(),
            publish_fails: false,
            create_unavailable: true,
        })
        .expect("unavailable provider registration succeeds");
    registry
        .register(StubProvider {
            id: "available",
            aliases: &[],
            capabilities: capabilities(DurabilityCapability::Ephemeral),
            creates: success_creates.clone(),
            publish_fails: false,
            create_unavailable: false,
        })
        .expect("successful provider registration succeeds");

    let selection = ProviderSelection::chain(["offline", "available"])
        .expect("selection is valid")
        .with_fallback_policy(FallbackPolicy::OnAbsence);
    let bus = registry
        .create_selected(&selection, &EventBusConfig::default())
        .expect("unavailable creation advances to the next provider");
    let request = PublishRequest::new(Topic::<u32>::new("test.topic").expect("topic is valid"), 42)
        .expect("event ID generation succeeds");
    let receipt = bus.publish(request).expect("fallback provider accepts publication");

    assert_eq!(1, unavailable_creates.load(Ordering::SeqCst));
    assert_eq!(1, success_creates.load(Ordering::SeqCst));
    assert_eq!("available", receipt.provider_id().as_str());
}

#[test]
fn creation_failure_is_distinct_from_provider_resolution_failure() {
    let registry = EventBusRegistry::new();
    let creates = Arc::new(AtomicUsize::new(0));
    registry
        .register(StubProvider {
            id: "ephemeral-only",
            aliases: &[],
            capabilities: capabilities(DurabilityCapability::Ephemeral),
            creates,
            publish_fails: false,
            create_unavailable: false,
        })
        .expect("provider registration succeeds");
    let selection = ProviderSelection::named("ephemeral-only").expect("selection is valid");
    let config = EventBusConfig::default().with_required_capabilities(RequiredCapabilities::new().durable());

    let error = match registry.create_selected(&selection, &config) {
        Ok(_) => panic!("unsupported provider must fail creation"),
        Err(error) => error,
    };
    assert!(matches!(error, ProviderError::Creation { .. }));
}

#[test]
fn registry_installs_configured_codec_registry_into_the_facade() {
    let registry = EventBusRegistry::new();
    let creates = Arc::new(AtomicUsize::new(0));
    registry
        .register(StubProvider {
            id: "encoded",
            aliases: &[],
            capabilities: EventBusCapabilities::new(
                PayloadModes::Encoded,
                SettlementCapabilities::AcceptOnly,
                OrderingCapability::None,
                DelayedDeliveryCapability::None,
                DurabilityCapability::Ephemeral,
                false,
                ReplayCapability::None,
                PublishGuarantee::Accepted,
                PublishVisibility::Opaque,
            ),
            creates,
            publish_fails: false,
            create_unavailable: false,
        })
        .expect("provider registration succeeds");

    let mut codecs = CodecRegistry::new();
    codecs.register::<u32>(Arc::new(U32Codec {
        content_type: ContentType::new("application/octet-stream").expect("MIME type is valid"),
    }));
    let config =
        EventBusConfig::default().with_facade_config(EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)));
    let bus = registry
        .create_selected(
            &ProviderSelection::named("encoded").expect("selector is valid"),
            &config,
        )
        .expect("encoded provider creates");
    let request = PublishRequest::new(Topic::<u32>::new("test.topic").expect("topic is valid"), 42)
        .expect("event ID generation succeeds");

    assert!(bus.publish(request).is_ok());
}

struct StubAsyncProvider {
    id: &'static str,
    aliases: &'static [&'static str],
    capabilities: EventBusCapabilities,
    creates: Arc<AtomicUsize>,
    encoded_messages: Arc<AtomicUsize>,
}

impl ProviderMetadata for StubAsyncProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(SpiProviderId::new(self.id).expect("test ID is valid"))
            .with_aliases(self.aliases.iter().copied())
            .expect("test aliases are valid")
    }
}

impl AsyncServiceProvider<EventBusSpec> for StubAsyncProvider {
    fn create_configured<'a>(
        &'a self,
        _: &'a EventBusConfig,
    ) -> ProviderFuture<'a, Result<Arc<dyn AsyncEventBusSpi>, ProviderFailure<EventBusProviderError>>> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        let capabilities = self.capabilities;
        let encoded_messages = self.encoded_messages.clone();
        Box::pin(async move {
            Ok(Arc::new(StubAsyncSpi {
                capabilities,
                encoded_messages,
            }) as Arc<dyn AsyncEventBusSpi>)
        })
    }
}

struct StubAsyncSpi {
    capabilities: EventBusCapabilities,
    encoded_messages: Arc<AtomicUsize>,
}

impl AsyncEventBusSpi for StubAsyncSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        if matches!(message.payload(), TransportPayload::Encoded(_)) {
            self.encoded_messages.fetch_add(1, Ordering::SeqCst);
        }
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
        Box::pin(async {
            Err(SpiError::Operation {
                provider_id: "async-stub".into(),
                operation: "subscribe",
                resource: None,
                kind: "unused",
                retryable: Some(false),
                source: Box::new(std::io::Error::other("unused")),
            })
        })
    }

    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

#[test]
fn async_registry_fallback_retains_the_successful_provider_identity() {
    let registry = AsyncEventBusRegistry::new();
    let ephemeral_creates = Arc::new(AtomicUsize::new(0));
    let durable_creates = Arc::new(AtomicUsize::new(0));
    registry
        .register(StubAsyncProvider {
            id: "async-ephemeral",
            aliases: &[],
            capabilities: capabilities(DurabilityCapability::Ephemeral),
            creates: ephemeral_creates.clone(),
            encoded_messages: Arc::new(AtomicUsize::new(0)),
        })
        .expect("first async provider registration succeeds");
    registry
        .register(StubAsyncProvider {
            id: "async-durable",
            aliases: &[],
            capabilities: capabilities(DurabilityCapability::Durable),
            creates: durable_creates.clone(),
            encoded_messages: Arc::new(AtomicUsize::new(0)),
        })
        .expect("second async provider registration succeeds");

    let selection = ProviderSelection::chain(["async-ephemeral", "async-durable"])
        .expect("selection is valid")
        .with_fallback_policy(FallbackPolicy::OnAbsence);
    let config = EventBusConfig::default().with_required_capabilities(RequiredCapabilities::new().durable());
    let bus = block_on(registry.create_selected(&selection, &config))
        .expect("capability mismatch advances to the next async provider");
    let request = PublishRequest::new(Topic::<u32>::new("test.topic").expect("topic is valid"), 42)
        .expect("event ID generation succeeds");
    let receipt = block_on(bus.publish(request)).expect("selected async SPI accepts publication");

    assert_eq!(1, ephemeral_creates.load(Ordering::SeqCst));
    assert_eq!(1, durable_creates.load(Ordering::SeqCst));
    assert_eq!("async-durable", receipt.provider_id().as_str());
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(thread::Thread);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
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

#[test]
fn async_registry_installs_configured_codec_registry_into_the_facade() {
    let registry = AsyncEventBusRegistry::new();
    let encoded_messages = Arc::new(AtomicUsize::new(0));
    registry
        .register(StubAsyncProvider {
            id: "async-encoded",
            aliases: &[],
            capabilities: EventBusCapabilities::new(
                PayloadModes::Encoded,
                SettlementCapabilities::AcceptOnly,
                OrderingCapability::None,
                DelayedDeliveryCapability::None,
                DurabilityCapability::Ephemeral,
                false,
                ReplayCapability::None,
                PublishGuarantee::Accepted,
                PublishVisibility::Opaque,
            ),
            creates: Arc::new(AtomicUsize::new(0)),
            encoded_messages: encoded_messages.clone(),
        })
        .expect("async provider registration succeeds");

    let mut codecs = CodecRegistry::new();
    codecs.register::<u32>(Arc::new(U32Codec {
        content_type: ContentType::new("application/octet-stream").expect("MIME type is valid"),
    }));
    let config =
        EventBusConfig::default().with_facade_config(EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs)));
    let bus = block_on(registry.create_selected(
        &ProviderSelection::named("async-encoded").expect("selector is valid"),
        &config,
    ))
    .expect("encoded async provider creates");
    let request = PublishRequest::new(Topic::<u32>::new("test.topic").expect("topic is valid"), 42)
        .expect("event ID generation succeeds");

    block_on(bus.publish(request)).expect("registered codec encodes async publication");
    assert_eq!(1, encoded_messages.load(Ordering::SeqCst));
}

struct U32Codec {
    content_type: ContentType,
}

impl EventCodec<u32> for U32Codec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &u32) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.to_be_bytes()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<u32, CodecError> {
        let array: [u8; 4] = bytes.try_into().map_err(|error| CodecError::Decode {
            source: Box::new(error),
        })?;
        Ok(u32::from_be_bytes(array))
    }
}
