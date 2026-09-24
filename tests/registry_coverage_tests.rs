// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public-contract coverage for provider capability validation and async SPI
//! delegation.

use std::error::Error;
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
use std::time::Duration;

use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::EventBusSpec;
use qubit_event_bus::ProviderError;
use qubit_event_bus::RequiredCapabilities;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::SettlementToken;
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
use qubit_spi::error::ProviderFailure;

struct TestProvider {
    id: &'static str,
    aliases: &'static [&'static str],
    calls: Arc<Counters>,
    capabilities: EventBusCapabilities,
}

#[derive(Default)]
struct Counters {
    creates: AtomicUsize,
    capabilities: AtomicUsize,
    publish: AtomicUsize,
    subscribe: AtomicUsize,
    close: AtomicUsize,
    shutdown: AtomicUsize,
}

impl ProviderMetadata for TestProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(SpiProviderId::new(self.id).expect("valid provider ID"))
            .with_aliases(self.aliases.iter().copied())
            .expect("valid aliases")
    }
}

impl AsyncServiceProvider<EventBusSpec> for TestProvider {
    fn create_configured<'a>(
        &'a self,
        _: &'a EventBusConfig,
    ) -> ProviderFuture<'a, Result<Arc<dyn AsyncEventBusSpi>, ProviderFailure<EventBusProviderError>>> {
        self.calls.creates.fetch_add(1, Ordering::SeqCst);
        let calls = self.calls.clone();
        let capabilities = self.capabilities;
        Box::pin(async move { Ok(Arc::new(TestSpi { calls, capabilities }) as Arc<dyn AsyncEventBusSpi>) })
    }
}

struct TestSpi {
    calls: Arc<Counters>,
    capabilities: EventBusCapabilities,
}

impl AsyncEventBusSpi for TestSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.calls.capabilities.fetch_add(1, Ordering::SeqCst);
        self.capabilities
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.calls.publish.fetch_add(1, Ordering::SeqCst);
        assert!(matches!(message.payload(), TransportPayload::Native(_)));
        Box::pin(async move {
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
        self.calls.subscribe.fetch_add(1, Ordering::SeqCst);
        let calls = self.calls.clone();
        Box::pin(async move { Ok(Box::new(TestSubscription { calls }) as Box<dyn AsyncEventSubscriptionSpi>) })
    }

    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.calls.shutdown.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

struct TestSubscription {
    calls: Arc<Counters>,
}

impl AsyncEventSubscriptionSpi for TestSubscription {
    fn receive<'a>(&'a mut self, _: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        Box::pin(async { Ok(ReceiveOutcome::TimedOut) })
    }

    fn settle<'a>(&'a mut self, _: &SettlementToken, _: DeliveryDisposition) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async { Ok(()) })
    }

    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        self.calls.close.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

fn capabilities() -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::AcceptOnly,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        false,
        ReplayCapability::None,
        PublishGuarantee::FireAndForget,
        PublishVisibility::Opaque,
    )
}

fn create_error(required: RequiredCapabilities) -> ProviderError {
    let registry = AsyncEventBusRegistry::new();
    registry
        .register(TestProvider {
            id: "coverage-async",
            aliases: &[],
            calls: Arc::new(Counters::default()),
            capabilities: capabilities(),
        })
        .expect("provider registration succeeds");
    block_on(registry.create_selected(
        &ProviderSelection::named("coverage-async").expect("selection is valid"),
        &EventBusConfig::default().with_required_capabilities(required),
    ))
    .map_or_else(|error| error, |_| panic!("incompatible provider must be rejected"))
}

#[test]
fn async_registry_reports_each_unsatisfied_capability_through_creation_error() {
    let error = create_error(
        RequiredCapabilities::new()
            .with_payload(PayloadModes::Encoded)
            .with_settlement(SettlementCapabilities::AcceptRetryReject)
            .with_ordering(OrderingCapability::PerKey)
            .with_delayed_delivery(DelayedDeliveryCapability::Native)
            .with_durability(DurabilityCapability::Durable)
            .with_consumer_groups(true)
            .with_replay(ReplayCapability::Timestamp)
            .with_publish_guarantee(PublishGuarantee::DurablyStored)
            .with_publish_visibility(PublishVisibility::DestinationAdmissions),
    );

    let ProviderError::Creation { source } = error else {
        panic!("capability validation is a provider-creation failure");
    };
    let display = format!("{source:#}");
    for capability in [
        "payload",
        "settlement",
        "ordering",
        "delayed_delivery",
        "durability",
        "consumer_groups",
        "replay",
        "publish_guarantee",
        "publish_visibility",
    ] {
        assert!(display.contains(capability), "missing {capability:?} in {display}");
    }
}

#[test]
fn async_registry_resolution_failure_is_distinct_from_creation_failure() {
    let error = block_on(AsyncEventBusRegistry::new().create_selected(
        &ProviderSelection::named("missing").expect("selection is valid"),
        &EventBusConfig::default(),
    ))
    .map_or_else(|error| error, |_| panic!("unknown provider cannot be resolved"));
    assert!(matches!(error, ProviderError::Resolution { .. }));
}

#[test]
fn empty_registries_resolve_their_default_selection_during_create() {
    let sync_error = EventBusRegistry::new()
        .create(&EventBusConfig::default())
        .map_or_else(|error| error, |_| panic!("empty sync registry has no default provider"));
    assert!(matches!(sync_error, ProviderError::Resolution { .. }));

    let async_error = block_on(AsyncEventBusRegistry::new().create(&EventBusConfig::default())).map_or_else(
        |error| error,
        |_| panic!("empty async registry has no default provider"),
    );
    assert!(matches!(async_error, ProviderError::Resolution { .. }));
}

#[test]
fn async_identified_spi_delegates_facade_operations_and_keeps_provider_identity() {
    let registry = AsyncEventBusRegistry::new();
    let calls = Arc::new(Counters::default());
    registry
        .register(TestProvider {
            id: "coverage-async",
            aliases: &[],
            calls: calls.clone(),
            capabilities: capabilities(),
        })
        .expect("provider registration succeeds");
    let bus = block_on(registry.create(&EventBusConfig::default())).expect("provider creates facade");

    let request = PublishRequest::new(Topic::<u32>::new("coverage.topic").unwrap(), 7).unwrap();
    let receipt = block_on(bus.publish(request)).expect("SPI accepts publication");
    assert_eq!("coverage-async", receipt.provider_id().as_str());
    assert_eq!(1, calls.publish.load(Ordering::SeqCst));

    let request = SubscribeRequest::new(
        SubscriberId::new("coverage-subscriber").unwrap(),
        Topic::<u32>::new("coverage.topic").unwrap(),
    );
    let mut subscription = block_on(bus.subscribe(request)).expect("SPI creates receiver");
    assert_eq!(1, calls.subscribe.load(Ordering::SeqCst));
    block_on(subscription.close()).expect("receiver closes");
    block_on(bus.shutdown(ShutdownMode::Immediate)).expect("SPI shuts down");

    assert!(calls.capabilities.load(Ordering::SeqCst) >= 2);
    assert_eq!(1, calls.shutdown.load(Ordering::SeqCst));
    assert_eq!(1, calls.close.load(Ordering::SeqCst));
}

#[test]
fn async_registry_uses_default_and_configured_selections_and_resolves_aliases() {
    let registry = AsyncEventBusRegistry::new();
    let calls = Arc::new(Counters::default());
    registry
        .register_shared(Arc::new(TestProvider {
            id: "coverage-async-alias-target",
            aliases: &["coverage-async-alias"],
            calls: calls.clone(),
            capabilities: capabilities(),
        }))
        .expect("shared provider registration succeeds");
    assert_eq!(
        vec!["coverage-async-alias-target"],
        registry
            .provider_ids()
            .iter()
            .map(SpiProviderId::as_str)
            .collect::<Vec<_>>()
    );
    assert_eq!(1, registry.descriptors()[0].aliases().len());
    assert_eq!(ProviderSelection::auto(), registry.default_selection());

    let alias = ProviderSelection::named("coverage-async-alias").expect("alias selector is valid");
    registry
        .set_default_selection(alias.clone())
        .expect("default selection can be replaced before sealing");
    assert_eq!(alias, registry.default_selection());
    let default_bus =
        block_on(registry.create(&EventBusConfig::default())).expect("create uses the configured default selection");
    let request = PublishRequest::new(Topic::<u32>::new("coverage.default").unwrap(), 1).unwrap();
    let receipt = block_on(default_bus.publish(request)).expect("default-selected provider publishes");
    assert_eq!("coverage-async-alias-target", receipt.provider_id().as_str());

    let config = EventBusConfig::default().with_selection(alias);
    let configured_bus = block_on(registry.create(&config)).expect("create honors config selection");
    let request = PublishRequest::new(Topic::<u32>::new("coverage.config").unwrap(), 2).unwrap();
    let receipt = block_on(configured_bus.publish(request)).expect("config-selected provider publishes");
    assert_eq!("coverage-async-alias-target", receipt.provider_id().as_str());
    assert_eq!(2, calls.creates.load(Ordering::SeqCst));

    registry.seal();
    assert!(registry.is_sealed());
    assert!(registry.set_default_selection(ProviderSelection::auto()).is_err());
}

#[test]
fn async_registry_falls_back_for_unsupported_candidates_and_reports_exhaustion() {
    let registry = AsyncEventBusRegistry::new();
    let ephemeral_calls = Arc::new(Counters::default());
    let durable_calls = Arc::new(Counters::default());
    registry
        .register(TestProvider {
            id: "coverage-ephemeral",
            aliases: &[],
            calls: ephemeral_calls.clone(),
            capabilities: capabilities(),
        })
        .expect("ephemeral provider registers");
    registry
        .register(TestProvider {
            id: "coverage-durable",
            aliases: &[],
            calls: durable_calls.clone(),
            capabilities: EventBusCapabilities::new(
                PayloadModes::Native,
                SettlementCapabilities::AcceptOnly,
                OrderingCapability::None,
                DelayedDeliveryCapability::None,
                DurabilityCapability::Durable,
                false,
                ReplayCapability::None,
                PublishGuarantee::Accepted,
                PublishVisibility::Opaque,
            ),
        })
        .expect("durable provider registers");

    let selection = ProviderSelection::chain(["coverage-ephemeral", "coverage-durable"])
        .expect("candidate selection is valid")
        .with_fallback_policy(FallbackPolicy::OnAbsence);
    let required = EventBusConfig::default().with_required_capabilities(RequiredCapabilities::new().durable());
    let bus = block_on(registry.create_selected(&selection, &required))
        .expect("unsupported first provider falls back to the capable provider");
    let request = PublishRequest::new(Topic::<u32>::new("coverage.fallback").unwrap(), 3).unwrap();
    let receipt = block_on(bus.publish(request)).expect("fallback provider publishes");
    assert_eq!("coverage-durable", receipt.provider_id().as_str());
    assert_eq!(1, ephemeral_calls.creates.load(Ordering::SeqCst));
    assert_eq!(1, durable_calls.creates.load(Ordering::SeqCst));

    let exhausted_registry = AsyncEventBusRegistry::new();
    let first = Arc::new(Counters::default());
    let second = Arc::new(Counters::default());
    exhausted_registry
        .register(TestProvider {
            id: "coverage-first-ephemeral",
            aliases: &[],
            calls: first.clone(),
            capabilities: capabilities(),
        })
        .expect("first provider registers");
    exhausted_registry
        .register(TestProvider {
            id: "coverage-second-ephemeral",
            aliases: &[],
            calls: second.clone(),
            capabilities: capabilities(),
        })
        .expect("second provider registers");
    let exhausted = ProviderSelection::chain(["coverage-first-ephemeral", "coverage-second-ephemeral"])
        .expect("candidate selection is valid")
        .with_fallback_policy(FallbackPolicy::OnAbsence);
    let error = block_on(exhausted_registry.create_selected(&exhausted, &required))
        .map_or_else(|error| error, |_| panic!("all unsupported candidates must fail"));
    assert!(matches!(error, ProviderError::Creation { .. }));
    assert_eq!(1, first.creates.load(Ordering::SeqCst));
    assert_eq!(1, second.creates.load(Ordering::SeqCst));
}

#[test]
fn sync_local_registry_supports_default_and_configured_selection() {
    let registry = EventBusRegistry::with_local().expect("built-in local provider registers");
    assert_eq!("local", registry.provider_ids()[0].as_str());
    assert_eq!(2, registry.descriptors()[0].aliases().len());
    let alias = ProviderSelection::named("memory").expect("local alias is valid");
    registry
        .set_default_selection(alias)
        .expect("default selection can be changed before sealing");

    let default_bus = registry
        .create(&EventBusConfig::default())
        .expect("registry create uses its default selection");
    let request = PublishRequest::new(Topic::<u32>::new("coverage.sync-default").unwrap(), 10).unwrap();
    let receipt = default_bus.publish(request).expect("local default provider publishes");
    assert_eq!("local", receipt.provider_id().as_str());

    let config =
        EventBusConfig::default().with_selection(ProviderSelection::named("in-process").expect("alias is valid"));
    let configured_bus = registry.create(&config).expect("config selection is honored");
    let request = PublishRequest::new(Topic::<u32>::new("coverage.sync-config").unwrap(), 11).unwrap();
    let receipt = configured_bus
        .publish(request)
        .expect("selected local provider publishes");
    assert_eq!("local", receipt.provider_id().as_str());
}

#[test]
fn provider_error_preserves_and_displays_its_cause() {
    let error = EventBusProviderError::provider(std::io::Error::other("backend init failed"));
    assert!(error.to_string().contains("backend init failed"));
    assert_eq!(
        "backend init failed",
        error.source().expect("provider cause is retained").to_string()
    );
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
