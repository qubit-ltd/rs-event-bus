// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public facade checks for publisher interceptor ordering and drop behavior.

use std::io::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_event_bus::CodecError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventEnvelope;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::PublishMetadata;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EncodedPayload;
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
use qubit_event_bus::spi::TransportPayload;
use qubit_retry::RetryPolicy;

#[test]
fn test_typed_and_global_interceptors_run_in_order_before_drop() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let typed_order = order.clone();
    let global_order = order.clone();
    let config = EventBusFacadeConfig::new().publisher_interceptor(move |metadata: &mut PublishMetadata| {
        global_order.lock().unwrap().push("global");
        assert_eq!(metadata.header("origin"), Some("typed"));
        Ok(false)
    });
    let registry = EventBusRegistry::with_local().expect("local provider registers");
    let event_config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().provider_options())
        .with_facade_config(config);
    let bus = registry
        .create(&event_config)
        .expect("local event bus accepts facade configuration");
    let options = PublishOptions::<u32>::builder()
        .interceptor(move |mut envelope| {
            typed_order.lock().unwrap().push("typed");
            envelope.set_header("origin", "typed").expect("valid header");
            Ok(Some(envelope))
        })
        .build();

    let receipt = bus
        .publish(
            PublishRequest::new(Topic::new("pipeline.publisher").unwrap(), 17_u32)
                .unwrap()
                .with_options(options),
        )
        .unwrap();

    assert!(receipt.acknowledgement().is_dropped());
    assert_eq!(*order.lock().unwrap(), ["typed", "global"]);
    assert_eq!(bus.publish_metrics().dropped, 1);
    let _ = bus.shutdown(ShutdownMode::Immediate);
}

/// A transport probe records the exact metadata supplied on every attempt.
struct WeightProbe {
    modes: PayloadModes,
    fail_first: bool,
    messages: Mutex<Vec<OutboundMessage>>,
}

impl EventBusSpi for WeightProbe {
    /// Returns a transport capability set with the selected payload modes.
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            self.modes,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    /// Records each attempt and optionally injects one definitely unaccepted
    /// failure.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let mut messages = self.messages.lock().expect("probe lock");
        messages.push(message);
        if self.fail_first && messages.len() == 1 {
            return Err(SpiError::Publish {
                provider_id: "weight-probe".into(),
                resource: None,
                kind: "transient",
                retryable: Some(true),
                effect: PublishEffect::NotAccepted,
                source: Box::new(Error::other("retry the weight probe")),
            });
        }
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    /// Subscriptions are outside the publication metadata contract.
    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        unreachable!("weight probe only publishes")
    }

    /// Completes shutdown without retained delivery work.
    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

/// Weight observes the transformed payload exactly once across automatic
/// retries.
#[test]
fn test_native_payload_weight_after_interceptors_is_retained_across_retries() {
    for modes in [PayloadModes::Native, PayloadModes::NativeAndEncoded] {
        let probe = Arc::new(WeightProbe {
            modes,
            fail_first: true,
            messages: Mutex::new(Vec::new()),
        });
        let bus = EventBus::from_spi(ProviderId::new("weight-probe").expect("valid provider"), probe.clone())
            .expect("facade starts");
        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = calls.clone();
        let options = PublishOptions::<String>::builder()
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(2)
                    .build()
                    .expect("valid retry policy"),
            )
            .interceptor(|event| {
                Ok(Some(EventEnvelope::with_id_and_shared_payload(
                    event.topic().clone(),
                    Arc::new(String::from("transformed")),
                    event.id().clone(),
                )))
            })
            .native_payload_weight(move |payload| {
                callback_calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(payload, "transformed");
                NonZeroUsize::new(payload.len()).expect("nonempty transformed payload")
            })
            .build();
        let _ = bus
            .publish(
                PublishRequest::new(
                    Topic::new("weights.retry").expect("valid topic"),
                    String::from("original"),
                )
                .expect("valid request")
                .with_options(options),
            )
            .expect("retry succeeds");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let messages = probe.messages.lock().expect("probe lock");
        assert_eq!(messages.len(), 2);
        for message in messages.iter() {
            assert_eq!(message.native_payload_weight_bytes(), NonZeroUsize::new(11));
            let TransportPayload::Native(payload) = message.payload() else {
                panic!("native payload expected")
            };
            assert_eq!(payload.downcast_ref::<String>().expect("string payload"), "transformed");
        }
        drop(messages);
        let _ = bus.shutdown(ShutdownMode::Immediate).expect("shutdown succeeds");
    }
}

/// A weight callback panic is structured and stops before transport admission.
#[test]
fn test_native_payload_weight_panic_is_preflight_failure() {
    let probe = Arc::new(WeightProbe {
        modes: PayloadModes::Native,
        fail_first: false,
        messages: Mutex::new(Vec::new()),
    });
    let bus = EventBus::from_spi(ProviderId::new("weight-probe").expect("valid provider"), probe.clone())
        .expect("facade starts");
    let options = PublishOptions::<u32>::builder()
        .native_payload_weight(|_| panic!("weight panic"))
        .build();
    let failure = bus
        .publish(
            PublishRequest::new(Topic::new("weights.panic").expect("valid topic"), 7)
                .expect("valid request")
                .with_options(options),
        )
        .expect_err("weight panic fails preflight");
    assert!(matches!(
        failure.cause(),
        PublishError::InterceptorPanicked { scope: "payload_weight", message }
            if message.as_ref() == "weight panic"
    ));
    assert_eq!(failure.effect(), PublishEffect::NotAccepted);
    assert!(probe.messages.lock().expect("probe lock").is_empty());
    let _ = bus.shutdown(ShutdownMode::Immediate).expect("shutdown succeeds");
}

/// Encoded transports skip native estimation even when the callback would
/// panic.
#[test]
fn test_native_payload_weight_is_skipped_for_encoded_transport() {
    let probe = Arc::new(WeightProbe {
        modes: PayloadModes::Encoded,
        fail_first: false,
        messages: Mutex::new(Vec::new()),
    });
    let bus = EventBus::from_spi(ProviderId::new("weight-probe").expect("valid provider"), probe.clone())
        .expect("facade starts");
    let options = PublishOptions::<u32>::builder()
        .native_payload_weight(|_| panic!("encoded provider must skip weight"))
        .build();
    let _receipt = bus
        .publish(
            PublishRequest::new(
                Topic::new("weights.encoded")
                    .expect("valid topic")
                    .with_codec(WeightCodec {
                        content_type: ContentType::new("application/octet-stream").expect("valid content type"),
                    }),
                7,
            )
            .expect("valid request")
            .with_options(options),
        )
        .expect("encoded publish succeeds without evaluating native weight");
    let messages = probe.messages.lock().expect("probe lock");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].native_payload_weight_bytes(), None);
    assert!(matches!(messages[0].payload(), TransportPayload::Encoded(_)));
    drop(messages);
    let _ = bus.shutdown(ShutdownMode::Immediate).expect("shutdown succeeds");
}

/// Minimal codec used to observe successful encoded transport admission.
struct WeightCodec {
    content_type: ContentType,
}

impl EventCodec<u32> for WeightCodec {
    /// Returns the fixed content type used by this probe.
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }
    /// No schema is required for the four-byte probe payload.
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }
    /// Encodes the borrowed integer as four little-endian bytes.
    fn encode(&self, value: &u32) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.to_le_bytes()))
    }
    /// Decoding is outside the outbound metadata contract.
    fn decode(&self, _: &EncodedPayload) -> Result<u32, CodecError> {
        unreachable!("weight probe only encodes")
    }
}
