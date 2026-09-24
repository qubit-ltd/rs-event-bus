// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public publisher-pipeline regression coverage for error and observer
//! branches.

mod support;

use std::any::TypeId;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CapabilityError;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::error::PublishAttemptError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::facade::EventBus;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::EventEnvelope;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::pipeline::Diagnostic;
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
use qubit_id::Id;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryErrorReason;
use qubit_retry::RetryPolicy;
use support::fake_spi::FakeEventBusSpi;
use support::manual_async::block_on;

struct FailingStringCodec {
    content_type: ContentType,
}

struct SuccessfulStringCodec {
    content_type: ContentType,
}

impl EventCodec<String> for SuccessfulStringCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

impl EventCodec<String> for FailingStringCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, _value: &String) -> Result<Arc<[u8]>, CodecError> {
        Err(CodecError::Encode {
            source: Box::new(std::io::Error::other("injected codec failure")),
        })
    }

    fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

struct CoverageSpi {
    payload_modes: PayloadModes,
    reject_admission: bool,
    publish_calls: AtomicUsize,
    native_payload_types: Mutex<Vec<TypeId>>,
}

struct PanickingSyncPublishSpi;

impl EventBusSpi for PanickingSyncPublishSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, _message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        panic!("scripted sync publish panic")
    }

    fn subscribe(&self, _request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        unreachable!("publisher coverage SPI is not used for subscriptions")
    }

    fn shutdown(&self, _mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

impl CoverageSpi {
    fn new(payload_modes: PayloadModes, reject_admission: bool) -> Self {
        Self {
            payload_modes,
            reject_admission,
            publish_calls: AtomicUsize::new(0),
            native_payload_types: Mutex::new(Vec::new()),
        }
    }
}

impl EventBusSpi for CoverageSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            self.payload_modes,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.publish_calls.fetch_add(1, Ordering::AcqRel);
        if let TransportPayload::Native(payload) = message.payload() {
            self.native_payload_types
                .lock()
                .unwrap()
                .push(payload.as_ref().type_id());
        }
        if self.reject_admission {
            Ok(PublishAcknowledgement::DestinationAdmissions(vec![
                DestinationAdmission::new(
                    Id::new(1),
                    SubscriberId::new("rejected-subscriber").expect("valid subscriber ID"),
                    AdmissionStatus::Rejected("injected admission rejection".into()),
                ),
            ]))
        } else {
            Ok(PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            })
        }
    }

    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Err(SpiError::Operation {
            provider_id: "coverage".into(),
            operation: "subscribe",
            resource: Some(request.topic().as_str().into()),
            kind: "unused_test_operation",
            retryable: Some(false),
            source: Box::new(std::io::Error::other("subscription is not used by this test SPI")),
        })
    }

    fn shutdown(&self, _mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

fn bus(spi: Arc<dyn EventBusSpi>) -> EventBus {
    EventBus::new(ProviderId::new("publisher-coverage").unwrap(), spi)
}

struct ScriptedFailureSpi {
    retryable: Option<bool>,
    failures_before_success: usize,
    publish_calls: AtomicUsize,
}

impl ScriptedFailureSpi {
    fn new(retryable: Option<bool>, failures_before_success: usize) -> Self {
        Self {
            retryable,
            failures_before_success,
            publish_calls: AtomicUsize::new(0),
        }
    }
}

impl EventBusSpi for ScriptedFailureSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, _message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let attempt = self.publish_calls.fetch_add(1, Ordering::AcqRel) + 1;
        if attempt <= self.failures_before_success {
            return Err(SpiError::Operation {
                provider_id: "scripted".into(),
                operation: "publish",
                resource: None,
                kind: "scripted_failure",
                retryable: self.retryable,
                source: Box::new(std::io::Error::other("scripted provider failure")),
            });
        }
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(&self, _request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        unreachable!("publisher coverage SPI is not used for subscriptions")
    }

    fn shutdown(&self, _mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

struct PanickingAsyncPublishSpi;
struct PanickingAsyncPublishConstructionSpi;

struct AcceptingAsyncPublishSpi;

impl AsyncEventBusSpi for AcceptingAsyncPublishSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish<'a>(&'a self, _message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async {
            Ok(PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            })
        })
    }

    fn subscribe<'a>(
        &'a self,
        _request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async { unreachable!("publisher coverage SPI is not used for subscriptions") })
    }

    fn shutdown<'a>(&'a self, _mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

impl AsyncEventBusSpi for PanickingAsyncPublishSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish<'a>(&'a self, _message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async { panic!("scripted async publish panic") })
    }

    fn subscribe<'a>(
        &'a self,
        _request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async { unreachable!("publisher coverage SPI is not used for subscriptions") })
    }

    fn shutdown<'a>(&'a self, _mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

impl AsyncEventBusSpi for PanickingAsyncPublishConstructionSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish<'a>(&'a self, _message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        panic!("scripted async publish construction panic")
    }

    fn subscribe<'a>(
        &'a self,
        _request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async { unreachable!("publisher coverage SPI is not used for subscriptions") })
    }

    fn shutdown<'a>(&'a self, _mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

#[test]
fn encoded_publish_retains_codec_failure_and_skips_provider_call() {
    let spi = Arc::new(CoverageSpi::new(PayloadModes::Encoded, false));
    let bus = bus(spi.clone());
    let topic = Topic::with_codec(
        "codec.failure",
        FailingStringCodec {
            content_type: ContentType::new("text/plain").unwrap(),
        },
    )
    .unwrap();
    let request = PublishRequest::new(topic, "payload".to_owned()).unwrap();

    let error = bus.publish(request).unwrap_err();
    assert!(matches!(error, PublishError::Codec(CodecError::Encode { .. })));
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(spi.publish_calls.load(Ordering::Acquire), 0);
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn sync_spi_publish_panic_becomes_source_preserving_publish_error() {
    let bus = bus(Arc::new(PanickingSyncPublishSpi));
    let request = PublishRequest::new(Topic::new("sync.panic").unwrap(), 11_u32).unwrap();

    let error = bus.publish(request).unwrap_err();
    let PublishError::Spi(source) = error else {
        panic!("sync SPI panic should be converted to a provider error, got {error:?}");
    };
    assert!(matches!(
        &source,
        SpiError::Operation {
            provider_id,
            operation: "publish",
            resource: Some(resource),
            kind: "spi_panic",
            ..
        } if provider_id.as_ref() == "publisher-coverage" && resource.as_ref() == "sync.panic"
    ));
    assert!(std::error::Error::source(&source).is_some());
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn native_publisher_supports_many_domain_payload_types_without_clone_bounds() {
    struct NonCloneCommand {
        _name: String,
    }

    let spi = Arc::new(CoverageSpi::new(PayloadModes::Native, false));
    let bus = bus(spi.clone());
    bus.publish(PublishRequest::new(Topic::new("generic.bool").unwrap(), true).unwrap())
        .unwrap();
    bus.publish(PublishRequest::new(Topic::new("generic.i8").unwrap(), -8_i8).unwrap())
        .unwrap();
    bus.publish(PublishRequest::new(Topic::new("generic.char").unwrap(), 'x').unwrap())
        .unwrap();
    bus.publish(PublishRequest::new(Topic::new("generic.u64").unwrap(), 64_u64).unwrap())
        .unwrap();
    bus.publish(PublishRequest::new(Topic::new("generic.u128").unwrap(), 42_u128).unwrap())
        .unwrap();
    bus.publish(PublishRequest::new(Topic::new("generic.bytes").unwrap(), vec![1_u8, 2, 3]).unwrap())
        .unwrap();
    bus.publish(PublishRequest::new(Topic::new("generic.array").unwrap(), [4_u8, 5, 6]).unwrap())
        .unwrap();
    bus.publish(PublishRequest::new(Topic::new("generic.option").unwrap(), Some(7_u16)).unwrap())
        .unwrap();
    bus.publish(PublishRequest::new(Topic::new("generic.result").unwrap(), Ok::<i32, &'static str>(8)).unwrap())
        .unwrap();
    bus.publish(
        PublishRequest::new(
            Topic::new("generic.domain").unwrap(),
            NonCloneCommand {
                _name: "reconcile".to_owned(),
            },
        )
        .unwrap(),
    )
    .unwrap();

    let observed = spi.native_payload_types.lock().unwrap();
    assert_eq!(spi.publish_calls.load(Ordering::Acquire), 10);
    assert_eq!(
        observed.as_slice(),
        [
            TypeId::of::<bool>(),
            TypeId::of::<i8>(),
            TypeId::of::<char>(),
            TypeId::of::<u64>(),
            TypeId::of::<u128>(),
            TypeId::of::<Vec<u8>>(),
            TypeId::of::<[u8; 3]>(),
            TypeId::of::<Option<u16>>(),
            TypeId::of::<Result<i32, &'static str>>(),
            TypeId::of::<NonCloneCommand>(),
        ]
    );
    drop(observed);
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn encoded_and_hybrid_capabilities_choose_the_supported_representation() {
    let encoded_spi = Arc::new(CoverageSpi::new(PayloadModes::Encoded, false));
    let encoded_bus = bus(encoded_spi.clone());
    let encoded_topic = Topic::with_codec(
        "representation.encoded",
        SuccessfulStringCodec {
            content_type: ContentType::new("text/plain").unwrap(),
        },
    )
    .unwrap();
    encoded_bus
        .publish(PublishRequest::new(encoded_topic, "encoded body".to_owned()).unwrap())
        .unwrap();
    assert_eq!(encoded_spi.publish_calls.load(Ordering::Acquire), 1);
    assert!(encoded_spi.native_payload_types.lock().unwrap().is_empty());
    encoded_bus.shutdown(ShutdownMode::Immediate).unwrap();

    let hybrid_spi = Arc::new(CoverageSpi::new(PayloadModes::NativeAndEncoded, false));
    let hybrid_bus = bus(hybrid_spi.clone());
    let hybrid_topic = Topic::with_codec(
        "representation.hybrid",
        SuccessfulStringCodec {
            content_type: ContentType::new("text/plain").unwrap(),
        },
    )
    .unwrap();
    hybrid_bus
        .publish(PublishRequest::new(hybrid_topic, "native preferred".to_owned()).unwrap())
        .unwrap();
    assert_eq!(
        hybrid_spi.native_payload_types.lock().unwrap().as_slice(),
        [TypeId::of::<String>()]
    );
    hybrid_bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn typed_metadata_mutation_error_is_returned_before_provider_publish() {
    let spi = Arc::new(CoverageSpi::new(PayloadModes::Native, false));
    let bus = bus(spi.clone());
    let options = PublishOptions::<String>::builder()
        .interceptor(|mut envelope| {
            envelope.set_header("invalid key", "value")?;
            Ok(Some(envelope))
        })
        .build();
    let request = PublishRequest::new(Topic::new("metadata.failure").unwrap(), "payload".to_owned())
        .unwrap()
        .with_options(options);

    let error = bus.publish(request).unwrap_err();
    assert!(matches!(error, PublishError::Configuration(_)));
    assert_eq!(spi.publish_calls.load(Ordering::Acquire), 0);
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn retry_policy_aborts_non_retryable_provider_failure_after_one_attempt() {
    let spi = Arc::new(FakeEventBusSpi::new());
    spi.fail_next_publish();
    let bus = bus(spi.clone());
    let options = PublishOptions::<u32>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(3).build().unwrap())
        .build();
    let request = PublishRequest::new(Topic::new("retry.classification").unwrap(), 7)
        .unwrap()
        .with_options(options);

    let error = bus.publish(request).unwrap_err();
    let PublishError::Retry(retry) = error else {
        panic!("retry policy failure should retain RetryError, got {error:?}");
    };
    assert!(matches!(retry.reason(), RetryErrorReason::Aborted));
    assert_eq!(retry.context().attempts(), 1);
    assert_eq!(
        spi.operation_log()
            .iter()
            .filter(|operation| **operation == "publish")
            .count(),
        1
    );
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn direct_spi_error_is_not_wrapped_in_retry_when_no_policy_is_configured() {
    let spi = Arc::new(ScriptedFailureSpi::new(Some(false), 1));
    let bus = bus(spi.clone());
    let request = PublishRequest::new(Topic::new("retry.disabled").unwrap(), 4_u32).unwrap();

    let error = bus.publish(request).unwrap_err();
    let PublishError::Spi(source) = error else {
        panic!("without a retry policy, the direct SPI error should be returned, got {error:?}");
    };
    assert!(matches!(
        source,
        SpiError::Operation {
            kind: "scripted_failure",
            ..
        }
    ));
    assert!(std::error::Error::source(&source).is_some());
    assert_eq!(spi.publish_calls.load(Ordering::Acquire), 1);
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn custom_retry_rule_can_override_explicit_non_retryable_spi_classification() {
    let spi = Arc::new(ScriptedFailureSpi::new(Some(false), 1));
    let bus = bus(spi.clone());
    let options = PublishOptions::<u32>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .retry_rule(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .build();
    let request = PublishRequest::new(Topic::new("retry.override").unwrap(), 5_u32)
        .unwrap()
        .with_options(options);

    bus.publish(request)
        .expect("custom rule should allow the second attempt");
    assert_eq!(spi.publish_calls.load(Ordering::Acquire), 2);
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn terminal_publish_error_handler_can_inspect_shared_non_clone_event_context() {
    struct NonClonePayload(String);

    let spi = Arc::new(ScriptedFailureSpi::new(Some(false), 1));
    let bus = bus(spi.clone());
    let observed = Arc::new(Mutex::new(None));
    let observed_by_handler = observed.clone();
    let created_at = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(123);
    let options = PublishOptions::<NonClonePayload>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
        .error_handler(move |context, _| {
            let cloned = context.clone();
            *observed_by_handler.lock().unwrap() = Some((
                context.payload().0.clone(),
                context.event_id().as_str().to_owned(),
                context.topic().name().to_owned(),
                context.header("trace-id").map(str::to_owned),
                context.ordering_key().map(str::to_owned),
                context.timestamp(),
                context.delay(),
                Arc::ptr_eq(&context.payload_arc(), &cloned.payload_arc()),
            ));
        })
        .build();
    let request = PublishRequest::builder()
        .topic(Topic::new("orders.non_clone").unwrap())
        .payload(NonClonePayload("payload-value".to_owned()))
        .event_id(EventId::new("failed-event").unwrap())
        .header("trace-id", "trace-123")
        .timestamp(created_at)
        .options(options)
        .build()
        .unwrap();

    assert!(bus.publish(request).is_err());
    assert_eq!(spi.publish_calls.load(Ordering::Acquire), 1);
    assert_eq!(
        *observed.lock().unwrap(),
        Some((
            "payload-value".to_owned(),
            "failed-event".to_owned(),
            "orders.non_clone".to_owned(),
            Some("trace-123".to_owned()),
            None,
            created_at,
            None,
            true,
        ))
    );
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn typed_publisher_interceptor_panic_is_converted_to_scoped_error() {
    let spi = Arc::new(CoverageSpi::new(PayloadModes::Native, false));
    let bus = bus(spi.clone());
    let options = PublishOptions::<u32>::builder()
        .interceptor(|_| -> Result<Option<EventEnvelope<u32>>, PublishError> {
            panic!("typed publisher middleware panic")
        })
        .build();
    let request = PublishRequest::new(Topic::new("interceptor.panic").unwrap(), 9_u32)
        .unwrap()
        .with_options(options);

    let error = bus.publish(request).unwrap_err();
    assert!(matches!(
        error,
        PublishError::InterceptorPanicked {
            scope: "typed",
            ref message,
        } if message.contains("typed publisher middleware panic")
    ));
    assert_eq!(spi.publish_calls.load(Ordering::Acquire), 0);
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn async_spi_future_panic_becomes_source_preserving_publish_error() {
    let bus = AsyncEventBus::new(
        ProviderId::new("async-publisher-coverage").unwrap(),
        Arc::new(PanickingAsyncPublishSpi),
    );
    let request = PublishRequest::new(Topic::new("async.panic").unwrap(), 11_u32).unwrap();

    let error = block_on(bus.publish(request)).unwrap_err();
    let PublishError::Spi(source) = error else {
        panic!("async SPI panic should be converted to a provider error, got {error:?}");
    };
    assert!(matches!(
        &source,
        SpiError::Operation {
            operation: "publish",
            kind: "spi_panic",
            ..
        }
    ));
    assert!(std::error::Error::source(&source).is_some());
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_spi_future_construction_panic_becomes_source_preserving_publish_error() {
    let bus = AsyncEventBus::new(
        ProviderId::new("async-publisher-construction-panic").unwrap(),
        Arc::new(PanickingAsyncPublishConstructionSpi),
    );
    let request = PublishRequest::new(Topic::new("async.construction.panic").unwrap(), 11_u32).unwrap();

    let error = block_on(bus.publish(request)).unwrap_err();
    let PublishError::Spi(source) = error else {
        panic!("async SPI construction panic should be converted to a provider error, got {error:?}");
    };
    assert!(matches!(
        &source,
        SpiError::Operation {
            provider_id,
            operation: "publish",
            resource: Some(resource),
            kind: "spi_panic",
            ..
        } if provider_id.as_ref() == "async-publisher-construction-panic"
            && resource.as_ref() == "async.construction.panic"
    ));
    assert!(std::error::Error::source(&source).is_some());
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_publisher_accepts_distinct_native_payload_types_without_clone_bounds() {
    let bus = AsyncEventBus::new(
        ProviderId::new("async-generic-publisher-coverage").unwrap(),
        Arc::new(AcceptingAsyncPublishSpi),
    );

    macro_rules! publish {
        ($name:literal, $request:expr) => {
            block_on(bus.publish($request))
                .unwrap_or_else(|error| panic!("async publish for {} payload failed: {error}", $name));
        };
    }
    publish!(
        "bool",
        PublishRequest::new(Topic::new("async.bool").unwrap(), true).unwrap()
    );
    publish!(
        "i16",
        PublishRequest::new(Topic::new("async.i16").unwrap(), -16_i16).unwrap()
    );
    publish!(
        "char",
        PublishRequest::new(Topic::new("async.char").unwrap(), 'z').unwrap()
    );
    publish!(
        "u64",
        PublishRequest::new(Topic::new("async.u64").unwrap(), 64_u64).unwrap()
    );
    publish!(
        "u128",
        PublishRequest::new(Topic::new("async.u128").unwrap(), 128_u128).unwrap()
    );
    publish!(
        "bytes",
        PublishRequest::new(Topic::new("async.bytes").unwrap(), vec![1_u8, 2]).unwrap()
    );
    publish!(
        "array",
        PublishRequest::new(Topic::new("async.array").unwrap(), [3_u8, 4]).unwrap()
    );
    publish!(
        "option",
        PublishRequest::new(Topic::new("async.option").unwrap(), Some(5_u16)).unwrap()
    );
    publish!(
        "result",
        PublishRequest::new(Topic::new("async.result").unwrap(), Ok::<i32, &'static str>(6)).unwrap()
    );
    publish!(
        "non-clone domain",
        PublishRequest::new(
            Topic::new("async.domain").unwrap(),
            std::sync::Mutex::new(String::from("non-clone payload")),
        )
        .unwrap()
    );

    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn diagnostics_skip_preflight_failures_isolate_panics_and_stop_after_observer_drop() {
    let spi = Arc::new(CoverageSpi::new(PayloadModes::Native, true));
    let bus = bus(spi.clone());
    let panic_handle = bus.observe_diagnostics(|_| panic!("observer panic is isolated"));
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = calls.clone();
    let handle = bus.observe_diagnostics(move |diagnostic| {
        assert!(matches!(diagnostic, Diagnostic::AdmissionRejected { .. }));
        observed_calls.fetch_add(1, Ordering::AcqRel);
    });

    let preflight_error = bus
        .publish(
            PublishRequest::builder()
                .topic(Topic::<u32>::new("diagnostics.preflight").unwrap())
                .payload(1)
                .delay(std::time::Duration::from_secs(1))
                .build()
                .unwrap(),
        )
        .unwrap_err();
    assert!(matches!(
        preflight_error,
        PublishError::Capability(CapabilityError::Unsupported {
            capability: "delayed_delivery"
        })
    ));
    assert_eq!(spi.publish_calls.load(Ordering::Acquire), 0);
    assert_eq!(calls.load(Ordering::Acquire), 0);

    bus.publish(PublishRequest::new(Topic::new("diagnostics.valid").unwrap(), 2).unwrap())
        .unwrap();
    assert_eq!(calls.load(Ordering::Acquire), 1);

    drop(handle);
    bus.publish(PublishRequest::new(Topic::new("diagnostics.closed").unwrap(), 3).unwrap())
        .unwrap();
    assert_eq!(calls.load(Ordering::Acquire), 1);

    drop(panic_handle);
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}
