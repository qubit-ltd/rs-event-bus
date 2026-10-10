// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public contracts for the boundary refactor.

use std::error::Error;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::num::NonZeroUsize;
use std::sync::Arc;

use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusError;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::error::PublishAttemptError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::error::PublishFailure;
use qubit_event_bus::error::ReceiveError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::facade::PayloadLimits;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DuplicateRiskPolicy;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

/// Supplies a codec with exact metadata matching and observable metadata input.
struct TextCodec {
    content_type: ContentType,
    schema_id: Option<SchemaId>,
}

impl EventCodec<String> for TextCodec {
    /// Returns the codec content type.
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    /// Returns the optional schema expected by this codec.
    fn schema_id(&self) -> Option<&SchemaId> {
        self.schema_id.as_ref()
    }

    /// Encodes text into immutable UTF-8 bytes.
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    /// Decodes the supplied bytes while retaining access to metadata.
    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        Ok(format!(
            "{}:{}",
            payload.content_type().as_str(),
            String::from_utf8_lossy(payload.bytes())
        ))
    }
}

/// Verifies finite defaults and independent positive limits.
#[test]
fn test_payload_limits_defaults_and_custom_values() {
    let limits = PayloadLimits::default();
    assert_eq!(limits.max_publish_bytes().get(), 1_048_576);
    assert_eq!(limits.max_receive_bytes().get(), 1_048_576);
    let custom = PayloadLimits::new(
        NonZeroUsize::new(8).expect("positive"),
        NonZeroUsize::new(16).expect("positive"),
    );
    assert_eq!(custom.max_publish_bytes().get(), 8);
    assert_eq!(custom.max_receive_bytes().get(), 16);
}

/// Verifies that metadata equality includes case and optional schema identity.
#[test]
fn test_metadata_validation_is_exact() {
    let codec = TextCodec {
        content_type: ContentType::TEXT_PLAIN,
        schema_id: None,
    };
    let exact = EncodedPayload::new(Arc::from(&b"hello"[..]), codec.content_type.clone(), None);
    codec.validate_metadata(&exact).expect("matching metadata");
    assert_eq!(codec.decode(&exact).expect("decode"), "text/plain:hello");
    let mismatched = EncodedPayload::new(
        Arc::from(&b"hello"[..]),
        ContentType::new("Text/plain").expect("valid MIME"),
        Some(SchemaId::new("v1").expect("valid schema")),
    );
    match codec.validate_metadata(&mismatched).expect_err("different metadata") {
        CodecError::MetadataMismatch {
            expected_content_type,
            actual_content_type,
            expected_schema_id,
            actual_schema_id,
        } => {
            assert_eq!(expected_content_type.as_str(), "text/plain");
            assert_eq!(actual_content_type.as_str(), "Text/plain");
            assert_eq!(expected_schema_id, None);
            assert_eq!(actual_schema_id.as_ref().map(SchemaId::as_str), Some("v1"));
        }
        other => panic!("unexpected metadata error: {other}"),
    }
}

/// Verifies that the wrapper exposes the original cause as its source.
#[test]
fn test_publish_failure_preserves_cause_and_identity() {
    let event_id = EventId::new("failed-event").expect("valid event ID");
    let failure = PublishFailure::new(event_id.clone(), PublishEffect::NotAccepted, PublishError::Closed);
    assert_eq!(failure.event_id(), &event_id);
    assert_eq!(failure.effect(), PublishEffect::NotAccepted);
    assert!(matches!(failure.cause(), PublishError::Closed));
    assert!(failure.source().expect("cause source").is::<PublishError>());
    assert!(matches!(failure.into_cause(), PublishError::Closed));
}

/// Verifies default duplicate protection and an explicit override.
#[test]
fn test_duplicate_risk_policy_default_and_builder() {
    assert_eq!(
        PublishOptions::<String>::default().duplicate_risk_policy(),
        DuplicateRiskPolicy::Forbid
    );
    assert_eq!(
        PublishOptions::<String>::builder()
            .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
            .build()
            .duplicate_risk_policy(),
        DuplicateRiskPolicy::AllowDuplicates
    );
}

/// Verifies that every optional schema mismatch is rejected independently of
/// MIME.
#[test]
fn test_metadata_validation_schema_options_and_versions() {
    let mime = ContentType::TEXT_PLAIN;
    let v1 = SchemaId::new("v1").expect("valid schema");
    let v2 = SchemaId::new("v2").expect("valid schema");
    for (expected, actual) in [
        (None, Some(v1.clone())),
        (Some(v1.clone()), None),
        (Some(v1.clone()), Some(v2)),
        (Some(v1.clone()), Some(v1)),
    ] {
        let should_match = expected == actual;
        let codec = TextCodec {
            content_type: mime.clone(),
            schema_id: expected,
        };
        let payload = EncodedPayload::new(Arc::from(&b"text"[..]), mime.clone(), actual);
        assert_eq!(codec.validate_metadata(&payload).is_ok(), should_match);
    }
}

/// Verifies explicit publish effect and conservative generic failure
/// classification.
#[test]
fn test_spi_publish_error_getters_and_attempt_effect() {
    let source = IoError::other("provider unavailable");
    let error = SpiError::Publish {
        provider_id: "provider".into(),
        resource: Some("topic".into()),
        kind: "unavailable",
        retryable: Some(true),
        effect: PublishEffect::NotAccepted,
        source: Box::new(source),
    };
    assert_eq!(error.provider_id(), "provider");
    assert_eq!(error.resource(), Some("topic"));
    assert_eq!(error.operation(), "publish");
    assert_eq!(error.kind(), "unavailable");
    assert_eq!(error.retryable(), Some(true));
    assert_eq!(error.publish_effect(), PublishEffect::NotAccepted);
    let attempt = PublishAttemptError::new(error.kind(), error.retryable(), error.publish_effect(), error);
    assert_eq!(attempt.effect(), PublishEffect::NotAccepted);
    assert!(attempt.source().expect("SPI cause").is::<SpiError>());
    let generic = SpiError::Operation {
        provider_id: "provider".into(),
        operation: "publish",
        resource: None,
        kind: "unavailable",
        retryable: Some(true),
        source: Box::new(IoError::other("unknown admission")),
    };
    assert_eq!(generic.publish_effect(), PublishEffect::MayHaveBeenAccepted);
}

/// Verifies that terminal errors share a structured cause without copying it.
#[test]
fn test_receive_stopped_retains_shared_reason() {
    let reason = Arc::new(SubscriptionStopReason::Codec {
        event_id: EventId::new("stopped-event").expect("valid event ID"),
        error: Arc::new(CodecError::NativeTypeMismatch),
    });
    let stopped = ReceiveError::Stopped(reason.clone());
    match stopped {
        ReceiveError::Stopped(actual) => assert!(Arc::ptr_eq(&reason, &actual)),
        other => panic!("unexpected: {other}"),
    }
}

/// Verifies that interception cannot replace the original publication identity.
#[test]
fn test_typed_interceptor_cannot_change_event_id() {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("local bus");
    let topic = Topic::<String>::new("identity.test").expect("valid topic");
    let event_id = EventId::new("original").expect("valid event ID");
    let request = PublishRequest::builder()
        .topic(topic)
        .payload("text".to_owned())
        .event_id(event_id.clone())
        .interceptor(|envelope| {
            Ok(Some(
                PublishRequest::builder()
                    .topic(envelope.topic().clone())
                    .payload(envelope.payload().clone())
                    .event_id(EventId::new("replacement").expect("valid ID"))
                    .build()
                    .expect("replacement request")
                    .envelope()
                    .clone(),
            ))
        })
        .build()
        .expect("valid request");
    let failure = bus.publish(request).expect_err("identity change rejected");
    assert_eq!(failure.event_id(), &event_id);
    assert_eq!(failure.effect(), PublishEffect::NotAccepted);
    assert!(matches!(failure.cause(), PublishError::Configuration(_)));
    let report = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
}

/// Verifies that receipts default to no prior uncertain admission.
#[test]
fn test_receipt_duplicate_possible_evidence() {
    let receipt = PublishReceipt::new(
        EventId::new("event").expect("valid event"),
        None,
        ProviderId::new("provider").expect("valid provider"),
        PublishAcknowledgement::DroppedByInterceptor,
    );
    assert!(!receipt.duplicate_possible());
    assert!(receipt.with_duplicate_possible(true).duplicate_possible());
}

/// Verifies aggregate conversion retains uncertain admission and the original
/// cause.
#[test]
fn test_event_bus_error_conversion_retains_publish_failure() {
    let event_id = EventId::new("uncertain-aggregate").expect("valid event ID");
    let cause = SpiError::Publish {
        provider_id: "provider".into(),
        resource: Some("topic".into()),
        kind: "lost_response",
        retryable: Some(true),
        effect: PublishEffect::MayHaveBeenAccepted,
        source: Box::new(IoError::new(ErrorKind::TimedOut, "original source")),
    };
    let aggregate: EventBusError = PublishFailure::new(
        event_id.clone(),
        PublishEffect::MayHaveBeenAccepted,
        PublishError::Spi(cause),
    )
    .into();
    match aggregate {
        EventBusError::PublishFailure(failure) => {
            assert_eq!(failure.event_id(), &event_id);
            assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
            assert!(matches!(failure.cause(), PublishError::Spi(_)));
            let source = failure
                .cause()
                .source()
                .expect("provider cause")
                .downcast_ref::<IoError>()
                .expect("original source type");
            assert_eq!(source.kind(), ErrorKind::TimedOut);
        }
        other => panic!("unexpected aggregate error: {other}"),
    }
}

/// Publishes through the facade while preserving its failure in an aggregate
/// result. Returns the receipt on admission, or the original publication
/// failure converted into EventBusError; invoking this helper may call the
/// provider.
fn publish_with_aggregate_error(
    bus: &EventBus,
    request: PublishRequest<String>,
) -> Result<PublishReceipt, EventBusError> {
    Ok(bus.publish(request)?)
}

/// Verifies the public facade question-mark path retains closed-publication
/// metadata.
#[test]
fn test_facade_publish_question_mark_preserves_failure() {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("local bus");
    let report = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
    let event_id = EventId::new("closed-aggregate").expect("valid event ID");
    let request = PublishRequest::builder()
        .topic(Topic::<String>::new("aggregate.test").expect("valid topic"))
        .payload("payload".to_owned())
        .event_id(event_id.clone())
        .build()
        .expect("valid request");
    match publish_with_aggregate_error(&bus, request).expect_err("closed publication") {
        EventBusError::PublishFailure(failure) => {
            assert_eq!(failure.event_id(), &event_id);
            assert_eq!(failure.effect(), PublishEffect::NotAccepted);
            assert!(matches!(failure.cause(), PublishError::Closed));
        }
        other => panic!("unexpected aggregate error: {other}"),
    }
}
