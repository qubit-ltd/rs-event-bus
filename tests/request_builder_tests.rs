// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use qubit_event_bus::CodecError;
use qubit_event_bus::PublishError;
use qubit_event_bus::SubscriberId;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::DeliveryAttemptError;
use qubit_event_bus::error::PublishAttemptError;
use qubit_event_bus::model::AckMode;
use qubit_event_bus::model::Acknowledgement;
use qubit_event_bus::model::AcknowledgementError;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::BatchPublishResult;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::DeliveryContext;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::EventEnvelope;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::PublishRequestBuildError;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscribeRequestBuildError;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_id::Id;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryPolicy;

#[test]
fn simple_and_builder_requests_are_equivalent() -> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::<String>::new("orders.created")?;
    let simple = PublishRequest::new(topic.clone(), "one".to_owned())?;
    let built = PublishRequest::builder()
        .topic(topic.clone())
        .payload("one".to_owned())
        .header("trace-id", "t-1")
        .ordering_key("customer-1")
        .build()?;
    assert_eq!(simple.topic(), &topic);
    assert_eq!(built.header("trace-id"), Some("t-1"));

    let subscribe = SubscribeRequest::new("audit", topic)?;
    assert_eq!(subscribe.options().ack_mode(), AckMode::Auto);
    assert_eq!(subscribe.options().durability(), SubscriptionDurability::Ephemeral);
    assert_eq!(subscribe.options().start_position(), &StartPosition::New);
    Ok(())
}

#[test]
fn option_builders_replace_then_append_handlers_in_call_order() -> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::<String>::new("orders.created")?;
    let options = PublishOptions::<String>::builder().error_handler(|_, _| ()).build();
    let request = PublishRequest::builder()
        .topic(topic.clone())
        .payload("one".to_owned())
        .error_handler(|_, _| ())
        .options(options)
        .error_handler(|_, _| ())
        .build()?;
    assert_eq!(request.options().error_handler_count(), 2);

    let subscribe_options = SubscribeOptions::<String>::builder().ack_mode(AckMode::Manual).build();
    let subscriber = SubscribeRequest::builder()
        .subscriber_id(SubscriberId::new("audit")?)
        .topic(topic)
        .options(subscribe_options)
        .ack_mode(AckMode::Auto)
        .build()?;
    assert_eq!(subscriber.options().ack_mode(), AckMode::Auto);
    Ok(())
}

#[test]
fn retry_policy_from_direct_dependency_reaches_both_builders() -> Result<(), Box<dyn std::error::Error>> {
    let policy = RetryPolicy::builder().max_attempts(3).build()?;
    let topic = Topic::<String>::new("orders.created")?;
    let publish = PublishRequest::builder()
        .topic(topic.clone())
        .payload("one".into())
        .retry_policy(policy.clone())
        .retry_rule(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::UseDefault)
        .build()?;
    let subscribe = SubscribeRequest::builder()
        .subscriber_id(SubscriberId::new("audit")?)
        .topic(topic)
        .retry_policy(policy)
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| RetryDecision::UseDefault)
        .build()?;
    assert_eq!(
        publish
            .options()
            .retry_policy()
            .unwrap()
            .admission_limits()
            .max_attempts()
            .get(),
        3
    );
    assert_eq!(
        subscribe
            .options()
            .retry_policy()
            .unwrap()
            .admission_limits()
            .max_attempts()
            .get(),
        3
    );
    Ok(())
}

#[test]
fn request_builders_validate_required_fields_and_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::<String>::new("orders.created")?;
    assert!(matches!(
        PublishRequest::<String>::builder().build(),
        Err(PublishRequestBuildError::MissingField("topic"))
    ));
    assert!(matches!(
        PublishRequest::builder().topic(topic.clone()).build(),
        Err(PublishRequestBuildError::MissingField("payload"))
    ));
    assert!(matches!(
        PublishRequest::builder()
            .topic(topic.clone())
            .payload("one".into())
            .header("bad key", "x")
            .build(),
        Err(PublishRequestBuildError::InvalidHeader(_))
    ));
    assert!(matches!(
        PublishRequest::builder()
            .topic(topic.clone())
            .payload("one".into())
            .ordering_key(" ")
            .build(),
        Err(PublishRequestBuildError::InvalidOrderingKey)
    ));
    let built = PublishRequest::builder()
        .topic(topic)
        .payload("one".into())
        .header("trace-id", "first")
        .headers([("trace-id", "second"), ("origin", "web")])
        .delay(Duration::from_secs(1))
        .build()?;
    assert_eq!(built.header("trace-id"), Some("second"));
    assert_eq!(built.envelope().delay(), Some(Duration::from_secs(1)));
    assert!(matches!(
        SubscribeRequest::<String>::builder().build(),
        Err(SubscribeRequestBuildError::MissingField("subscriber_id"))
    ));
    assert!(matches!(
        SubscribeRequest::<String>::builder()
            .subscriber_id(SubscriberId::new("audit")?)
            .build(),
        Err(SubscribeRequestBuildError::MissingField("topic"))
    ));
    assert!(matches!(
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("audit")?)
            .topic(Topic::<String>::new("orders.created")?)
            .provider_option("unnamespaced", "x")
            .build(),
        Err(SubscribeRequestBuildError::InvalidProviderOption(_))
    ));
    Ok(())
}

#[test]
fn generated_publish_request_ids_are_uuid_v4_and_custom_ids_are_preserved() -> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::<String>::new("orders.created")?;
    let generated = PublishRequest::new(topic.clone(), "one".to_owned())?;
    let generated_id = generated.envelope().id().as_str();
    assert_eq!(generated_id.len(), 36);
    assert_eq!(&generated_id[14..15], "4");
    assert!(matches!(&generated_id[19..20], "8" | "9" | "a" | "b"));

    let custom_id = EventId::new("caller-event-42")?;
    let built = PublishRequest::builder()
        .topic(topic)
        .payload("two".to_owned())
        .event_id(custom_id)
        .build()?;
    assert_eq!(built.envelope().id().as_str(), "caller-event-42");
    Ok(())
}

#[test]
fn acknowledgement_first_terminal_decision_wins() {
    let ack = Acknowledgement::new();
    let clone = ack.clone();
    assert!(ack.ack().is_ok());
    assert!(clone.ack().is_ok());
    assert!(matches!(clone.nack(), Err(AcknowledgementError::AlreadyCompleted)));
    assert!(ack.is_acked());
    let nack = Acknowledgement::new();
    assert!(!nack.is_nacked());
    assert!(!nack.is_completed());
    assert!(nack.nack().is_ok());
    assert!(nack.nack().is_ok());
    assert!(nack.is_nacked());
    assert!(nack.is_completed());
    assert!(matches!(nack.ack(), Err(AcknowledgementError::AlreadyCompleted)));
}

#[test]
fn batch_counts_admission_drop_and_failure_without_handler_completion() -> Result<(), Box<dyn std::error::Error>> {
    let id_1 = EventId::new("event-1")?;
    let id_2 = EventId::new("event-2")?;
    let provider = ProviderId::new("local")?;
    let accepted = PublishReceipt::new(
        id_1.clone(),
        Some(id_1),
        provider.clone(),
        PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        },
    );
    let dropped = PublishReceipt::new(id_2, None, provider, PublishAcknowledgement::DroppedByInterceptor);
    let batch = BatchPublishResult::new(vec![Ok(accepted), Ok(dropped), Err(PublishError::Closed)]);
    assert_eq!(batch.total_count(), 3);
    assert_eq!(batch.accepted_count(), 1);
    assert_eq!(batch.dropped_count(), 1);
    assert_eq!(batch.failure_count(), 1);
    assert!(batch.items()[0].is_ok());
    assert!(batch.items()[2].is_err());
    Ok(())
}

#[test]
fn batch_counts_destination_admissions_without_claiming_handler_completion() -> Result<(), Box<dyn std::error::Error>> {
    let provider = ProviderId::new("local")?;
    let receipt =
        |name: &str, acknowledgement: PublishAcknowledgement| -> Result<PublishReceipt, Box<dyn std::error::Error>> {
            let id = EventId::new(name)?;
            Ok(PublishReceipt::new(
                id.clone(),
                Some(id),
                provider.clone(),
                acknowledgement,
            ))
        };
    let admission = |number: u64,
                     subscriber: &str,
                     status: AdmissionStatus|
     -> Result<DestinationAdmission, Box<dyn std::error::Error>> {
        Ok(DestinationAdmission::new(
            Id::new(number),
            SubscriberId::new(subscriber)?,
            status,
        ))
    };
    let batch = BatchPublishResult::new(vec![
        Ok(receipt(
            "broker",
            PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            },
        )?),
        Ok(receipt(
            "local-accepted",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(1, "accepted", AdmissionStatus::Accepted)?]),
        )?),
        Ok(receipt(
            "local-empty",
            PublishAcknowledgement::DestinationAdmissions(vec![]),
        )?),
        Ok(receipt(
            "local-rejected",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(
                2,
                "rejected",
                AdmissionStatus::Rejected("full".into()),
            )?]),
        )?),
        Ok(receipt(
            "local-mixed",
            PublishAcknowledgement::DestinationAdmissions(vec![
                admission(3, "accepted-2", AdmissionStatus::Accepted)?,
                admission(4, "rejected-2", AdmissionStatus::Rejected("closed".into()))?,
            ]),
        )?),
        Ok(receipt(
            "local-filtered",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(5, "filtered", AdmissionStatus::Filtered)?]),
        )?),
        Ok(PublishReceipt::new(
            EventId::new("dropped")?,
            None,
            provider,
            PublishAcknowledgement::DroppedByInterceptor,
        )),
        Err(PublishError::Closed),
    ]);
    assert_eq!(batch.total_count(), 8);
    assert_eq!(batch.accepted_count(), 3);
    assert_eq!(batch.dropped_count(), 1);
    assert_eq!(batch.failure_count(), 3);
    assert_eq!(
        batch.items()[2].as_ref().unwrap().input_event_id().as_str(),
        "local-empty"
    );
    assert!(batch.items()[7].is_err());
    Ok(())
}

#[test]
fn topic_identity_ignores_codec_instance() -> Result<(), Box<dyn std::error::Error>> {
    struct StringCodec(ContentType);
    impl EventCodec<String> for StringCodec {
        fn content_type(&self) -> &ContentType {
            &self.0
        }
        fn schema_id(&self) -> Option<&SchemaId> {
            None
        }
        fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from(value.as_bytes()))
        }
        fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
            Ok(String::from_utf8_lossy(bytes).into_owned())
        }
    }
    let native = Topic::<String>::new("orders.created")?;
    let encoded = Topic::<String>::new_with_codec("orders.created", StringCodec(ContentType::new("text/plain")?))?;
    assert_eq!(native, encoded);
    assert!(native.codec().is_none());
    assert!(encoded.codec().is_some());
    let event = EventEnvelope::new(encoded, "payload".to_owned())?;
    assert_eq!(event.payload(), "payload");
    Ok(())
}

#[test]
fn request_interceptors_append_after_reused_options() -> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::<String>::new("orders.created")?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let first = seen.clone();
    let options = PublishOptions::<String>::builder()
        .interceptor(move |event| {
            first.lock().unwrap().push(1);
            Ok(Some(event))
        })
        .build();
    let second = seen.clone();
    let request = PublishRequest::builder()
        .topic(topic)
        .payload("payload".into())
        .options(options)
        .interceptor(move |event| {
            second.lock().unwrap().push(2);
            Ok(Some(event))
        })
        .build()?;
    let mut event = request.envelope().clone();
    for interceptor in request.options().interceptors() {
        event = interceptor(event)?.unwrap();
    }
    assert_eq!(event.payload(), "payload");
    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
    Ok(())
}

#[test]
fn delivery_context_preserves_transport_and_attempt_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber_id = SubscriberId::new("audit")?;
    let context = DeliveryContext::new(ProviderId::new("local")?, Id::new(7), subscriber_id)
        .with_retry_attempt(2)
        .with_provider_attempt(3)
        .with_settlement(true)
        .as_dead_letter();
    let event = Arc::new(EventEnvelope::new(
        Topic::<String>::new("orders.created")?,
        "payload".to_owned(),
    )?);
    let delivery = Delivery::new(event, context);
    assert_eq!(delivery.context().retry_attempt(), 2);
    assert_eq!(delivery.context().provider_attempt(), Some(3));
    assert!(delivery.context().can_settle());
    assert!(delivery.context().is_dead_letter());
    assert!(delivery.acknowledgement().ack().is_ok());
    Ok(())
}

#[test]
fn attempt_errors_keep_classification_and_source() {
    use std::error::Error;

    let publish = PublishAttemptError::new("transient", Some(true), std::io::Error::other("offline"));
    let delivery = DeliveryAttemptError::new("handler", Some(false), std::io::Error::other("bad record"));
    assert_eq!(publish.kind(), "transient");
    assert_eq!(publish.retryable(), Some(true));
    assert_eq!(publish.source().unwrap().to_string(), "offline");
    assert_eq!(delivery.kind(), "handler");
    assert_eq!(delivery.retryable(), Some(false));
    assert_eq!(delivery.source().unwrap().to_string(), "bad record");
}

#[test]
fn acknowledgement_race_has_one_terminal_winner() {
    use std::sync::Barrier;
    use std::thread;

    let ack = Acknowledgement::new();
    let barrier = Arc::new(Barrier::new(3));
    let left_ack = ack.clone();
    let left_barrier = barrier.clone();
    let left = thread::spawn(move || {
        left_barrier.wait();
        left_ack.ack().is_ok()
    });
    let right_ack = ack.clone();
    let right_barrier = barrier.clone();
    let right = thread::spawn(move || {
        right_barrier.wait();
        right_ack.nack().is_ok()
    });
    barrier.wait();
    assert_ne!(left.join().unwrap(), right.join().unwrap());
    assert!(ack.is_completed());
}

#[test]
fn subscriber_interceptors_append_to_reused_options() -> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::<String>::new("orders.created")?;
    let options = SubscribeOptions::<String>::builder()
        .interceptor(|delivery, next| next(delivery))
        .build();
    let request = SubscribeRequest::builder()
        .subscriber_id(SubscriberId::new("audit")?)
        .topic(topic)
        .options(options)
        .interceptor(|delivery, next| next(delivery))
        .build()?;
    assert_eq!(request.options().interceptors().len(), 2);
    Ok(())
}

#[test]
fn codec_registry_returns_typed_codec() -> Result<(), Box<dyn std::error::Error>> {
    struct TextCodec(ContentType);
    impl EventCodec<String> for TextCodec {
        fn content_type(&self) -> &ContentType {
            &self.0
        }
        fn schema_id(&self) -> Option<&SchemaId> {
            None
        }
        fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from(value.as_bytes()))
        }
        fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
            Ok(String::from_utf8_lossy(bytes).into_owned())
        }
    }
    let mut registry = CodecRegistry::new();
    registry.register::<String>(Arc::new(TextCodec(ContentType::new("text/plain")?)));
    let codec = registry.get::<String>().unwrap();
    assert_eq!(codec.decode(&codec.encode(&"payload".to_owned())?)?, "payload");
    let topic = Topic::<String>::new_with_shared_codec("orders.created", codec)?;
    assert!(topic.codec().is_some());
    assert!(registry.get::<u32>().is_none());
    Ok(())
}

#[test]
fn non_clone_payload_can_be_published_and_delivery_cloned() -> Result<(), Box<dyn std::error::Error>> {
    struct NonClone(u32);
    let topic = Topic::<NonClone>::new("orders.created")?;
    let request = PublishRequest::new(topic, NonClone(42))?;
    let event = Arc::new(request.into_parts().0);
    let context = DeliveryContext::new(ProviderId::new("local")?, Id::new(9), SubscriberId::new("audit")?);
    let delivery = Delivery::new(event, context);
    let clone = delivery.clone();
    assert_eq!(clone.payload().0, 42);
    Ok(())
}

#[test]
fn content_type_requires_both_mime_components() {
    assert!(ContentType::new("/").is_err());
    assert!(ContentType::new("text/").is_err());
    assert!(ContentType::new("/plain").is_err());
}
