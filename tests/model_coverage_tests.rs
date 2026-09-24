// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public model behavior not exercised by the request-builder contract tests.

use std::any::TypeId;
use std::any::type_name;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::CodecError;
use qubit_event_bus::EventBus;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::error::DeliveryAttemptError;
use qubit_event_bus::error::DeliveryError;
use qubit_event_bus::error::PublishAttemptError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AckMode;
use qubit_event_bus::model::BatchPublishResult;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DEAD_LETTER_HEADER;
use qubit_event_bus::model::DeadLetterEvent;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::DeliveryContext;
use qubit_event_bus::model::EventEnvelope;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::ProviderMessageMetadata;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishOptionsBuilder;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeOptionsBuilder;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscribeRequestBuildError;
use qubit_event_bus::model::SubscribeRequestBuilder;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_id::Id;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryPolicy;

#[test]
fn test_subscribe_options_builder_exposes_configured_policy_and_clones_callbacks()
-> Result<(), Box<dyn std::error::Error>> {
    let defaults = SubscribeOptions::<u32>::new();
    let from_new = SubscribeOptionsBuilder::<u32>::new().build();
    let from_default = SubscribeOptionsBuilder::<u32>::default().build();
    assert_eq!(from_new.ack_mode(), defaults.ack_mode());
    assert_eq!(from_default.start_position(), defaults.start_position());
    assert_eq!(defaults.ack_mode(), AckMode::Auto);
    assert!(defaults.filter().is_none());
    assert!(defaults.retry_policy().is_none());
    assert!(defaults.retry_rule().is_none());
    assert!(defaults.retry_cancellation_token().is_none());
    assert!(defaults.error_handlers().is_empty());
    assert!(defaults.interceptors().is_empty());
    assert!(defaults.async_interceptors().is_empty());
    assert!(defaults.dead_letter().is_none());
    assert_eq!(defaults.ordering_policy(), OrderingPolicy::Unordered);
    assert!(defaults.consumer_group().is_none());
    assert_eq!(defaults.durability(), SubscriptionDurability::Ephemeral);
    assert_eq!(defaults.start_position(), &StartPosition::New);
    assert!(defaults.provider_options().is_empty());

    let options = SubscribeOptions::<u32>::builder()
        .ack_mode(AckMode::Manual)
        .filter(|event| *event.payload() > 10)
        .retry_policy(RetryPolicy::builder().max_attempts(2).build()?)
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| RetryDecision::UseDefault)
        .retry_cancellation_token(RetryCancellationToken::new())
        .error_handler(|_, _| FailureDirective::Discard)
        .interceptor(|delivery, next| next(delivery))
        .async_interceptor(|delivery, next| next(delivery))
        .dead_letter(DeadLetterPolicy::topic("dead.events")?)
        .ordering_policy(OrderingPolicy::PerKey)
        .consumer_group(ConsumerGroup::new("workers")?)
        .durability(SubscriptionDurability::Durable)
        .start_position(StartPosition::At("cursor-7".into()))
        .provider_option("local.prefetch", "4")
        .build();
    let event = EventEnvelope::new(Topic::<u32>::new("orders.created")?, 11)?;
    assert_eq!(options.ack_mode(), AckMode::Manual);
    assert!(options.filter().expect("configured filter")(&event));
    assert_eq!(
        options
            .retry_policy()
            .expect("configured retry policy")
            .admission_limits()
            .max_attempts()
            .get(),
        2
    );
    assert!(options.retry_rule().is_some());
    assert!(options.retry_cancellation_token().is_some());
    assert_eq!(options.error_handlers().len(), 1);
    assert_eq!(options.interceptors().len(), 1);
    assert_eq!(options.async_interceptors().len(), 1);
    assert!(matches!(options.dead_letter(), Some(DeadLetterPolicy::Topic(name)) if name.as_ref() == "dead.events"));
    assert_eq!(options.ordering_policy(), OrderingPolicy::PerKey);
    assert_eq!(options.consumer_group().expect("configured group").as_str(), "workers");
    assert_eq!(options.durability(), SubscriptionDurability::Durable);
    assert_eq!(options.start_position(), &StartPosition::At("cursor-7".into()));
    assert_eq!(
        options.provider_options().get("local.prefetch").map(String::as_str),
        Some("4")
    );

    let cloned = options.clone();
    assert!(Arc::ptr_eq(
        options.filter().expect("configured filter"),
        cloned.filter().expect("cloned filter")
    ));
    assert!(Arc::ptr_eq(
        options.retry_rule().expect("configured retry rule"),
        cloned.retry_rule().expect("cloned retry rule")
    ));
    assert!(cloned.retry_cancellation_token().is_some());
    assert_eq!(cloned.provider_options(), options.provider_options());
    assert!(Arc::ptr_eq(&options.error_handlers()[0], &cloned.error_handlers()[0]));
    assert!(Arc::ptr_eq(&options.interceptors()[0], &cloned.interceptors()[0]));
    assert!(Arc::ptr_eq(
        &options.async_interceptors()[0],
        &cloned.async_interceptors()[0]
    ));
    Ok(())
}

#[test]
fn test_string_subscribe_options_expose_retry_rule_and_shared_cancellation() -> Result<(), Box<dyn std::error::Error>> {
    let defaults = SubscribeOptions::<String>::new();
    assert!(defaults.retry_rule().is_none());
    assert!(defaults.retry_cancellation_token().is_none());

    let cancellation = RetryCancellationToken::new();
    let options = SubscribeOptions::<String>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build()?)
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| RetryDecision::Abort)
        .retry_cancellation_token(cancellation.clone())
        .build();
    let request = SubscribeRequest::new(
        SubscriberId::new("string-worker")?,
        Topic::<String>::new("orders.text")?,
    )
    .with_options(options);
    let configured = request.options();
    assert!(configured.retry_rule().is_some());
    let configured_token = configured
        .retry_cancellation_token()
        .expect("configured cancellation token");
    assert!(configured_token.shares_source_with(&cancellation));
    assert!(!configured_token.is_cancelled());
    cancellation.cancel();
    assert!(configured_token.is_cancelled());
    Ok(())
}

#[test]
fn test_subscribe_request_builder_exposes_every_policy_field() -> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::<u32>::new("orders.created")?;
    let request = SubscribeRequestBuilder::<u32>::new()
        .subscriber_id(SubscriberId::new("worker")?)
        .topic(topic.clone())
        .ack_mode(AckMode::Manual)
        .filter(|event| *event.payload() == 42)
        .retry_policy(RetryPolicy::builder().max_attempts(3).build()?)
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| RetryDecision::UseDefault)
        .retry_cancellation_token(RetryCancellationToken::new())
        .error_handler(|_, _| FailureDirective::Requeue)
        .interceptor(|delivery, next| next(delivery))
        .async_interceptor(|delivery, next| next(delivery))
        .dead_letter(DeadLetterPolicy::topic("dead.events")?)
        .ordering_policy(OrderingPolicy::PerKey)
        .consumer_group(ConsumerGroup::new("workers")?)
        .durability(SubscriptionDurability::Durable)
        .start_position(StartPosition::Earliest)
        .provider_option("local.prefetch", "12")
        .build()?;

    assert_eq!(request.subscriber_id().as_str(), "worker");
    assert_eq!(request.topic(), &topic);
    let options = request.options();
    assert_eq!(options.ack_mode(), AckMode::Manual);
    let event = EventEnvelope::new(topic, 42)?;
    assert!(options.filter().expect("configured filter")(&event));
    assert_eq!(
        options
            .retry_policy()
            .expect("configured policy")
            .admission_limits()
            .max_attempts()
            .get(),
        3
    );
    assert!(options.retry_rule().is_some());
    assert!(options.retry_cancellation_token().is_some());
    let error = DeliveryError::Handler {
        source: Box::new(std::io::Error::other("test failure")),
    };
    assert_eq!(options.error_handlers()[0](&event, &error), FailureDirective::Requeue);
    assert_eq!(options.interceptors().len(), 1);
    assert_eq!(options.async_interceptors().len(), 1);
    assert!(matches!(options.dead_letter(), Some(DeadLetterPolicy::Topic(name)) if name.as_ref() == "dead.events"));
    assert_eq!(options.ordering_policy(), OrderingPolicy::PerKey);
    assert_eq!(options.consumer_group().expect("configured group").as_str(), "workers");
    assert_eq!(options.durability(), SubscriptionDurability::Durable);
    assert_eq!(options.start_position(), &StartPosition::Earliest);
    assert_eq!(
        options.provider_options().get("local.prefetch").map(String::as_str),
        Some("12")
    );
    Ok(())
}

#[test]
fn test_subscribe_request_builder_default_reports_missing_identity() {
    assert!(matches!(
        SubscribeRequestBuilder::<u32>::default().build(),
        Err(SubscribeRequestBuildError::MissingField("subscriber_id"))
    ));
}

#[test]
fn test_subscribe_request_builder_replaces_policy_then_appends_later_values() -> Result<(), Box<dyn std::error::Error>>
{
    let topic = Topic::<u32>::new("orders.created")?;
    let reusable = SubscribeOptions::<u32>::builder()
        .ack_mode(AckMode::Manual)
        .filter(|event| *event.payload() > 100)
        .error_handler(|_, _| FailureDirective::Discard)
        .provider_option("local.mode", "reusable")
        .build();
    let mut provider_options = ProviderOptions::new();
    provider_options.insert("local.mode".into(), "merged".into());
    provider_options.insert("local.limit".into(), "8".into());
    let request = SubscribeRequest::builder()
        .subscriber_id(SubscriberId::new("old-subscriber")?)
        .topic(Topic::<u32>::new("orders.old")?)
        .error_handler(|_, _| FailureDirective::Retry)
        .provider_option("local.before", "discarded")
        .options(reusable)
        .subscriber_id(SubscriberId::new("current-subscriber")?)
        .topic(topic.clone())
        .ack_mode(AckMode::Auto)
        .filter(|event| *event.payload() % 2 == 0)
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .provider_options(provider_options)
        .provider_option("local.mode", "final")
        .build()?;

    assert_eq!(request.subscriber_id().as_str(), "current-subscriber");
    assert_eq!(request.topic(), &topic);
    assert_eq!(request.options().ack_mode(), AckMode::Auto);
    let event = EventEnvelope::new(topic, 12)?;
    let filter = request.options().filter().expect("configured filter");
    assert!(filter(&event));
    let odd_event = EventEnvelope::new(Topic::<u32>::new("orders.created")?, 13)?;
    assert!(!filter(&odd_event));
    let error = DeliveryError::Handler {
        source: Box::new(std::io::Error::other("test failure")),
    };
    assert_eq!(request.options().error_handlers().len(), 2);
    assert_eq!(
        request.options().error_handlers()[0](&event, &error),
        FailureDirective::Discard
    );
    assert_eq!(
        request.options().error_handlers()[1](&event, &error),
        FailureDirective::DeadLetter
    );
    assert_eq!(request.options().provider_options().len(), 2);
    assert_eq!(
        request
            .options()
            .provider_options()
            .get("local.mode")
            .map(String::as_str),
        Some("final")
    );
    assert_eq!(
        request
            .options()
            .provider_options()
            .get("local.limit")
            .map(String::as_str),
        Some("8")
    );
    Ok(())
}

#[test]
fn test_subscribe_request_with_options_and_into_parts_preserve_all_fields() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber_id = SubscriberId::new("audit")?;
    let topic = Topic::<u32>::new("orders.created")?;
    let options = SubscribeOptions::<u32>::builder()
        .ack_mode(AckMode::Manual)
        .ordering_policy(OrderingPolicy::PerKey)
        .consumer_group(ConsumerGroup::new("auditors")?)
        .build();
    let request = SubscribeRequest::new(subscriber_id.clone(), topic.clone()).with_options(options);
    assert_eq!(request.subscriber_id(), &subscriber_id);
    assert_eq!(request.topic(), &topic);
    assert_eq!(request.options().ack_mode(), AckMode::Manual);
    assert_eq!(request.options().ordering_policy(), OrderingPolicy::PerKey);
    let (actual_id, actual_topic, actual_options) = request.into_parts();
    assert_eq!(actual_id, subscriber_id);
    assert_eq!(actual_topic, topic);
    assert_eq!(
        actual_options.consumer_group().expect("configured group").as_str(),
        "auditors"
    );
    assert_eq!(actual_options.ack_mode(), AckMode::Manual);
    assert_eq!(actual_options.ordering_policy(), OrderingPolicy::PerKey);
    Ok(())
}

#[test]
fn test_subscribe_request_builder_rejects_retry_without_policy_and_invalid_provider_options()
-> Result<(), Box<dyn std::error::Error>> {
    let make_builder = || {
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("audit").expect("valid subscriber ID"))
            .topic(Topic::<u32>::new("orders.created").expect("valid topic"))
    };
    assert!(matches!(
        make_builder()
            .retry_cancellation_token(RetryCancellationToken::new())
            .build(),
        Err(SubscribeRequestBuildError::InvalidRetryConfiguration)
    ));
    assert!(matches!(
        make_builder()
            .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| { RetryDecision::UseDefault })
            .build(),
        Err(SubscribeRequestBuildError::InvalidRetryConfiguration)
    ));
    for (key, value) in [
        (".leading", "ok"),
        ("trailing.", "ok"),
        ("local.\nkey", "ok"),
        ("local.key", "bad\nvalue"),
    ] {
        assert!(matches!(
            make_builder().provider_option(key, value).build(),
            Err(SubscribeRequestBuildError::InvalidProviderOption(invalid)) if invalid == key
        ));
    }
    Ok(())
}

#[test]
fn test_consumer_group_and_dead_letter_policy_validate_public_input() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(ConsumerGroup::new("workers")?.as_str(), "workers");
    for invalid in ["", " workers", "workers ", "work\ners"] {
        assert!(matches!(
            ConsumerGroup::new(invalid),
            Err(ConfigurationError::InvalidField {
                field: "consumer_group",
                ..
            })
        ));
    }
    assert!(
        matches!(DeadLetterPolicy::topic("dead.events")?, DeadLetterPolicy::Topic(name) if name.as_ref() == "dead.events")
    );
    for invalid in ["", " dead.events", "dead.events ", "dead\nevents"] {
        assert!(matches!(
            DeadLetterPolicy::topic(invalid),
            Err(ConfigurationError::InvalidField {
                field: "dead_letter",
                ..
            })
        ));
    }
    Ok(())
}

#[test]
fn test_dead_letter_event_public_accessors_preserve_non_clone_original() -> Result<(), Box<dyn std::error::Error>> {
    struct NonClonePayload(u32);

    let bus = EventBus::local(LocalEventBusConfig::new().queue_capacity(8))?;
    let source_topic = Topic::<NonClonePayload>::new("orders.created")?;
    let dead_letter_topic = Topic::<DeadLetterEvent<NonClonePayload>>::new("dead.events")?;
    let (sender, receiver) = mpsc::channel();
    let dead_letter_subscription = bus.subscribe(
        SubscribeRequest::new(SubscriberId::new("dead-letter-reader")?, dead_letter_topic),
        move |delivery| {
            let dead_letter = delivery.payload();
            let original = dead_letter.original_event_arc();
            sender
                .send((
                    dead_letter.original_event().id().as_str().to_owned(),
                    std::ptr::eq(dead_letter.original_event(), original.as_ref()),
                    original.payload().0,
                    dead_letter.subscriber_id().as_str().to_owned(),
                    dead_letter.reason().to_owned(),
                ))
                .expect("dead-letter receiver remains available");
        },
    )?;
    let source_subscription = bus.subscribe(
        SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("failing-worker")?)
            .topic(source_topic.clone())
            .error_handler(|_, _| FailureDirective::DeadLetter)
            .dead_letter(DeadLetterPolicy::topic("dead.events")?)
            .build()?,
        |_| -> Result<(), DeliveryError> {
            Err(DeliveryError::Handler {
                source: Box::new(std::io::Error::other("forced delivery failure")),
            })
        },
    )?;
    let publish = PublishRequest::builder()
        .topic(source_topic.clone())
        .payload(NonClonePayload(42))
        .event_id(EventId::new("original-event-42")?)
        .build()?;
    bus.publish(publish)?;

    let (event_id, same_original, payload, subscriber_id, reason) = receiver.recv_timeout(Duration::from_secs(2))?;
    assert_eq!(event_id, "original-event-42");
    assert!(same_original);
    assert_eq!(payload, 42);
    assert_eq!(subscriber_id, "failing-worker");
    assert!(reason.contains("forced delivery failure"));
    source_subscription.cancel()?;
    dead_letter_subscription.cancel()?;
    bus.shutdown(ShutdownMode::Immediate)?;
    Ok(())
}

#[test]
fn test_spi_subscription_request_exposes_all_transport_fields() -> Result<(), Box<dyn std::error::Error>> {
    let mut provider_options = ProviderOptions::new();
    provider_options.insert("local.prefetch".into(), "16".into());
    let request = SpiSubscriptionRequest::new(
        Id::new(7),
        TopicAddress::new("orders.created")?,
        SubscriberId::new("worker")?,
        Some(ConsumerGroup::new("workers")?),
        SubscriptionDurability::Durable,
        StartPosition::At("cursor-7".into()),
        provider_options.clone(),
        std::any::TypeId::of::<String>(),
    );
    assert_eq!(request.subscription_id(), Id::new(7));
    assert_eq!(request.topic().as_str(), "orders.created");
    assert_eq!(request.subscriber_id().as_str(), "worker");
    assert_eq!(request.group().expect("configured group").as_str(), "workers");
    assert_eq!(request.durability(), SubscriptionDurability::Durable);
    assert_eq!(request.start_position(), &StartPosition::At("cursor-7".into()));
    assert_eq!(request.provider_options(), &provider_options);

    let standalone = SpiSubscriptionRequest::new(
        Id::new(8),
        TopicAddress::new("orders.created")?,
        SubscriberId::new("standalone")?,
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        std::any::TypeId::of::<String>(),
    );
    assert!(standalone.group().is_none());
    assert!(standalone.provider_options().is_empty());
    Ok(())
}

#[test]
fn test_topic_identity_codec_metadata_and_clone_are_type_safe() -> Result<(), Box<dyn std::error::Error>> {
    struct TextCodec {
        content_type: ContentType,
        schema_id: SchemaId,
    }
    impl EventCodec<String> for TextCodec {
        fn content_type(&self) -> &ContentType {
            &self.content_type
        }
        fn schema_id(&self) -> Option<&SchemaId> {
            Some(&self.schema_id)
        }
        fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from(value.as_bytes()))
        }
        fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
            Ok(String::from_utf8_lossy(bytes).into_owned())
        }
    }

    let native = Topic::<String>::new("orders.created")?;
    assert_eq!(native.name(), "orders.created");
    assert_eq!(native.payload_type_id(), TypeId::of::<String>());
    assert_eq!(native.payload_type_name(), type_name::<String>());
    assert_ne!(
        native.payload_type_id(),
        Topic::<u32>::new("orders.created")?.payload_type_id()
    );
    assert!(native.codec().is_none());
    assert!(native.schema_id().is_none());

    let shared_codec: Arc<dyn EventCodec<String>> = Arc::new(TextCodec {
        content_type: ContentType::new("text/plain")?,
        schema_id: SchemaId::new("order-v1")?,
    });
    let shared = Topic::<String>::with_shared_codec("orders.created", shared_codec.clone())?;
    let owned = Topic::<String>::with_codec(
        "orders.created",
        TextCodec {
            content_type: ContentType::new("text/plain")?,
            schema_id: SchemaId::new("order-v1")?,
        },
    )?;
    assert_eq!(native, shared);
    assert_eq!(shared, owned);
    assert!(owned.codec().is_some());
    assert_eq!(owned.schema_id().expect("owned codec schema").as_str(), "order-v1");
    assert!(Arc::ptr_eq(shared.codec().expect("configured codec"), &shared_codec));
    assert_eq!(shared.schema_id().expect("configured schema").as_str(), "order-v1");
    assert_eq!(
        shared.codec().expect("configured codec").content_type().as_str(),
        "text/plain"
    );
    assert_eq!(shared.codec().expect("configured codec").decode(b"example")?, "example");

    let cloned = shared.clone();
    assert!(Arc::ptr_eq(cloned.codec().expect("cloned codec"), &shared_codec));
    let mut native_hash = DefaultHasher::new();
    let mut encoded_hash = DefaultHasher::new();
    native.hash(&mut native_hash);
    shared.hash(&mut encoded_hash);
    assert_eq!(native_hash.finish(), encoded_hash.finish());
    assert!(format!("{shared:?}").contains("orders.created"));
    for invalid in ["", " orders.created", "orders.created ", "orders\ncreated"] {
        assert!(matches!(
            Topic::<String>::new(invalid),
            Err(ConfigurationError::InvalidField { field: "topic", .. })
        ));
    }
    assert!(Topic::<String>::new(&"x".repeat(256)).is_err());
    Ok(())
}

#[test]
fn test_publish_options_builder_and_clone_preserve_retry_and_interceptor_policy()
-> Result<(), Box<dyn std::error::Error>> {
    let defaults = PublishOptions::<String>::new();
    assert!(defaults.retry_policy().is_none());
    assert!(defaults.retry_rule().is_none());
    assert!(defaults.retry_cancellation_token().is_none());
    assert!(defaults.error_handlers().is_empty());
    assert_eq!(defaults.error_handler_count(), 0);
    assert!(defaults.interceptors().is_empty());
    assert!(PublishOptionsBuilder::<String>::new().build().interceptors().is_empty());
    assert!(
        PublishOptionsBuilder::<String>::default()
            .build()
            .retry_policy()
            .is_none()
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let first = seen.clone();
    let second = seen.clone();
    let options = PublishOptions::<String>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(4).build()?)
        .retry_rule(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::UseDefault)
        .retry_cancellation_token(RetryCancellationToken::new())
        .error_handler(|_, _| {})
        .error_handler(|_, _| {})
        .interceptor(move |event| {
            first.lock().expect("test mutex available").push(1);
            Ok(Some(event))
        })
        .interceptor(move |event| {
            second.lock().expect("test mutex available").push(2);
            Ok(Some(event))
        })
        .build();
    assert_eq!(
        options
            .retry_policy()
            .expect("configured policy")
            .admission_limits()
            .max_attempts()
            .get(),
        4
    );
    assert!(options.retry_rule().is_some());
    assert!(options.retry_cancellation_token().is_some());
    assert_eq!(options.error_handler_count(), 2);
    assert_eq!(options.error_handlers().len(), 2);
    assert_eq!(options.interceptors().len(), 2);

    let cloned = options.clone();
    assert!(Arc::ptr_eq(
        options.retry_rule().expect("configured retry rule"),
        cloned.retry_rule().expect("cloned retry rule")
    ));
    assert!(Arc::ptr_eq(&options.error_handlers()[0], &cloned.error_handlers()[0]));
    assert!(Arc::ptr_eq(&options.interceptors()[1], &cloned.interceptors()[1]));
    let request = PublishRequest::new(Topic::<String>::new("orders.created")?, "value".into())?.with_options(cloned);
    let (mut event, request_options) = request.into_parts();
    for interceptor in request_options.interceptors() {
        event = interceptor(event)?.expect("interceptor keeps event");
    }
    assert_eq!(event.payload(), "value");
    assert_eq!(*seen.lock().expect("test mutex available"), vec![1, 2]);
    Ok(())
}

#[test]
fn test_event_envelope_metadata_getters_and_header_mutations_preserve_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::<String>::new("orders.created")?;
    let plain = EventEnvelope::new(topic.clone(), "plain".to_owned())?;
    assert!(plain.headers().is_empty());
    assert_eq!(plain.header("trace-id"), None);
    assert_eq!(plain.ordering_key(), None);
    let request = PublishRequest::builder()
        .topic(topic.clone())
        .payload("payload".to_owned())
        .event_id(EventId::new("event-42")?)
        .header("trace-id", "first")
        .ordering_key("customer-7")
        .delay(Duration::from_millis(25))
        .build()?;
    let (mut event, _) = request.into_parts();
    assert_eq!(event.id().as_str(), "event-42");
    assert_eq!(event.topic(), &topic);
    assert_eq!(event.payload(), "payload");
    assert_eq!(event.header("trace-id"), Some("first"));
    assert_eq!(event.header("missing"), None);
    assert_eq!(event.headers().len(), 1);
    assert_eq!(event.headers().get("trace-id").map(String::as_str), Some("first"));
    assert_eq!(event.ordering_key(), Some("customer-7"));
    assert_eq!(event.delay(), Some(Duration::from_millis(25)));
    assert_eq!(event.timestamp(), event.clone().timestamp());

    assert_eq!(event.set_header("trace-id", "second")?, Some("first".into()));
    assert_eq!(event.set_header("tenant_id", "acme")?, None);
    assert_eq!(event.header("trace-id"), Some("second"));
    assert_eq!(event.remove_header("trace-id")?, Some("second".into()));
    assert_eq!(event.remove_header("trace-id")?, None);
    assert_eq!(event.headers().get("tenant_id").map(String::as_str), Some("acme"));
    for (key, value) in [("", "ok"), ("bad key", "ok"), ("trace-id", "bad\nvalue")] {
        assert!(matches!(
            event.set_header(key, value),
            Err(ConfigurationError::InvalidField {
                field: "event_header",
                ..
            })
        ));
    }
    assert!(event.set_header(DEAD_LETTER_HEADER.to_uppercase(), "v1").is_err());
    assert!(event.remove_header(DEAD_LETTER_HEADER).is_err());
    let shared = event.clone().into_payload();
    assert!(Arc::ptr_eq(&shared, &event.into_payload()));
    Ok(())
}

#[test]
fn test_string_publish_request_into_parts_preserves_specific_payload_and_policy()
-> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::<String>::new("orders.text")?;
    let options = PublishOptions::<String>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build()?)
        .build();
    let request = PublishRequest::builder()
        .topic(topic.clone())
        .payload("text payload".to_owned())
        .event_id(EventId::new("text-event-1")?)
        .header("trace-id", "trace-7")
        .build()?
        .with_options(options);
    let (event, extracted_options) = request.into_parts();
    assert_eq!(event.topic(), &topic);
    assert_eq!(event.id().as_str(), "text-event-1");
    assert_eq!(event.payload(), "text payload");
    assert_eq!(event.headers().get("trace-id").map(String::as_str), Some("trace-7"));
    assert_eq!(event.header("trace-id"), Some("trace-7"));
    assert_eq!(event.header("absent"), None);
    assert_eq!(event.ordering_key(), None);
    assert_eq!(
        extracted_options
            .retry_policy()
            .expect("reused retry policy")
            .admission_limits()
            .max_attempts()
            .get(),
        2
    );
    Ok(())
}

#[test]
fn test_delivery_context_and_delivery_getters_preserve_transport_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let provider = ProviderId::new("local")?;
    let subscriber = SubscriberId::new("audit")?;
    let default_context = DeliveryContext::new(provider.clone(), Id::new(9), subscriber.clone());
    assert_eq!(default_context.retry_attempt(), 1);
    assert_eq!(default_context.provider_attempt(), None);
    assert!(default_context.provider_metadata().is_empty());
    assert!(!default_context.can_settle());
    assert!(!default_context.is_dead_letter());

    let mut metadata = ProviderMessageMetadata::new();
    metadata.insert("partition".into(), "p1".into());
    let context = default_context
        .with_retry_attempt(3)
        .with_provider_attempt(5)
        .with_provider_metadata(metadata.clone())
        .with_settlement(true)
        .as_dead_letter();
    let event = Arc::new(EventEnvelope::new(Topic::<u32>::new("orders.created")?, 42)?);
    let delivery = Delivery::new(event.clone(), context);
    assert_eq!(delivery.payload(), &42);
    assert!(std::ptr::eq(delivery.event(), event.as_ref()));
    let actual = delivery.context();
    assert_eq!(actual.provider_id(), &provider);
    assert_eq!(actual.subscription_id(), Id::new(9));
    assert_eq!(actual.subscriber_id(), &subscriber);
    assert_eq!(actual.retry_attempt(), 3);
    assert_eq!(actual.provider_attempt(), Some(5));
    assert_eq!(actual.provider_metadata(), &metadata);
    assert!(actual.can_settle());
    assert!(actual.is_dead_letter());
    assert!(delivery.acknowledgement().ack().is_ok());
    assert!(delivery.clone().acknowledgement().is_acked());
    Ok(())
}

#[test]
fn test_publish_receipt_and_batch_into_items_preserve_order_and_admission() -> Result<(), Box<dyn std::error::Error>> {
    let provider = ProviderId::new("local")?;
    let input = EventId::new("original")?;
    let dispatched = EventId::new("transformed")?;
    let accepted = PublishReceipt::new(
        input.clone(),
        Some(dispatched.clone()),
        provider.clone(),
        PublishAcknowledgement::Accepted {
            provider_message_id: Some("message-1".into()),
            metadata: ProviderMessageMetadata::new(),
        },
    );
    let dropped = PublishReceipt::new(
        input.clone(),
        None,
        provider.clone(),
        PublishAcknowledgement::DroppedByInterceptor,
    );
    assert_eq!(accepted.input_event_id(), &input);
    assert_eq!(accepted.dispatched_event_id(), Some(&dispatched));
    assert_eq!(accepted.provider_id(), &provider);
    assert!(!accepted.acknowledgement().is_dropped());
    assert_eq!(dropped.dispatched_event_id(), None);
    assert!(dropped.acknowledgement().is_dropped());

    let batch = BatchPublishResult::new(vec![Ok(accepted.clone()), Ok(dropped.clone())]);
    assert_eq!(batch.total_count(), 2);
    assert_eq!(batch.accepted_count(), 1);
    assert_eq!(batch.dropped_count(), 1);
    assert_eq!(batch.failure_count(), 0);
    assert_eq!(batch.items()[0].as_ref().expect("accepted receipt"), &accepted);
    let results = batch.into_items();
    assert_eq!(results[0].as_ref().expect("first receipt"), &accepted);
    assert_eq!(results[1].as_ref().expect("second receipt"), &dropped);
    assert!(BatchPublishResult::new(Vec::new()).into_items().is_empty());
    Ok(())
}
