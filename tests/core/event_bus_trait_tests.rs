// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the event bus abstraction traits.

use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_event_bus::DeadLetterOriginalPayload;
use qubit_event_bus::DeadLetterPayload;
use qubit_event_bus::DeadLetterRecord;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusError;
use qubit_event_bus::EventBusFactory;
use qubit_event_bus::EventBusResult;
use qubit_event_bus::EventEnvelope;
use qubit_event_bus::EventEnvelopeMetadata;
use qubit_event_bus::IntoEventBusResult;
use qubit_event_bus::LocalEventBus;
use qubit_event_bus::LocalEventBusFactory;
use qubit_event_bus::PublishOptions;
use qubit_event_bus::SubscribeOptions;
use qubit_event_bus::SubscriberInterceptorAnyChain;
use qubit_event_bus::SubscriberInterceptorChain;
use qubit_event_bus::Subscription;
use qubit_event_bus::Topic;
use qubit_event_bus::TransactionalEventBus;
use qubit_event_bus::TransactionalPublisher;
use qubit_event_bus::UnsupportedTransactionalEventBus;
use qubit_event_bus::UnsupportedTransactionalPublisher;

#[derive(Clone, Default)]
struct DefaultingEventBus {
    published: Arc<Mutex<Vec<String>>>,
    start_count: Arc<AtomicUsize>,
    shutdown_count: Arc<AtomicUsize>,
}

impl DefaultingEventBus {
    fn published_payloads(&self) -> Vec<String> {
        captured_payloads(&self.published)
    }
}

impl EventBus for DefaultingEventBus {
    fn start(&self) -> EventBusResult<bool> {
        self.start_count.fetch_add(1, Ordering::SeqCst);
        Ok(true)
    }

    fn shutdown(&self) -> bool {
        self.shutdown_count.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn publish_envelope_with_options<T>(
        &self,
        envelope: EventEnvelope<T>,
        _options: PublishOptions<T>,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        let payload = envelope.payload() as &dyn Any;
        if let Some(value) = payload.downcast_ref::<String>() {
            if value == "fail" {
                return Err(EventBusError::handler_failed("default publish failed"));
            }
            self.published
                .lock()
                .expect("published events should lock")
                .push(value.clone());
        }
        Ok(())
    }

    fn subscribe_with_options<T, S, F, R>(
        &self,
        _subscriber_id: S,
        _topic: &Topic<T>,
        _handler: F,
        _options: SubscribeOptions<T>,
    ) -> EventBusResult<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        Err(EventBusError::unsupported_operation("defaulting_subscribe"))
    }

    fn wait_for_idle<T>(&self, _topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static,
    {
        Ok(())
    }

    fn wait_for_idle_timeout<T>(&self, _topic: &Topic<T>, _timeout: Duration) -> EventBusResult<bool>
    where
        T: 'static,
    {
        Ok(true)
    }
}

struct DefaultingFactory;

impl EventBusFactory for DefaultingFactory {
    type Bus = DefaultingEventBus;
    type TransactionalBus = UnsupportedTransactionalEventBus;

    fn create(&self) -> Self::Bus {
        DefaultingEventBus::default()
    }
}

fn create_topic(name: &str) -> Topic<String> {
    Topic::try_new(name).expect("topic should build")
}

fn captured_payloads(events: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    let mut payloads = events.lock().expect("events should lock").clone();
    payloads.sort();
    payloads
}

fn expect_subscription_error<T>(result: EventBusResult<Subscription<T>>, message: &str) -> EventBusError {
    match result {
        Ok(_) => panic!("{message}"),
        Err(error) => error,
    }
}

#[test]
fn test_event_bus_trait_publish_subscribe_lifecycle() {
    let bus = LocalEventBus::new();
    let topic = create_topic("trait-lifecycle");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    assert_eq!(
        EventBus::publish(&bus, &topic, "before-start".to_string()).expect_err("stopped bus should reject publish"),
        EventBusError::not_started()
    );

    assert!(EventBus::start(&bus).expect("trait start should work"));
    let subscription = EventBus::subscribe(&bus, "trait-sub", &topic, move |event: EventEnvelope<String>| {
        captured
            .lock()
            .expect("received events should lock")
            .push(event.payload().clone());
    })
    .expect("trait subscribe should work");
    EventBus::publish(&bus, &topic, "payload".to_string()).expect("trait publish should work");
    EventBus::wait_for_idle(&bus, &topic).expect("topic should become idle");
    assert!(
        EventBus::wait_for_idle_timeout(&bus, &topic, Duration::from_millis(1))
            .expect("idle topic should return before timeout")
    );

    assert_eq!(subscription.subscriber_id(), "trait-sub");
    assert_eq!(captured_payloads(&received), vec!["payload".to_string()]);
    assert!(EventBus::shutdown(&bus));
}

#[test]
fn test_event_bus_trait_batch_methods() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("trait-batch");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    let subscription = EventBus::subscribe(&bus, "batch-sub", &topic, move |event: EventEnvelope<String>| {
        captured
            .lock()
            .expect("received events should lock")
            .push(event.payload().clone());
    })
    .expect("trait subscribe should work");
    assert!(subscription.is_active());

    EventBus::publish_envelope_with_options(
        &bus,
        EventEnvelope::create(topic.clone(), "single".to_string()),
        PublishOptions::empty(),
    )
    .expect("publish with options should work");
    EventBus::publish_with_options(&bus, &topic, "with-options".to_string(), PublishOptions::empty())
        .expect("publish payload with options should work");

    EventBus::publish_all(
        &bus,
        vec![
            EventEnvelope::create(topic.clone(), "batch-1".to_string()),
            EventEnvelope::create(topic.clone(), "batch-2".to_string()),
        ],
    )
    .expect("batch publish should work");
    EventBus::publish_all_with_options(
        &bus,
        vec![EventEnvelope::create(topic.clone(), "batch-with-options".to_string())],
        PublishOptions::empty(),
    )
    .expect("batch publish with options should work");

    EventBus::wait_for_idle(&bus, &topic).expect("topic should become idle");

    assert_eq!(
        captured_payloads(&received),
        vec![
            "batch-1".to_string(),
            "batch-2".to_string(),
            "batch-with-options".to_string(),
            "single".to_string(),
            "with-options".to_string(),
        ]
    );
}

#[test]
fn test_event_bus_trait_add_dead_letter_handler_delegates_to_subscription() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("trait-dead-letter-handler");
    let dead_letter_topic =
        Topic::<DeadLetterPayload>::try_new("trait-dead-letter-handler-dlq").expect("dead letter topic should build");
    let dead_letter_target = dead_letter_topic.clone();
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let captured_dead_letters = Arc::clone(&dead_letters);
    EventBus::add_dead_letter_handler(
        &bus,
        &dead_letter_topic,
        move |event| {
            captured_dead_letters
                .lock()
                .expect("dead letters should lock")
                .push(event);
        },
        SubscribeOptions::empty(),
    )
    .expect("dead letter handler should subscribe");
    let options = SubscribeOptions::<String>::builder()
        .dead_letter_strategy(move |subscriber_id, failed, error, _options| {
            Ok(Some(EventEnvelope::create(
                dead_letter_target.clone(),
                DeadLetterRecord::from_failure(subscriber_id, failed, error),
            )))
        })
        .build();
    EventBus::subscribe_with_options(
        &bus,
        "failing-sub",
        &topic,
        |_event| Err(EventBusError::handler_failed("handler failed")),
        options,
    )
    .expect("failing subscription should register");

    EventBus::publish(&bus, &topic, "payload".to_string()).expect("publish should work");
    EventBus::wait_for_idle(&bus, &topic).expect("source topic should become idle");
    EventBus::wait_for_idle(&bus, &dead_letter_topic).expect("dlq topic should become idle");

    let dead_letters = dead_letters.lock().expect("dead letters should lock");
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(
        dead_letters[0].payload().downcast_original_payload_ref::<String>(),
        Some(&"payload".to_string())
    );
}

#[test]
fn test_local_event_bus_trait_overrides_delegate_to_inherent_methods() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_publisher_interceptor::<String, _>(|event: EventEnvelope<String>| Some(event.with_header("trait", "true")))
        .expect("publisher interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("trait-local-overrides");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    EventBus::subscribe_with_options(
        &bus,
        "local-trait-sub",
        &topic,
        move |event: EventEnvelope<String>| {
            captured.lock().expect("received events should lock").push(format!(
                "{}:{}",
                event.payload(),
                event.headers().get("trait").expect("header should exist")
            ));
        },
        SubscribeOptions::empty(),
    )
    .expect("trait subscribe with options should work");

    EventBus::publish_envelope(&bus, EventEnvelope::create(topic.clone(), "envelope".to_string()))
        .expect("trait envelope publish should work");
    EventBus::publish_envelope_with_options(
        &bus,
        EventEnvelope::create(topic.clone(), "options".to_string()),
        PublishOptions::empty(),
    )
    .expect("trait publish with options should work");
    EventBus::wait_for_idle(&bus, &topic).expect("topic should become idle");

    assert_eq!(
        captured_payloads(&received),
        vec!["envelope:true".to_string(), "options:true".to_string(),]
    );
}

#[test]
fn test_event_bus_trait_default_methods_delegate_to_required_backend_methods() {
    let bus = DefaultingEventBus::default();
    let topic = create_topic("trait-defaults");

    assert!(EventBus::start(&bus).expect("default start should work"));
    assert!(EventBus::shutdown(&bus));
    EventBus::publish(&bus, &topic, "publish".to_string()).expect("default publish should work");
    EventBus::publish_with_options(
        &bus,
        &topic,
        "publish-with-options".to_string(),
        PublishOptions::empty(),
    )
    .expect("default publish with options should work");
    EventBus::publish_envelope(&bus, EventEnvelope::create(topic.clone(), "envelope".to_string()))
        .expect("default envelope publish should work");
    EventBus::publish_all(
        &bus,
        vec![EventEnvelope::create(topic.clone(), "batch-default".to_string())],
    )
    .expect("default batch publish should work");
    EventBus::publish_all_with_options(
        &bus,
        vec![EventEnvelope::create(
            topic.clone(),
            "batch-with-options-default".to_string(),
        )],
        PublishOptions::empty(),
    )
    .expect("default batch publish with options should work");
    let failed = EventEnvelope::create(topic.clone(), "fail".to_string());
    let failed_event_id = failed.id().to_string();
    let batch_result = EventBus::publish_all_with_options(
        &bus,
        vec![
            EventEnvelope::create(topic.clone(), "batch-before-failure".to_string()),
            failed,
            EventEnvelope::create(topic.clone(), "batch-after-failure".to_string()),
        ],
        PublishOptions::empty(),
    )
    .expect("default batch publish should summarize failures");
    assert_eq!(batch_result.total_count(), 3);
    assert_eq!(batch_result.accepted_count(), 2);
    assert_eq!(batch_result.dropped_count(), 0);
    assert_eq!(batch_result.failure_count(), 1);
    assert_eq!(batch_result.failures()[0].index(), 1);
    assert_eq!(batch_result.failures()[0].event_id(), failed_event_id);
    assert_eq!(
        batch_result.failures()[0].error(),
        &EventBusError::handler_failed("default publish failed")
    );

    assert_eq!(
        expect_subscription_error(
            EventBus::subscribe(&bus, "sub", &topic, |_| ()),
            "default subscribe should delegate",
        ),
        EventBusError::unsupported_operation("defaulting_subscribe")
    );
    EventBus::wait_for_idle(&bus, &topic).expect("default wait should work");

    assert_eq!(
        bus.published_payloads(),
        vec![
            "batch-after-failure".to_string(),
            "batch-before-failure".to_string(),
            "batch-default".to_string(),
            "batch-with-options-default".to_string(),
            "envelope".to_string(),
            "publish".to_string(),
            "publish-with-options".to_string(),
        ]
    );
}

#[test]
fn test_event_bus_factory_trait_creates_local_event_buses() {
    let mut factory = LocalEventBusFactory::new();
    factory.set_default_subscribe_options(
        SubscribeOptions::<String>::builder()
            .filter(|event| event.payload() == "accepted")
            .build(),
    );
    let bus = EventBusFactory::create(&factory);
    let topic = create_topic("trait-factory");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    assert!(!EventBusFactory::is_transactional_supported(&factory));
    assert_eq!(
        EventBus::publish(&bus, &topic, "accepted".to_string()).expect_err("factory-created bus should start stopped"),
        EventBusError::not_started()
    );

    EventBus::start(&bus).expect("factory-created bus should start");
    EventBus::subscribe(&bus, "factory-sub", &topic, move |event: EventEnvelope<String>| {
        captured
            .lock()
            .expect("received events should lock")
            .push(event.payload().clone());
    })
    .expect("factory-created bus should subscribe with defaults");
    EventBus::publish(&bus, &topic, "rejected".to_string()).expect("publish should work");
    EventBus::publish(&bus, &topic, "accepted".to_string()).expect("publish should work");
    EventBus::wait_for_idle(&bus, &topic).expect("topic should become idle");

    assert_eq!(captured_payloads(&received), vec!["accepted".to_string()]);

    let started_bus = EventBusFactory::create_started(&factory).expect("factory should create started bus");
    EventBus::publish(&started_bus, &topic, "no-subscribers".to_string())
        .expect("created started bus should accept publish");
}

#[test]
fn test_event_bus_factory_trait_rejects_transactional_create_when_unsupported() {
    let factory = LocalEventBusFactory::new();
    let topic = create_topic("trait-transactional");

    assert_eq!(
        EventBusFactory::create_transactional(&factory)
            .expect_err("local factory should reject transactional creation"),
        EventBusError::unsupported_operation("create_transactional")
    );

    let unsupported_bus = UnsupportedTransactionalEventBus::new();
    assert_eq!(
        TransactionalEventBus::create_transactional_publisher(&unsupported_bus)
            .expect_err("placeholder bus should reject publisher creation"),
        EventBusError::unsupported_operation("create_transactional_publisher")
    );

    let mut publisher = UnsupportedTransactionalPublisher::new();
    assert_eq!(
        TransactionalPublisher::publish(&mut publisher, &topic, "payload".to_string())
            .expect_err("placeholder publisher should reject staged publish"),
        EventBusError::unsupported_operation("transactional_publish")
    );
    assert_eq!(
        TransactionalPublisher::commit(&mut publisher).expect_err("placeholder publisher should reject commit"),
        EventBusError::unsupported_operation("transactional_commit")
    );
    assert!(TransactionalPublisher::rollback(&mut publisher).is_ok());
}

#[test]
fn test_event_bus_factory_trait_default_methods() {
    let mut factory = DefaultingFactory;
    let bus = EventBusFactory::create_started(&factory).expect("default factory should start bus");

    assert_eq!(bus.start_count.load(Ordering::SeqCst), 1);
    assert!(!EventBusFactory::is_transactional_supported(&factory));
    assert_eq!(
        EventBusFactory::create_transactional(&factory)
            .expect_err("default factory should reject transactional creation"),
        EventBusError::unsupported_operation("create_transactional")
    );
    assert_eq!(
        EventBusFactory::set_default_publish_options::<String>(&mut factory, PublishOptions::empty(),)
            .expect_err("default factory should reject publish defaults"),
        EventBusError::unsupported_operation("set_default_publish_options")
    );
    assert_eq!(
        EventBusFactory::set_default_subscribe_options::<String>(&mut factory, SubscribeOptions::empty(),)
            .expect_err("default factory should reject subscribe defaults"),
        EventBusError::unsupported_operation("set_default_subscribe_options")
    );
    assert_eq!(
        EventBusFactory::set_default_dead_letter_strategy::<String, _>(
            &mut factory,
            |_subscriber, _event, _error, _options| Ok(None),
        )
        .expect_err("default factory should reject dead-letter defaults"),
        EventBusError::unsupported_operation("set_default_dead_letter_strategy")
    );
    assert_eq!(
        EventBusFactory::set_global_default_dead_letter_strategy(
            &mut factory,
            |_subscriber: &str,
             _metadata: EventEnvelopeMetadata,
             _payload: DeadLetterOriginalPayload,
             _error: &EventBusError| { Ok(None) },
        )
        .expect_err("default factory should reject global dead-letter defaults"),
        EventBusError::unsupported_operation("set_global_default_dead_letter_strategy")
    );
    assert_eq!(
        EventBusFactory::add_publisher_interceptor::<String, _>(&mut factory, |event: EventEnvelope<String>| Some(
            event
        ),)
        .expect_err("default factory should reject publisher interceptors"),
        EventBusError::unsupported_operation("add_publisher_interceptor")
    );
    assert_eq!(
        EventBusFactory::add_global_publisher_interceptor(&mut factory, |metadata: EventEnvelopeMetadata| metadata,)
            .expect_err("default factory should reject global publisher interceptors"),
        EventBusError::unsupported_operation("add_global_publisher_interceptor")
    );
    assert_eq!(
        EventBusFactory::add_subscriber_interceptor::<String, _>(
            &mut factory,
            |event: EventEnvelope<String>, chain: SubscriberInterceptorChain<String>| { chain.proceed(event) },
        )
        .expect_err("default factory should reject subscriber interceptors"),
        EventBusError::unsupported_operation("add_subscriber_interceptor")
    );
    assert_eq!(
        EventBusFactory::add_global_subscriber_interceptor(
            &mut factory,
            |_metadata: EventEnvelopeMetadata, chain: SubscriberInterceptorAnyChain| { chain.proceed() },
        )
        .expect_err("default factory should reject global subscriber interceptors"),
        EventBusError::unsupported_operation("add_global_subscriber_interceptor")
    );
}

#[test]
fn test_unsupported_transactional_event_bus_rejects_all_operations() {
    let bus = UnsupportedTransactionalEventBus::new();
    let topic = create_topic("unsupported-transactional");

    assert!(!EventBus::start(&bus).expect("unsupported start should be idempotent"));
    assert!(!EventBus::shutdown(&bus));
    assert_eq!(
        EventBus::publish_envelope_with_options(
            &bus,
            EventEnvelope::create(topic.clone(), "payload".to_string()),
            PublishOptions::empty(),
        )
        .expect_err("placeholder bus should reject publish"),
        EventBusError::unsupported_operation("publish")
    );
    assert_eq!(
        expect_subscription_error(
            EventBus::subscribe_with_options(&bus, "sub", &topic, |_| (), SubscribeOptions::empty()),
            "placeholder bus should reject subscribe",
        ),
        EventBusError::unsupported_operation("subscribe")
    );
    assert_eq!(
        EventBus::wait_for_idle(&bus, &topic).expect_err("placeholder bus should reject wait"),
        EventBusError::unsupported_operation("wait_for_idle")
    );
    assert_eq!(
        EventBus::wait_for_idle_timeout(&bus, &topic, Duration::from_millis(1))
            .expect_err("placeholder bus should reject timeout wait"),
        EventBusError::unsupported_operation("wait_for_idle_timeout")
    );
    assert_eq!(
        TransactionalEventBus::publish_batch_atomically(
            &bus,
            vec![EventEnvelope::create(topic.clone(), "payload".to_string())],
            PublishOptions::empty(),
        )
        .expect_err("placeholder bus should reject atomic batch publish"),
        EventBusError::unsupported_operation("publish_batch_atomically")
    );
}
