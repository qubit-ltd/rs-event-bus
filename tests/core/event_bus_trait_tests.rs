/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Tests for the event bus abstraction traits.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use qubit_event_bus::{
    EventBus, EventBusError, EventBusFactory, EventEnvelope, LocalEventBus, LocalEventBusFactory,
    PublishOptions, SubscribeOptions, Topic, TransactionalEventBus, TransactionalPublisher,
    UnsupportedTransactionalEventBus, UnsupportedTransactionalPublisher,
};

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
    fn start(&self) -> bool {
        self.start_count.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn shutdown(&self) -> bool {
        self.shutdown_count.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn publish_envelope_with_options<T>(
        &self,
        envelope: EventEnvelope<T>,
        _options: PublishOptions<T>,
    ) -> qubit_event_bus::EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        let payload = envelope.payload() as &dyn Any;
        if let Some(value) = payload.downcast_ref::<String>() {
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
    ) -> qubit_event_bus::EventBusResult<qubit_event_bus::Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: qubit_event_bus::IntoEventBusResult + 'static,
    {
        Err(EventBusError::unsupported_operation("defaulting_subscribe"))
    }

    fn wait_for_idle<T>(&self, _topic: &Topic<T>) -> qubit_event_bus::EventBusResult<()>
    where
        T: 'static,
    {
        Ok(())
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

fn expect_subscription_error<T>(
    result: qubit_event_bus::EventBusResult<qubit_event_bus::Subscription<T>>,
    message: &str,
) -> EventBusError {
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
        EventBus::publish(&bus, &topic, "before-start".to_string())
            .expect_err("stopped bus should reject publish"),
        EventBusError::not_started()
    );

    assert!(EventBus::start(&bus));
    let subscription = EventBus::subscribe(
        &bus,
        "trait-sub",
        &topic,
        move |event: EventEnvelope<String>| {
            captured
                .lock()
                .expect("received events should lock")
                .push(event.payload().clone());
        },
    )
    .expect("trait subscribe should work");
    EventBus::publish(&bus, &topic, "payload".to_string()).expect("trait publish should work");
    EventBus::wait_for_idle(&bus, &topic).expect("topic should become idle");

    assert_eq!(subscription.subscriber_id(), "trait-sub");
    assert_eq!(captured_payloads(&received), vec!["payload".to_string()]);
    assert!(EventBus::shutdown(&bus));
}

#[test]
fn test_event_bus_trait_async_and_batch_methods() {
    let bus = LocalEventBus::started();
    let topic = create_topic("trait-async");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    let subscription_handle = EventBus::subscribe_async(
        &bus,
        "async-sub",
        &topic,
        move |event: EventEnvelope<String>| {
            captured
                .lock()
                .expect("received events should lock")
                .push(event.payload().clone());
        },
    )
    .expect("async subscribe should schedule");
    let subscription = subscription_handle
        .join()
        .expect("subscribe thread should join")
        .expect("async subscribe should work");
    assert!(subscription.is_active());

    let async_handle = EventBus::publish_envelope_with_options_async(
        &bus,
        EventEnvelope::create(topic.clone(), "async".to_string()),
        PublishOptions::empty(),
    )
    .expect("async publish should schedule");
    async_handle
        .join()
        .expect("publish thread should join")
        .expect("async publish should work");

    EventBus::publish_all(
        &bus,
        vec![
            EventEnvelope::create(topic.clone(), "batch-1".to_string()),
            EventEnvelope::create(topic.clone(), "batch-2".to_string()),
        ],
    )
    .expect("batch publish should work");

    let handles = EventBus::publish_all_async(
        &bus,
        vec![EventEnvelope::create(
            topic.clone(),
            "batch-async".to_string(),
        )],
        PublishOptions::empty(),
    )
    .expect("batch async publish should schedule");
    for handle in handles {
        handle
            .join()
            .expect("batch async thread should join")
            .expect("batch async publish should work");
    }

    EventBus::wait_for_idle(&bus, &topic).expect("topic should become idle");

    assert_eq!(
        captured_payloads(&received),
        vec![
            "async".to_string(),
            "batch-1".to_string(),
            "batch-2".to_string(),
            "batch-async".to_string(),
        ]
    );
}

#[test]
fn test_local_event_bus_trait_overrides_delegate_to_inherent_methods() {
    let bus = LocalEventBus::started();
    let topic = create_topic("trait-local-overrides");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    bus.add_publisher_interceptor::<String, _>(|event| Some(event.with_header("trait", "true")))
        .expect("publisher interceptor should register");
    EventBus::subscribe_with_options(
        &bus,
        "local-trait-sub",
        &topic,
        move |event: EventEnvelope<String>| {
            captured
                .lock()
                .expect("received events should lock")
                .push(format!(
                    "{}:{}",
                    event.payload(),
                    event.headers().get("trait").expect("header should exist")
                ));
        },
        SubscribeOptions::empty(),
    )
    .expect("trait subscribe with options should work");

    EventBus::publish_envelope(
        &bus,
        EventEnvelope::create(topic.clone(), "envelope".to_string()),
    )
    .expect("trait envelope publish should work");
    EventBus::publish_envelope_with_options(
        &bus,
        EventEnvelope::create(topic.clone(), "options".to_string()),
        PublishOptions::empty(),
    )
    .expect("trait publish with options should work");
    EventBus::publish_async(&bus, &topic, "async".to_string())
        .expect("trait payload async publish should schedule")
        .join()
        .expect("trait payload async publish should join")
        .expect("trait payload async publish should work");
    EventBus::wait_for_idle(&bus, &topic).expect("topic should become idle");

    assert_eq!(
        captured_payloads(&received),
        vec![
            "async:true".to_string(),
            "envelope:true".to_string(),
            "options:true".to_string(),
        ]
    );
}

#[test]
fn test_event_bus_trait_default_methods_delegate_to_required_backend_methods() {
    let bus = DefaultingEventBus::default();
    let topic = create_topic("trait-defaults");

    assert!(EventBus::start(&bus));
    assert!(EventBus::close(&bus));
    EventBus::publish(&bus, &topic, "publish".to_string()).expect("default publish should work");
    EventBus::publish_envelope(
        &bus,
        EventEnvelope::create(topic.clone(), "envelope".to_string()),
    )
    .expect("default envelope publish should work");
    EventBus::publish_all(
        &bus,
        vec![EventEnvelope::create(
            topic.clone(),
            "batch-default".to_string(),
        )],
    )
    .expect("default batch publish should work");

    let payload_handle = EventBus::publish_async(&bus, &topic, "payload-async".to_string())
        .expect("default async publish should schedule");
    payload_handle
        .join()
        .expect("default async publish thread should join")
        .expect("default async publish should work");
    let envelope_handle = EventBus::publish_envelope_async(
        &bus,
        EventEnvelope::create(topic.clone(), "envelope-async".to_string()),
    )
    .expect("default envelope async publish should schedule");
    envelope_handle
        .join()
        .expect("default envelope async thread should join")
        .expect("default envelope async publish should work");
    let options_handle = EventBus::publish_envelope_with_options_async(
        &bus,
        EventEnvelope::create(topic.clone(), "options-async".to_string()),
        PublishOptions::empty(),
    )
    .expect("default options async publish should schedule");
    options_handle
        .join()
        .expect("default options async thread should join")
        .expect("default options async publish should work");
    for handle in EventBus::publish_all_async(
        &bus,
        vec![EventEnvelope::create(
            topic.clone(),
            "batch-async-default".to_string(),
        )],
        PublishOptions::empty(),
    )
    .expect("default batch async publish should schedule")
    {
        handle
            .join()
            .expect("default batch async thread should join")
            .expect("default batch async publish should work");
    }

    assert_eq!(
        expect_subscription_error(
            EventBus::subscribe(&bus, "sub", &topic, |_| ()),
            "default subscribe should delegate",
        ),
        EventBusError::unsupported_operation("defaulting_subscribe")
    );
    assert_eq!(
        expect_subscription_error(
            EventBus::subscribe_async(&bus, "async-sub", &topic, |_| ())
                .expect("default async subscribe should schedule")
                .join()
                .expect("default async subscribe thread should join"),
            "default async subscribe should delegate",
        ),
        EventBusError::unsupported_operation("defaulting_subscribe")
    );
    assert_eq!(
        expect_subscription_error(
            EventBus::subscribe_with_options_async(
                &bus,
                "options-async-sub",
                &topic,
                |_| (),
                SubscribeOptions::empty(),
            )
            .expect("default async subscribe with options should schedule")
            .join()
            .expect("default async subscribe with options thread should join"),
            "default async subscribe with options should delegate",
        ),
        EventBusError::unsupported_operation("defaulting_subscribe")
    );
    EventBus::wait_for_idle(&bus, &topic).expect("default wait should work");

    assert_eq!(
        bus.published_payloads(),
        vec![
            "batch-async-default".to_string(),
            "batch-default".to_string(),
            "envelope".to_string(),
            "envelope-async".to_string(),
            "options-async".to_string(),
            "payload-async".to_string(),
            "publish".to_string(),
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
        EventBus::publish(&bus, &topic, "accepted".to_string())
            .expect_err("factory-created bus should start stopped"),
        EventBusError::not_started()
    );

    EventBus::start(&bus);
    EventBus::subscribe(
        &bus,
        "factory-sub",
        &topic,
        move |event: EventEnvelope<String>| {
            captured
                .lock()
                .expect("received events should lock")
                .push(event.payload().clone());
        },
    )
    .expect("factory-created bus should subscribe with defaults");
    EventBus::publish(&bus, &topic, "rejected".to_string()).expect("publish should work");
    EventBus::publish(&bus, &topic, "accepted".to_string()).expect("publish should work");
    EventBus::wait_for_idle(&bus, &topic).expect("topic should become idle");

    assert_eq!(captured_payloads(&received), vec!["accepted".to_string()]);

    let started_bus = EventBusFactory::create_started(&factory);
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
        TransactionalPublisher::commit(&mut publisher)
            .expect_err("placeholder publisher should reject commit"),
        EventBusError::unsupported_operation("transactional_commit")
    );
    assert!(TransactionalPublisher::rollback(&mut publisher).is_ok());
}

#[test]
fn test_event_bus_factory_trait_default_methods() {
    let factory = DefaultingFactory;
    let bus = EventBusFactory::create_started(&factory);

    assert_eq!(bus.start_count.load(Ordering::SeqCst), 1);
    assert!(!EventBusFactory::is_transactional_supported(&factory));
    assert_eq!(
        EventBusFactory::create_transactional(&factory)
            .expect_err("default factory should reject transactional creation"),
        EventBusError::unsupported_operation("create_transactional")
    );
}

#[test]
fn test_local_event_bus_trait_async_methods_reject_stopped_bus() {
    let bus = LocalEventBus::new();
    let topic = create_topic("local-stopped-async");

    assert_eq!(
        EventBus::publish_envelope_async(
            &bus,
            EventEnvelope::create(topic.clone(), "payload".to_string()),
        )
        .expect_err("stopped bus should reject envelope async publish"),
        EventBusError::not_started()
    );
    assert_eq!(
        EventBus::publish_envelope_with_options_async(
            &bus,
            EventEnvelope::create(topic.clone(), "payload".to_string()),
            PublishOptions::empty(),
        )
        .expect_err("stopped bus should reject envelope async publish with options"),
        EventBusError::not_started()
    );
    assert_eq!(
        EventBus::publish_all_async(
            &bus,
            vec![EventEnvelope::create(topic.clone(), "payload".to_string())],
            PublishOptions::empty(),
        )
        .expect_err("stopped bus should reject batch async publish"),
        EventBusError::not_started()
    );
    assert_eq!(
        EventBus::subscribe_async(&bus, "sub", &topic, |_| ())
            .expect_err("stopped bus should reject async subscribe"),
        EventBusError::not_started()
    );
    assert_eq!(
        EventBus::subscribe_with_options_async(
            &bus,
            "sub",
            &topic,
            |_| (),
            SubscribeOptions::empty()
        )
        .expect_err("stopped bus should reject async subscribe with options"),
        EventBusError::not_started()
    );
}

#[test]
fn test_unsupported_transactional_event_bus_rejects_all_operations() {
    let bus = UnsupportedTransactionalEventBus::new();
    let topic = create_topic("unsupported-transactional");

    assert!(!EventBus::start(&bus));
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
        EventBus::publish_envelope_with_options_async(
            &bus,
            EventEnvelope::create(topic.clone(), "payload".to_string()),
            PublishOptions::empty(),
        )
        .expect_err("placeholder bus should reject async publish"),
        EventBusError::unsupported_operation("publish_async")
    );
    assert_eq!(
        expect_subscription_error(
            EventBus::subscribe_with_options(
                &bus,
                "sub",
                &topic,
                |_| (),
                SubscribeOptions::empty()
            ),
            "placeholder bus should reject subscribe",
        ),
        EventBusError::unsupported_operation("subscribe")
    );
    assert_eq!(
        EventBus::subscribe_with_options_async(
            &bus,
            "sub",
            &topic,
            |_| (),
            SubscribeOptions::empty()
        )
        .expect_err("placeholder bus should reject async subscribe"),
        EventBusError::unsupported_operation("subscribe_async")
    );
    assert_eq!(
        EventBus::wait_for_idle(&bus, &topic).expect_err("placeholder bus should reject wait"),
        EventBusError::unsupported_operation("wait_for_idle")
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
