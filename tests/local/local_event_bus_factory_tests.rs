use std::sync::{
    Arc,
    Mutex,
};

use qubit_event_bus::{
    DeadLetterOriginalPayload,
    DeadLetterPayload,
    DeadLetterRecord,
    EventBus,
    EventBusError,
    EventBusFactory,
    EventBusResult,
    EventEnvelope,
    EventEnvelopeMetadata,
    LocalEventBusFactory,
    PublishOptions,
    PublisherInterceptor,
    SubscribeOptions,
    SubscriberInterceptor,
    SubscriberInterceptorAnyChain,
    SubscriberInterceptorChain,
    Topic,
};

use crate::support::PanicHookGuard;

struct PublicPublisherInterceptor;

impl PublisherInterceptor<String> for PublicPublisherInterceptor {
    fn on_publish(
        &self,
        envelope: EventEnvelope<String>,
    ) -> EventBusResult<Option<EventEnvelope<String>>> {
        Ok(Some(envelope.with_header("factory-publisher", "seen")))
    }
}

struct PublicSubscriberInterceptor {
    observed: Arc<Mutex<Vec<String>>>,
}

impl SubscriberInterceptor<String> for PublicSubscriberInterceptor {
    fn on_consume(
        &self,
        envelope: EventEnvelope<String>,
        chain: SubscriberInterceptorChain<String>,
    ) -> EventBusResult<()> {
        self.observed
            .lock()
            .expect("observed interceptors should lock")
            .push(format!("before:{}", envelope.payload()));
        let result =
            chain.proceed(envelope.with_header("factory-subscriber", "seen"));
        self.observed
            .lock()
            .expect("observed interceptors should lock")
            .push("after".to_string());
        result
    }
}

#[test]
fn test_event_bus_factory_trait_configures_defaults_and_public_interceptors() {
    let mut factory = LocalEventBusFactory::new();
    let observed_interceptors = Arc::new(Mutex::new(Vec::new()));
    EventBusFactory::set_default_publish_options::<String>(
        &mut factory,
        PublishOptions::empty(),
    )
    .expect("factory trait should accept default publish options");
    EventBusFactory::set_default_subscribe_options::<String>(
        &mut factory,
        SubscribeOptions::<String>::builder().priority(7).build(),
    )
    .expect("factory trait should accept default subscribe options");
    EventBusFactory::set_default_dead_letter_strategy::<String, _>(
        &mut factory,
        |_subscriber, _event, _error, _options| Ok(None),
    )
    .expect("factory trait should accept default dead-letter strategies");
    EventBusFactory::set_global_default_dead_letter_strategy(
        &mut factory,
        |_subscriber: &str,
         _metadata: EventEnvelopeMetadata,
         _payload: DeadLetterOriginalPayload,
         _error: &EventBusError| { Ok(None) },
    )
    .expect(
        "factory trait should accept global default dead-letter strategies",
    );
    EventBusFactory::add_publisher_interceptor::<String, _>(
        &mut factory,
        PublicPublisherInterceptor,
    )
    .expect("factory trait should accept public publisher interceptors");
    EventBusFactory::add_subscriber_interceptor::<String, _>(
        &mut factory,
        PublicSubscriberInterceptor {
            observed: Arc::clone(&observed_interceptors),
        },
    )
    .expect("factory trait should accept public subscriber interceptors");

    let bus = EventBusFactory::create_started(&factory)
        .expect("factory should start bus");
    let topic = Topic::<String>::try_new("factory-trait-config")
        .expect("topic should build");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);
    let subscription = EventBus::subscribe(&bus, "sub", &topic, move |event| {
        captured
            .lock()
            .expect("received payloads should lock")
            .push(format!(
                "{}:{}:{}",
                event.payload(),
                event
                    .headers()
                    .get("factory-publisher")
                    .expect("publisher header should exist"),
                event
                    .headers()
                    .get("factory-subscriber")
                    .expect("subscriber header should exist"),
            ));
    })
    .expect("subscription should use factory trait defaults");

    EventBus::publish(&bus, &topic, "payload".to_string())
        .expect("publish should work");
    EventBus::wait_for_idle(&bus, &topic).expect("topic should become idle");

    assert_eq!(subscription.options().priority(), 7);
    assert_eq!(
        received
            .lock()
            .expect("received payloads should lock")
            .as_slice(),
        ["payload:seen:seen"]
    );
    assert_eq!(
        observed_interceptors
            .lock()
            .expect("observed interceptors should lock")
            .as_slice(),
        ["before:payload", "after"]
    );
}

#[test]
fn test_event_bus_factory_trait_configures_global_interceptors() {
    let mut factory = LocalEventBusFactory::new();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let captured_observed = Arc::clone(&observed);
    EventBusFactory::add_global_publisher_interceptor(
        &mut factory,
        |metadata: EventEnvelopeMetadata| {
            Some(metadata.with_header("global-publisher", "seen"))
        },
    )
    .expect("factory trait should accept global publisher interceptors");
    EventBusFactory::add_global_subscriber_interceptor(
        &mut factory,
        move |metadata: EventEnvelopeMetadata,
              chain: SubscriberInterceptorAnyChain| {
            captured_observed
                .lock()
                .expect("observed should lock")
                .push(metadata.topic_name().to_string());
            chain.proceed()
        },
    )
    .expect("factory trait should accept global subscriber interceptors");

    let bus = EventBusFactory::create_started(&factory)
        .expect("factory should start bus");
    let topic = Topic::<String>::try_new("factory-global-interceptors")
        .expect("topic should build");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);
    EventBus::subscribe(&bus, "sub", &topic, move |event| {
        captured.lock().expect("received should lock").push(
            event
                .headers()
                .get("global-publisher")
                .expect("global publisher header should exist")
                .clone(),
        );
    })
    .expect("subscription should register");

    EventBus::publish(&bus, &topic, "payload".to_string())
        .expect("publish should work");
    EventBus::wait_for_idle(&bus, &topic).expect("topic should become idle");

    assert_eq!(
        received.lock().expect("received should lock").as_slice(),
        ["seen"]
    );
    assert_eq!(
        observed.lock().expect("observed should lock").as_slice(),
        ["factory-global-interceptors"]
    );
}

#[test]
fn test_local_event_bus_factory_applies_typed_default_subscribe_options() {
    let mut factory = LocalEventBusFactory::new();
    factory.set_default_subscribe_options(
        SubscribeOptions::<String>::builder()
            .filter(|event| event.payload() == "accepted")
            .priority(9)
            .build(),
    );
    let bus = factory.create_started().expect("factory should start bus");
    let topic =
        Topic::<String>::try_new("local-factory").expect("topic should build");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    let subscription = bus
        .subscribe("sub", &topic, move |event| {
            captured
                .lock()
                .expect("received payloads should lock")
                .push(event.payload().clone());
        })
        .expect("subscription should use factory defaults");
    bus.publish(&topic, "rejected".to_string())
        .expect("publish should succeed");
    bus.publish(&topic, "accepted".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(subscription.options().priority(), 9);
    assert_eq!(
        received
            .lock()
            .expect("received payloads should lock")
            .as_slice(),
        ["accepted"]
    );
}

#[test]
fn test_local_event_bus_factory_applies_default_dead_letter_strategy() {
    let mut factory = LocalEventBusFactory::default();
    let dead_letter_topic =
        Topic::<DeadLetterPayload>::try_new("local-factory-dlq")
            .expect("dlq topic should build");
    let dead_letter_target = dead_letter_topic.clone();
    factory.set_default_dead_letter_strategy::<String, _>(
        move |subscriber_id, failed, error, _options| {
            Ok(Some(EventEnvelope::create(
                dead_letter_target.clone(),
                DeadLetterRecord::from_failure(subscriber_id, failed, error),
            )))
        },
    );
    let bus = factory.create_started().expect("factory should start bus");
    let topic = Topic::<String>::try_new("local-factory-default-dlq")
        .expect("topic should build");
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
    })
    .expect("dead letter subscriber should register");
    bus.subscribe("sub", &topic, |_event| {
        Err(EventBusError::handler_failed("handler failed"))
    })
    .expect("subscription should use default dead-letter strategy");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    bus.wait_for_idle(&dead_letter_topic)
        .expect("dead letter topic should become idle");

    let dead_letters = dead_letters.lock().expect("dead letters should lock");
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(
        dead_letters[0]
            .payload()
            .metadata()
            .get::<String>("subscriber_id"),
        Some("sub".to_string())
    );
}

#[test]
fn test_local_event_bus_factory_applies_global_default_dead_letter_strategy() {
    let mut factory = LocalEventBusFactory::default();
    let dead_letter_topic =
        Topic::<DeadLetterPayload>::try_new("local-factory-global-dlq")
            .expect("dlq topic should build");
    let dead_letter_target = dead_letter_topic.clone();
    factory.set_global_default_dead_letter_strategy(
        move |subscriber_id: &str,
              failed: EventEnvelopeMetadata,
              original_payload: DeadLetterOriginalPayload,
              error: &EventBusError| {
            Ok(Some(EventEnvelope::create(
                dead_letter_target.clone(),
                DeadLetterRecord::from_metadata_failure(
                    subscriber_id,
                    failed,
                    original_payload,
                    error,
                ),
            )))
        },
    );
    let bus = factory.create_started().expect("factory should start bus");
    let topic = Topic::<i64>::try_new("local-factory-global-default-dlq")
        .expect("topic should build");
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
    })
    .expect("dead letter subscriber should register");
    bus.subscribe("sub", &topic, |_event| {
        Err(EventBusError::handler_failed("handler failed"))
    })
    .expect("subscription should use global default dead-letter strategy");

    bus.publish(&topic, 7_i64).expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    bus.wait_for_idle(&dead_letter_topic)
        .expect("dead letter topic should become idle");

    let dead_letters = dead_letters.lock().expect("dead letters should lock");
    assert_eq!(dead_letters.len(), 1);
    let record = dead_letters[0].payload();
    assert_eq!(
        record.metadata().get_str("subscriber_id"),
        Some("sub")
    );
    assert_eq!(
        record.metadata().get_str("topic"),
        Some("local-factory-global-default-dlq")
    );
    assert_eq!(record.downcast_original_payload_ref::<i64>(), Some(&7_i64));
}

#[test]
fn test_local_event_bus_factory_observes_global_default_dead_letter_strategy_error()
 {
    let mut factory = LocalEventBusFactory::new();
    factory.set_global_default_dead_letter_strategy(
        |_subscriber_id: &str,
         _metadata: EventEnvelopeMetadata,
         _payload: DeadLetterOriginalPayload,
         _error: &EventBusError| {
            Err(EventBusError::handler_failed(
                "global default dead-letter strategy failed",
            ))
        },
    );
    let bus = factory.create_started().expect("factory should start bus");
    let topic = Topic::<i64>::try_new("local-factory-global-dlq-error")
        .expect("topic should build");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("error observer should register");
    bus.subscribe("sub", &topic, |_event| {
        Err(EventBusError::handler_failed("handler failed"))
    })
    .expect("failing subscriber should register");

    bus.publish(&topic, 7_i64).expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    let observed = observed.lock().expect("observed errors should lock");
    assert!(observed.iter().any(|error| matches!(
        error,
        EventBusError::DeadLetterFailed { message }
            if message.contains("global default dead-letter strategy failed")
    )));
}

#[test]
fn test_local_event_bus_factory_observes_global_default_dead_letter_strategy_panic()
 {
    let mut factory = LocalEventBusFactory::new();
    factory.set_global_default_dead_letter_strategy(
        |_subscriber_id: &str,
         _metadata: EventEnvelopeMetadata,
         _payload: DeadLetterOriginalPayload,
         _error: &EventBusError| {
            panic!("global default dead-letter strategy panic");
        },
    );
    let bus = factory.create_started().expect("factory should start bus");
    let topic = Topic::<i64>::try_new("local-factory-global-dlq-panic")
        .expect("topic should build");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("error observer should register");
    bus.subscribe("sub", &topic, |_event| {
        Err(EventBusError::handler_failed("handler failed"))
    })
    .expect("failing subscriber should register");
    {
        let _panic_hook_guard = PanicHookGuard::suppress();
        bus.publish(&topic, 7_i64).expect("publish should succeed");
        bus.wait_for_idle(&topic).expect("topic should become idle");
    }

    let observed = observed.lock().expect("observed errors should lock");
    assert!(observed.iter().any(|error| matches!(
        error,
        EventBusError::DeadLetterFailed { message }
            if message.contains("global default dead-letter strategy panicked")
    )));
}

#[test]
fn test_subscription_dead_letter_none_disables_factory_default_strategy() {
    let mut factory = LocalEventBusFactory::default();
    let dead_letter_topic =
        Topic::<DeadLetterPayload>::try_new("local-factory-dlq-disabled")
            .expect("dlq topic should build");
    let dead_letter_target = dead_letter_topic.clone();
    factory.set_default_dead_letter_strategy::<String, _>(
        move |subscriber_id, failed, error, _options| {
            Ok(Some(EventEnvelope::create(
                dead_letter_target.clone(),
                DeadLetterRecord::from_failure(subscriber_id, failed, error),
            )))
        },
    );
    let bus = factory.create_started().expect("factory should start bus");
    let topic = Topic::<String>::try_new("local-factory-default-dlq-disabled")
        .expect("topic should build");
    let dead_letters =
        Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
    })
    .expect("dead letter subscriber should register");
    let options = SubscribeOptions::<String>::builder()
        .dead_letter_strategy(|_subscriber_id, _failed, _error, _options| {
            Ok(None)
        })
        .build();
    bus.subscribe_with_options(
        "sub",
        &topic,
        |_event| Err(EventBusError::handler_failed("handler failed")),
        options,
    )
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    bus.wait_for_idle(&dead_letter_topic)
        .expect("dead letter topic should become idle");

    assert!(
        dead_letters
            .lock()
            .expect("dead letters should lock")
            .is_empty()
    );
}

#[test]
fn test_local_event_bus_factory_observes_default_dead_letter_strategy_error() {
    let mut factory = LocalEventBusFactory::new();
    factory.set_default_dead_letter_strategy::<String, _>(
        |_subscriber_id, _failed, _error, _options| {
            Err(EventBusError::handler_failed(
                "default dead-letter strategy failed",
            ))
        },
    );
    let bus = factory.create_started().expect("factory should start bus");
    let topic = Topic::<String>::try_new("local-factory-default-dlq-error")
        .expect("topic should build");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("error observer should register");
    bus.subscribe("sub", &topic, |_event| {
        Err(EventBusError::handler_failed("handler failed"))
    })
    .expect("failing subscriber should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    let observed = observed.lock().expect("observed errors should lock");
    assert!(observed.iter().any(|error| matches!(
        error,
        EventBusError::DeadLetterFailed { message }
            if message.contains("default dead-letter strategy failed")
    )));
}

#[test]
fn test_local_event_bus_factory_observes_default_dead_letter_strategy_panic() {
    let mut factory = LocalEventBusFactory::new();
    factory.set_default_dead_letter_strategy::<String, _>(
        |_subscriber_id, _failed, _error, _options| {
            panic!("default dead-letter strategy panic");
        },
    );
    let bus = factory.create_started().expect("factory should start bus");
    let topic = Topic::<String>::try_new("local-factory-default-dlq-panic")
        .expect("topic should build");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("error observer should register");
    bus.subscribe("sub", &topic, |_event| {
        Err(EventBusError::handler_failed("handler failed"))
    })
    .expect("failing subscriber should register");
    {
        let _panic_hook_guard = PanicHookGuard::suppress();
        bus.publish(&topic, "payload".to_string())
            .expect("publish should succeed");
        bus.wait_for_idle(&topic).expect("topic should become idle");
    }

    let observed = observed.lock().expect("observed errors should lock");
    assert!(observed.iter().any(|error| matches!(
        error,
        EventBusError::DeadLetterFailed { message }
            if message.contains("default dead-letter strategy panicked")
    )));
}

#[test]
fn test_local_event_bus_factory_validates_handler_pool_options() {
    let mut factory = LocalEventBusFactory::new();

    assert_eq!(
        factory
            .set_subscription_handler_pool_size(0)
            .expect_err("zero pool size should be rejected"),
        EventBusError::invalid_argument(
            "pool_size",
            "subscription handler pool size must be greater than zero",
        )
    );
    assert_eq!(
        factory
            .set_subscription_handler_queue_capacity(Some(0))
            .expect_err("zero queue capacity should be rejected"),
        EventBusError::invalid_argument(
            "capacity",
            "subscription handler queue capacity must be greater than zero",
        )
    );
    factory
        .set_subscription_handler_pool_size(1)
        .expect("positive pool size should be accepted");
    factory
        .set_subscription_handler_queue_capacity(Some(1))
        .expect("positive queue capacity should be accepted");
    factory
        .set_subscription_handler_queue_capacity(None)
        .expect("unbounded queue should be accepted");
}
