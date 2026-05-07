use std::sync::{
    Arc,
    Mutex,
};

use qubit_event_bus::{
    DeadLetterPayload,
    DeadLetterRecord,
    EventBusError,
    EventEnvelope,
    LocalEventBusFactory,
    SubscribeOptions,
    Topic,
};

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
    let topic = Topic::<String>::try_new("local-factory").expect("topic should build");
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
        Topic::<DeadLetterPayload>::try_new("local-factory-dlq").expect("dlq topic should build");
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
    let topic = Topic::<String>::try_new("local-factory-default-dlq").expect("topic should build");
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
fn test_subscription_dead_letter_none_disables_factory_default_strategy() {
    let mut factory = LocalEventBusFactory::default();
    let dead_letter_topic = Topic::<DeadLetterPayload>::try_new("local-factory-dlq-disabled")
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
    let topic =
        Topic::<String>::try_new("local-factory-default-dlq-disabled").expect("topic should build");
    let dead_letters = Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
    })
    .expect("dead letter subscriber should register");
    let options = SubscribeOptions::<String>::builder()
        .dead_letter_strategy(|_subscriber_id, _failed, _error, _options| Ok(None))
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
    let topic =
        Topic::<String>::try_new("local-factory-default-dlq-error").expect("topic should build");
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
    let topic =
        Topic::<String>::try_new("local-factory-default-dlq-panic").expect("topic should build");
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
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    bus.publish(&topic, "payload".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    std::panic::set_hook(previous_hook);

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
