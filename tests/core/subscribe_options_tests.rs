use qubit_event_bus::{
    EventBusError,
    EventEnvelope,
    SubscribeOptions,
    Topic,
    prefixed_dead_letters,
};

#[test]
fn test_subscribe_options_empty_handles_every_event() {
    let topic = Topic::<String>::try_new("subscribe-options").expect("topic should build");
    let envelope = EventEnvelope::create(topic, "payload".to_string());

    assert!(SubscribeOptions::<String>::empty().should_handle(&envelope));
}

#[test]
fn test_subscribe_options_filter_controls_handling() {
    let topic = Topic::<String>::try_new("subscribe-filter").expect("topic should build");
    let options = SubscribeOptions::<String>::builder()
        .filter(|event| event.payload() == "accepted")
        .build();

    assert!(options.should_handle(&EventEnvelope::create(
        topic.clone(),
        "accepted".to_string()
    )));
    assert!(!options.should_handle(&EventEnvelope::create(topic, "rejected".to_string())));
}

#[test]
fn test_subscribe_options_filter_panic_returns_not_handled() {
    let topic = Topic::<String>::try_new("subscribe-filter-panic").expect("topic should build");
    let envelope = EventEnvelope::create(topic, "payload".to_string());
    let options = SubscribeOptions::<String>::builder()
        .filter(|_event| -> bool {
            panic!("filter panic");
        })
        .build();
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        options.should_handle(&envelope)
    }));
    std::panic::set_hook(previous_hook);

    assert!(!result.expect("filter panic should be isolated"));
}

#[test]
fn test_prefixed_dead_letter_strategy_builds_dead_letter_topic() {
    let topic = Topic::<String>::try_new("source-topic").expect("topic should build");
    let failed = EventEnvelope::create(topic, "payload".to_string());
    let strategy = prefixed_dead_letters::<String>("dead.");

    let dead_letter = strategy(
        "subscriber",
        &failed,
        &EventBusError::handler_failed("boom"),
        &SubscribeOptions::empty(),
    )
    .expect("strategy should succeed")
    .expect("strategy should create a dead letter");

    assert_eq!(dead_letter.topic().name(), "dead.source-topic");
    assert!(dead_letter.is_dead_letter());
    assert_eq!(
        dead_letter
            .payload()
            .metadata()
            .get::<String>("subscriber_id"),
        Some("subscriber".to_string())
    );
}
