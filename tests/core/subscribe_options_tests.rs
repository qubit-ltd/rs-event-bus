use qubit_event_bus::{
    EventEnvelope,
    SubscribeOptions,
    Topic,
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
