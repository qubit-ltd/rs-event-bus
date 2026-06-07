use qubit_event_bus::{
    EventEnvelope,
    PublishOptions,
    StagedEvent,
    StagedEventEnvelope,
    Topic,
};

#[test]
fn test_staged_event_envelope_preserves_typed_parts() {
    let topic = Topic::<String>::try_new("staged-event-envelope")
        .expect("topic should build");
    let options = PublishOptions::<String>::builder()
        .error_handler(|_event, _error| Ok(()))
        .build();
    let staged = StagedEventEnvelope::new(
        EventEnvelope::create(topic, "payload".to_string()),
        options,
    );

    assert_eq!(staged.envelope().payload(), "payload");
    assert_eq!(staged.options().error_handler_count(), 1);

    let boxed: Box<dyn StagedEvent> = Box::new(staged.clone());
    let recovered = boxed
        .into_any()
        .downcast::<StagedEventEnvelope<String>>()
        .expect("staged event should recover typed envelope");
    let (envelope, options) = recovered.into_parts();
    assert_eq!(envelope.payload(), "payload");
    assert_eq!(options.error_handler_count(), 1);
}
