use qubit_event_bus::{
    DEAD_LETTER_EVENT_ID,
    DEAD_LETTER_FAILURE_TYPE,
    DEAD_LETTER_ORDERING_KEY,
    DEAD_LETTER_SUBSCRIBER_ID,
    DeadLetterRecord,
    EventBusError,
    EventEnvelope,
    Topic,
};

#[test]
fn test_dead_letter_record_from_failure_preserves_metadata_and_payload() {
    let topic = Topic::<String>::try_new("dead-letter-record")
        .expect("topic should build");
    let envelope = EventEnvelope::create(topic, "payload".to_string())
        .with_ordering_key("order-1");
    let error = EventBusError::handler_failed("handler failed");

    let record =
        DeadLetterRecord::from_failure("subscriber", &envelope, &error);

    assert_eq!(
        record.metadata().get_str(DEAD_LETTER_SUBSCRIBER_ID),
        Some("subscriber")
    );
    assert_eq!(
        record.metadata().get_str(DEAD_LETTER_EVENT_ID),
        Some(envelope.id())
    );
    assert_eq!(
        record.metadata().get_str(DEAD_LETTER_FAILURE_TYPE),
        Some("handler_failed")
    );
    assert_eq!(
        record.metadata().get_str(DEAD_LETTER_ORDERING_KEY),
        Some("order-1")
    );
    assert_eq!(
        record.downcast_original_payload_ref::<String>(),
        Some(&"payload".to_string())
    );
    let original_payload = record.original_payload();
    assert_eq!(
        original_payload.as_ref().downcast_ref::<String>(),
        Some(&"payload".to_string())
    );
}
