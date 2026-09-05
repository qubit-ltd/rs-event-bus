use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::EventBusError;
use qubit_event_bus::EventEnvelope;
use qubit_event_bus::Topic;

#[test]
fn test_event_envelope_builder_requires_topic() {
    let error = EventEnvelope::<String>::builder()
        .payload("payload".to_string())
        .build()
        .expect_err("topic should be required");

    assert_eq!(error, EventBusError::missing_field("topic"));
}

#[test]
fn test_event_envelope_builder_sets_all_optional_metadata() {
    let topic = Topic::<String>::try_new("envelope-builder").expect("topic should build");
    let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(7);
    let envelope = EventEnvelope::builder()
        .id("event-1")
        .topic(topic.clone())
        .payload("payload".to_string())
        .header("trace-id", "trace-1")
        .timestamp(timestamp)
        .ordering_key("order-1")
        .delay(Duration::from_millis(5))
        .dead_letter(true)
        .build()
        .expect("complete envelope should build");

    assert_eq!(envelope.id(), "event-1");
    assert_eq!(envelope.topic(), &topic);
    assert_eq!(envelope.payload(), "payload");
    assert_eq!(envelope.headers().get("trace-id"), Some(&"trace-1".to_string()));
    assert_eq!(envelope.timestamp(), timestamp);
    assert_eq!(envelope.ordering_key(), Some("order-1"));
    assert_eq!(envelope.delay(), Some(Duration::from_millis(5)));
    assert!(envelope.is_dead_letter());
}
