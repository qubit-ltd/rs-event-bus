use std::any::TypeId;

use qubit_event_bus::{
    EventEnvelope,
    PublishOptions,
    StagedEvent,
    StagedEventEnvelope,
    Topic,
};

#[test]
fn test_staged_event_exposes_metadata_and_payload_type() {
    let topic = Topic::<String>::try_new("staged-event").expect("topic should build");
    let staged = StagedEventEnvelope::new(
        EventEnvelope::create(topic.clone(), "payload".to_string()),
        PublishOptions::empty(),
    );

    assert_eq!(staged.metadata().topic_name(), "staged-event");
    assert_eq!(staged.metadata().payload_type_name(), topic.payload_type_name());
    assert_eq!(staged.payload_type_id(), TypeId::of::<String>());
}
