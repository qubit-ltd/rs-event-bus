use std::time::Duration;

use qubit_event_bus::{
    EventBus,
    EventBusError,
    EventEnvelope,
    PublishOptions,
    Topic,
    UnsupportedTransactionalEventBus,
};

#[test]
fn test_unsupported_transactional_event_bus_rejects_event_bus_operations() {
    let bus = UnsupportedTransactionalEventBus::new();
    let topic = Topic::<String>::try_new("unsupported-bus")
        .expect("topic should build");

    assert!(
        !EventBus::start(&bus)
            .expect("unsupported bus start should be idempotent")
    );
    assert!(!EventBus::shutdown(&bus));
    assert_eq!(
        EventBus::publish_envelope_with_options(
            &bus,
            EventEnvelope::create(topic.clone(), "payload".to_string()),
            PublishOptions::empty()
        )
        .expect_err("unsupported bus should reject publish"),
        EventBusError::unsupported_operation("publish")
    );
    assert_eq!(
        EventBus::wait_for_idle_timeout(&bus, &topic, Duration::from_millis(1))
            .expect_err("unsupported bus should reject timeout wait"),
        EventBusError::unsupported_operation("wait_for_idle_timeout")
    );
}
