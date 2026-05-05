use qubit_event_bus::{
    EventBusError,
    TransactionalEventBus,
    UnsupportedTransactionalEventBus,
};

#[test]
fn test_transactional_event_bus_placeholder_rejects_publisher_creation() {
    let bus = UnsupportedTransactionalEventBus::new();

    assert_eq!(
        TransactionalEventBus::create_transactional_publisher(&bus)
            .expect_err("unsupported bus should reject publisher creation"),
        EventBusError::unsupported_operation("create_transactional_publisher")
    );
}
