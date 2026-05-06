use qubit_event_bus::{
    EventBusError,
    EventBusFactory,
    LocalEventBusFactory,
    Topic,
};

#[test]
fn test_event_bus_factory_create_returns_stopped_bus() {
    let factory = LocalEventBusFactory::new();
    let bus = EventBusFactory::create(&factory);
    let topic = Topic::<String>::try_new("factory-stopped").expect("topic should build");

    let error = bus
        .publish(&topic, "payload".to_string())
        .expect_err("factory-created bus should start stopped");

    assert_eq!(error, EventBusError::not_started());
}

#[test]
fn test_event_bus_factory_reports_transactions_unsupported() {
    let factory = LocalEventBusFactory::new();

    assert!(!EventBusFactory::is_transactional_supported(&factory));
    assert_eq!(
        EventBusFactory::create_transactional(&factory)
            .expect_err("local factory should not create transactional bus"),
        EventBusError::unsupported_operation("create_transactional")
    );
}
