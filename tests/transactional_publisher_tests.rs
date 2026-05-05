use qubit_event_bus::{
    EventBusError,
    Topic,
    TransactionalPublisher,
    UnsupportedTransactionalPublisher,
};

#[test]
fn test_transactional_publisher_default_publish_builds_envelope_and_delegates() {
    let topic = Topic::<String>::try_new("transactional-publisher").expect("topic should build");
    let mut publisher = UnsupportedTransactionalPublisher::new();

    let error = TransactionalPublisher::publish(&mut publisher, &topic, "payload".to_string())
        .expect_err("unsupported publisher should reject publish");

    assert_eq!(
        error,
        EventBusError::unsupported_operation("transactional_publish")
    );
}
