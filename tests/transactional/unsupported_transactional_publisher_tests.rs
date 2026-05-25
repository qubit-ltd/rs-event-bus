use qubit_event_bus::{
    EventBusError,
    Topic,
    TransactionalPublisher,
    UnsupportedTransactionalPublisher,
};

#[test]
fn test_unsupported_transactional_publisher_rejects_commit_but_allows_rollback() {
    let mut publisher = UnsupportedTransactionalPublisher;

    assert_eq!(
        TransactionalPublisher::commit(&mut publisher).expect_err("unsupported publisher should reject commit"),
        EventBusError::unsupported_operation("transactional_commit")
    );
    assert!(TransactionalPublisher::rollback(&mut publisher).is_ok());
}

#[test]
fn test_unsupported_transactional_publisher_rejects_envelope_publish() {
    let topic = Topic::<String>::try_new("unsupported-publisher").expect("topic should build");
    let mut publisher = UnsupportedTransactionalPublisher::new();

    assert_eq!(
        TransactionalPublisher::publish(&mut publisher, &topic, "payload".to_string())
            .expect_err("unsupported publisher should reject publish"),
        EventBusError::unsupported_operation("transactional_publish")
    );
}
