use qubit_event_bus::{
    LocalEventBus,
    Topic,
};

#[test]
fn test_subscription_exposes_id_topic_options_and_active_state() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = Topic::<String>::try_new("subscription").expect("topic should build");
    let subscription = bus
        .subscribe("sub-1", &topic, |_| ())
        .expect("subscription should be created");

    assert_eq!(subscription.subscriber_id(), "sub-1");
    assert_eq!(subscription.topic(), &topic);
    assert_eq!(subscription.options().priority(), 0);
    assert!(subscription.is_active());

    subscription.cancel().expect("cancel should succeed");
    assert!(!subscription.is_active());
}
