use std::sync::{Arc, Mutex};

use qubit_event_bus::{EventBusError, LocalEventBusFactory, SubscribeOptions, Topic};

#[test]
fn test_local_event_bus_factory_applies_typed_default_subscribe_options() {
    let mut factory = LocalEventBusFactory::new();
    factory.set_default_subscribe_options(
        SubscribeOptions::<String>::builder()
            .filter(|event| event.payload() == "accepted")
            .priority(9)
            .build(),
    );
    let bus = factory.create_started();
    let topic = Topic::<String>::try_new("local-factory").expect("topic should build");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    let subscription = bus
        .subscribe("sub", &topic, move |event| {
            captured
                .lock()
                .expect("received payloads should lock")
                .push(event.payload().clone());
        })
        .expect("subscription should use factory defaults");
    bus.publish(&topic, "rejected".to_string())
        .expect("publish should succeed");
    bus.publish(&topic, "accepted".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(subscription.options().priority(), 9);
    assert_eq!(
        received
            .lock()
            .expect("received payloads should lock")
            .as_slice(),
        ["accepted"]
    );
}

#[test]
fn test_local_event_bus_factory_validates_handler_pool_options() {
    let mut factory = LocalEventBusFactory::new();

    assert_eq!(
        factory
            .set_subscription_handler_pool_size(0)
            .expect_err("zero pool size should be rejected"),
        EventBusError::invalid_argument(
            "pool_size",
            "subscription handler pool size must be greater than zero",
        )
    );
    assert_eq!(
        factory
            .set_subscription_handler_queue_capacity(Some(0))
            .expect_err("zero queue capacity should be rejected"),
        EventBusError::invalid_argument(
            "capacity",
            "subscription handler queue capacity must be greater than zero",
        )
    );
    factory
        .set_subscription_handler_pool_size(1)
        .expect("positive pool size should be accepted");
    factory
        .set_subscription_handler_queue_capacity(Some(1))
        .expect("positive queue capacity should be accepted");
    factory
        .set_subscription_handler_queue_capacity(None)
        .expect("unbounded queue should be accepted");
}
