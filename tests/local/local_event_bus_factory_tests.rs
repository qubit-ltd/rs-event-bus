use std::sync::{Arc, Mutex};

use qubit_event_bus::{LocalEventBusFactory, SubscribeOptions, Topic};

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
