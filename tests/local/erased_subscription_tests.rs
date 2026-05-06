use std::sync::{Arc, Mutex};

use qubit_event_bus::{LocalEventBus, Topic};

#[test]
fn test_subscription_cancel_removes_type_erased_entry() {
    let bus = LocalEventBus::started();
    let topic = Topic::<String>::try_new("erased-subscription").expect("topic should build");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);
    let subscription = bus
        .subscribe("sub", &topic, move |event| {
            captured
                .lock()
                .expect("received payloads should lock")
                .push(event.payload().clone());
        })
        .expect("subscription should be created");

    subscription.cancel().expect("cancel should succeed");
    bus.publish(&topic, "ignored".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert!(!subscription.is_active());
    assert!(
        received
            .lock()
            .expect("received payloads should lock")
            .is_empty()
    );
}
