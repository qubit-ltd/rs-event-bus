use std::sync::{
    Arc,
    Mutex,
};

use qubit_event_bus::{
    EventBus,
    LocalEventBus,
    Topic,
};

#[test]
fn test_event_bus_trait_publish_delegates_to_backend() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = Topic::<String>::try_new("event-bus-trait").expect("topic should build");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    EventBus::subscribe(&bus, "sub", &topic, move |event| {
        captured
            .lock()
            .expect("received payloads should lock")
            .push(event.payload().clone());
    })
    .expect("trait subscribe should succeed");
    EventBus::publish(&bus, &topic, "payload".to_string()).expect("trait publish should succeed");
    EventBus::wait_for_idle(&bus, &topic).expect("topic should become idle");

    assert_eq!(
        received
            .lock()
            .expect("received payloads should lock")
            .as_slice(),
        ["payload"]
    );
}
