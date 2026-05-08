use std::sync::{
    Arc,
    Mutex,
};

use qubit_event_bus::{
    EventEnvelope,
    LocalEventBusFactory,
    Topic,
};

#[test]
fn test_publisher_interceptor_entry_can_enrich_matching_payload_type() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_publisher_interceptor::<String, _>(|event: EventEnvelope<String>| {
            Some(event.with_header("seen", "true"))
        })
        .expect("interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = Topic::<String>::try_new("publisher-interceptor").expect("topic should build");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    bus.subscribe("sub", &topic, move |event| {
        captured
            .lock()
            .expect("received headers should lock")
            .push(event.headers().get("seen").cloned());
    })
    .expect("subscription should succeed");
    bus.publish(&topic, "payload".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(
        received
            .lock()
            .expect("received headers should lock")
            .as_slice(),
        [Some("true".to_string())]
    );
}
