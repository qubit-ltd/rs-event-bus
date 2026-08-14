use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::EventEnvelope;
use qubit_event_bus::LocalEventBusFactory;
use qubit_event_bus::SubscriberInterceptorChain;
use qubit_event_bus::Topic;

#[test]
fn test_subscriber_interceptor_entry_applies_to_matching_payload_type() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_subscriber_interceptor::<String, _>(
            |event: EventEnvelope<String>,
             chain: SubscriberInterceptorChain<String>| {
                chain.proceed(event.with_header("subscriber-entry", "true"))
            },
        )
        .expect("subscriber interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = Topic::<String>::try_new("subscriber-entry")
        .expect("topic should build");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);
    bus.subscribe("sub", &topic, move |event| {
        captured
            .lock()
            .expect("received headers should lock")
            .push(event.headers().get("subscriber-entry").cloned());
    })
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(
        received
            .lock()
            .expect("received headers should lock")
            .as_slice(),
        [Some("true".to_string())]
    );
}
