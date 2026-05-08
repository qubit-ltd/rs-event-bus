use std::sync::{
    Arc,
    Mutex,
};

use qubit_event_bus::{
    EventEnvelope,
    LocalEventBusFactory,
    SubscriberInterceptorChain,
    Topic,
};

#[test]
fn test_subscriber_interceptor_chain_proceeds_to_handler() {
    let sequence = Arc::new(Mutex::new(Vec::<String>::new()));
    let interceptor_sequence = Arc::clone(&sequence);
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_subscriber_interceptor::<String, _>(
            move |event: EventEnvelope<String>, chain: SubscriberInterceptorChain<String>| {
                interceptor_sequence
                    .lock()
                    .expect("sequence should lock")
                    .push("before".to_string());
                let result = chain.proceed(event.with_header("chain", "seen"));
                interceptor_sequence
                    .lock()
                    .expect("sequence should lock")
                    .push("after".to_string());
                result
            },
        )
        .expect("subscriber interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = Topic::<String>::try_new("subscriber-chain").expect("topic should build");
    let handler_sequence = Arc::clone(&sequence);
    bus.subscribe("sub", &topic, move |event| {
        assert_eq!(event.headers().get("chain"), Some(&"seen".to_string()));
        handler_sequence
            .lock()
            .expect("sequence should lock")
            .push(format!("handler:{}", event.payload()));
    })
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(
        sequence.lock().expect("sequence should lock").as_slice(),
        ["before", "handler:payload", "after"]
    );
}
