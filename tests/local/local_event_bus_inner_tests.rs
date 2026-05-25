use std::sync::Arc;
use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};

use qubit_event_bus::{
    EventBusError,
    LocalEventBus,
    RetryDelay,
    RetryJitter,
    RetryOptions,
    SubscribeOptions,
    Topic,
};

#[test]
fn test_local_event_bus_inner_retries_handler_until_success() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = Topic::<String>::try_new("inner-retry").expect("topic should build");
    let attempts = Arc::new(AtomicUsize::new(0));
    let captured_attempts = Arc::clone(&attempts);
    let options = SubscribeOptions::builder()
        .retry_options(
            RetryOptions::new(3, None, None, RetryDelay::none(), RetryJitter::none()).expect("retry should build"),
        )
        .build();

    bus.subscribe_with_options(
        "sub",
        &topic,
        move |_| {
            let attempt = captured_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt < 3 {
                Err(EventBusError::handler_failed("try again"))
            } else {
                Ok(())
            }
        },
        options,
    )
    .expect("subscription should succeed");
    bus.publish(&topic, "payload".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}
