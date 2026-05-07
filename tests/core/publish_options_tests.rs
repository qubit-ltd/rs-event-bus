use std::sync::{
    Arc,
    Mutex,
};

use qubit_event_bus::{
    EventBusError,
    EventEnvelope,
    LocalEventBus,
    PublishOptions,
    Topic,
};

#[test]
fn test_publish_options_empty_has_no_retry_or_error_handlers() {
    let options = PublishOptions::<String>::empty();

    assert_eq!(options.retry_options(), None);
    assert_eq!(options.error_handler_count(), 0);
}

#[test]
fn test_publish_options_clone_preserves_shared_handlers() {
    let options = PublishOptions::<String>::builder()
        .error_handler(|_, _| ())
        .build();
    let cloned = options.clone();

    assert_eq!(cloned.error_handler_count(), 1);
}

#[test]
fn test_publish_options_converts_error_handler_panic_to_failure() {
    let bus = LocalEventBus::new();
    let topic = Topic::<String>::try_new("publish-options-panic").expect("topic should build");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("observer should register");
    let options = PublishOptions::<String>::builder()
        .error_handler(|_, _| -> qubit_event_bus::EventBusResult<()> {
            panic!("publish handler panic");
        })
        .build();
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    assert_eq!(
        bus.publish_envelope_with_options(
            EventEnvelope::create(topic, "payload".to_string()),
            options,
        )
        .expect_err("stopped bus should reject publish"),
        EventBusError::not_started()
    );
    std::panic::set_hook(previous_hook);

    let observed = observed.lock().expect("observed errors should lock");
    assert!(matches!(
        observed.as_slice(),
        [EventBusError::ErrorHandlerFailed { phase, message }]
            if *phase == "publish" && message.contains("panicked")
    ));
}
