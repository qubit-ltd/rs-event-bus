use std::time::Duration;

use qubit_event_bus::{
    AckMode,
    RetryOptions,
    SubscribeOptions,
};

#[test]
fn test_subscribe_options_builder_sets_ack_retry_and_priority() {
    let retry_options =
        RetryOptions::new(2, Duration::from_millis(1)).expect("retry options should build");
    let options = SubscribeOptions::<String>::builder()
        .ack_mode(AckMode::Manual)
        .retry_options(retry_options)
        .priority(11)
        .build();

    assert_eq!(options.ack_mode(), AckMode::Manual);
    assert_eq!(options.retry_options(), Some(retry_options));
    assert_eq!(options.priority(), 11);
}

#[test]
fn test_subscribe_options_builder_counts_error_handlers() {
    let options = SubscribeOptions::<String>::builder()
        .error_handler(|_, _, _, _| ())
        .error_handler(|_, _, _, _| Ok(()))
        .build();

    assert_eq!(options.error_handler_count(), 2);
}
