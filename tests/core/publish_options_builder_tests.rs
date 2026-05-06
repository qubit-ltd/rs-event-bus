use std::time::Duration;

use qubit_event_bus::{PublishOptions, RetryOptions};

#[test]
fn test_publish_options_builder_sets_retry_options() {
    let retry_options =
        RetryOptions::new(4, Duration::from_millis(10)).expect("retry options should build");
    let options = PublishOptions::<String>::builder()
        .retry_options(retry_options)
        .build();

    assert_eq!(options.retry_options(), Some(retry_options));
}

#[test]
fn test_publish_options_builder_adds_multiple_error_handlers() {
    let options = PublishOptions::<String>::builder()
        .error_handler(|_, _| ())
        .error_handler(|_, _| Ok(()))
        .build();

    assert_eq!(options.error_handler_count(), 2);
}
