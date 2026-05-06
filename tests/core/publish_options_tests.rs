use qubit_event_bus::PublishOptions;

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
