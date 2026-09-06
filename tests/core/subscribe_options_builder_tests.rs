use qubit_event_bus::AckMode;
use qubit_event_bus::SubscribeOptions;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryPolicy;

#[test]
fn test_retry_cancellation_token_build_and_clone_share_source() {
    let token = RetryCancellationToken::new();
    let options = SubscribeOptions::<String>::builder()
        .retry_cancellation_token(token.clone())
        .build();
    let cloned = options.clone();
    assert!(
        !options
            .retry_cancellation_token()
            .expect("configured token")
            .is_cancelled()
    );
    assert!(options.retry_options().is_none());
    token.cancel();
    assert!(
        options
            .retry_cancellation_token()
            .expect("configured token")
            .is_cancelled()
    );
    assert!(cloned.retry_cancellation_token().expect("cloned token").is_cancelled());
    assert!(
        SubscribeOptions::<String>::builder()
            .build()
            .retry_cancellation_token()
            .is_none()
    );
}

#[test]
fn test_subscribe_options_builder_sets_ack_retry_and_priority() {
    let retry_options = RetryPolicy::builder().max_attempts(2).build().unwrap();
    let options = SubscribeOptions::<String>::builder()
        .ack_mode(AckMode::Manual)
        .retry_options(retry_options.clone())
        .priority(11)
        .build();

    assert_eq!(options.ack_mode(), AckMode::Manual);
    assert_eq!(options.retry_options(), Some(&retry_options));
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
