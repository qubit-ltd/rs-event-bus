use qubit_event_bus::{
    RetryDelay,
    RetryJitter,
    RetryOptions,
};

#[test]
fn test_retry_options_new_rejects_zero_attempts() {
    let error = RetryOptions::new(
        0,
        None,
        None,
        RetryDelay::none(),
        RetryJitter::none(),
    )
    .expect_err("zero max_attempts should be rejected");

    assert_eq!(error.path(), qubit_retry::constants::KEY_MAX_ATTEMPTS);
}

#[test]
fn test_retry_options_reexports_rs_retry_defaults() {
    let options = RetryOptions::default();

    assert_eq!(
        options.max_attempts(),
        qubit_retry::constants::DEFAULT_RETRY_MAX_ATTEMPTS
    );
    assert_eq!(options.delay(), &RetryDelay::default());
}
