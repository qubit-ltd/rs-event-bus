use std::time::Duration;

use qubit_event_bus::{
    EventBusError,
    RetryOptions,
};

#[test]
fn test_retry_options_new_rejects_zero_attempts() {
    let error =
        RetryOptions::new(0, Duration::ZERO).expect_err("zero max_attempts should be rejected");

    assert_eq!(
        error,
        EventBusError::invalid_argument(
            "max_attempts",
            "retry max_attempts must be greater than zero"
        )
    );
}

#[test]
fn test_retry_options_default_runs_single_immediate_attempt() {
    let options = RetryOptions::default();

    assert_eq!(options.max_attempts(), 1);
    assert_eq!(options.delay(), Duration::ZERO);
}
