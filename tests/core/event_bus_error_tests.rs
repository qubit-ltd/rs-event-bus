use std::error::Error;
use std::time::Duration;

use qubit_event_bus::EventBusError;

#[test]
fn test_event_bus_error_constructors_preserve_context() {
    assert_eq!(
        EventBusError::invalid_argument("topic", "blank"),
        EventBusError::InvalidArgument {
            field: "topic",
            message: "blank".to_string(),
        }
    );
    assert_eq!(
        EventBusError::missing_field("payload").to_string(),
        "missing required field `payload`"
    );
    assert_eq!(
        EventBusError::dead_letter_failed("boom").kind(),
        "dead_letter_failed"
    );
}

#[test]
fn test_event_bus_error_kind_covers_all_variants() {
    let cases = [
        (EventBusError::not_started(), "not_started"),
        (EventBusError::start_failed("boom"), "start_failed"),
        (
            EventBusError::invalid_argument("field", "bad"),
            "invalid_argument",
        ),
        (EventBusError::missing_field("payload"), "missing_field"),
        (EventBusError::handler_failed("boom"), "handler_failed"),
        (EventBusError::handler_panicked(), "handler_panicked"),
        (
            EventBusError::interceptor_failed("publish", "boom"),
            "interceptor_failed",
        ),
        (
            EventBusError::error_handler_failed("publish", "boom"),
            "error_handler_failed",
        ),
        (
            EventBusError::dead_letter_failed("boom"),
            "dead_letter_failed",
        ),
        (
            EventBusError::execution_rejected("queue full"),
            "execution_rejected",
        ),
        (
            EventBusError::shutdown_timed_out(Duration::from_millis(10)),
            "shutdown_timed_out",
        ),
        (
            EventBusError::lock_poisoned("subscriptions"),
            "lock_poisoned",
        ),
        (
            EventBusError::type_mismatch("String", "i32"),
            "type_mismatch",
        ),
        (
            EventBusError::unsupported_operation("transaction"),
            "unsupported_operation",
        ),
    ];

    for (error, expected_kind) in cases {
        assert_eq!(error.kind(), expected_kind);
    }
}

#[test]
fn test_event_bus_error_display_covers_shutdown_timed_out() {
    assert_eq!(
        EventBusError::shutdown_timed_out(Duration::from_millis(10)).to_string(),
        "event bus shutdown timed out after 10ms"
    );
}

#[test]
fn test_event_bus_error_display_covers_execution_rejected() {
    assert_eq!(
        EventBusError::execution_rejected("queue full").to_string(),
        "event processing task was rejected: queue full"
    );
}

#[test]
fn test_event_bus_error_implements_std_error() {
    let error: &dyn Error = &EventBusError::handler_failed("boom");

    assert_eq!(error.to_string(), "event handler failed: boom");
}
