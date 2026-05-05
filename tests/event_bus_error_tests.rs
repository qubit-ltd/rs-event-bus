use std::error::Error;

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
}

#[test]
fn test_event_bus_error_implements_std_error() {
    let error: &dyn Error = &EventBusError::handler_failed("boom");

    assert_eq!(error.to_string(), "event handler failed: boom");
}
