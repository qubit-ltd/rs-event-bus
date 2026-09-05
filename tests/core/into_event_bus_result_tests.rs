use qubit_event_bus::EventBusError;
use qubit_event_bus::EventBusResult;
use qubit_event_bus::IntoEventBusResult;

#[test]
fn test_unit_converts_to_successful_event_bus_result() {
    assert_eq!(().into_event_bus_result(), Ok(()));
}

#[test]
fn test_event_bus_result_conversion_preserves_error() {
    let result: EventBusResult<()> = Err(EventBusError::handler_failed("failed"));

    assert_eq!(
        result.into_event_bus_result(),
        Err(EventBusError::handler_failed("failed"))
    );
}
