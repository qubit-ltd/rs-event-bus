use qubit_event_bus::{
    EventBusError,
    coverage_exercise_local_event_bus_defensive_paths,
    coverage_exercise_local_event_bus_inner_defensive_paths,
};

#[test]
fn test_coverage_helpers_exercise_local_defensive_paths() {
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let local_errors = coverage_exercise_local_event_bus_defensive_paths();
    let inner_errors = coverage_exercise_local_event_bus_inner_defensive_paths();
    std::panic::set_hook(previous_hook);

    assert!(
        local_errors
            .iter()
            .any(|error| matches!(error, EventBusError::TypeMismatch { .. }))
    );
    assert!(
        inner_errors
            .iter()
            .any(|error| matches!(error, EventBusError::LockPoisoned { .. }))
    );
    assert!(inner_errors.iter().any(|error| matches!(
        error,
        EventBusError::HandlerFailed { message }
            if message.contains("invoked more than once")
    )));
    assert!(
        inner_errors
            .iter()
            .any(|error| matches!(error, EventBusError::StartFailed { .. }))
    );
}
