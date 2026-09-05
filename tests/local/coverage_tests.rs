use qubit_event_bus::EventBusError;
use qubit_event_bus::coverage_exercise_local_event_bus_defensive_paths;
use qubit_event_bus::coverage_exercise_local_event_bus_inner_defensive_paths;
use qubit_event_bus::coverage_exercise_subscriber_interceptor_chain_defensive_paths;

use crate::support::PanicHookGuard;

#[test]
fn test_coverage_helpers_exercise_local_defensive_paths() {
    let (local_errors, inner_errors, chain_errors) = {
        let _panic_hook_guard = PanicHookGuard::suppress();
        (
            coverage_exercise_local_event_bus_defensive_paths(),
            coverage_exercise_local_event_bus_inner_defensive_paths(),
            coverage_exercise_subscriber_interceptor_chain_defensive_paths(),
        )
    };

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
    assert!(
        chain_errors
            .iter()
            .any(|error| matches!(error, EventBusError::UnsupportedOperation { .. }))
    );
}
