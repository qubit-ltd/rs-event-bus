use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use qubit_event_bus::EventBusError;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryCallbackFailure;
use qubit_retry::RetryCallbackKind;
use qubit_retry::RetryCallbackPhase;
use qubit_retry::RetryCancellationPhase;
use qubit_retry::RetryContext;
use qubit_retry::RetryInfrastructureFailure;
use qubit_retry::RetryPanic;
use qubit_retry::RetryTimeoutScope;

fn assert_retry_error_contract(
    error: EventBusError,
    expected_kind: &str,
    expected_display: &str,
) {
    assert_eq!(error.kind(), expected_kind);
    assert_eq!(error.to_string(), expected_display);
    assert!(error.source().is_none());
}

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
        EventBusError::shutdown_timed_out(Duration::from_millis(10))
            .to_string(),
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

#[test]
fn test_retry_timed_out_public_contract() {
    assert_retry_error_contract(
        EventBusError::RetryTimedOut {
            scope: RetryTimeoutScope::Attempt,
            last_failure: Some(Box::new(AttemptFailure::TimedOut {
                scope: RetryTimeoutScope::Attempt,
            })),
            context: Arc::new(RetryContext::new(1, 2)),
        },
        "retry_timed_out",
        "event-bus retry timed out at attempt scope after 1 attempt(s); last attempt failed: attempt timed out (attempt)",
    );
}

#[test]
fn test_retry_cancelled_public_contract() {
    assert_retry_error_contract(
        EventBusError::RetryCancelled {
            phase: RetryCancellationPhase::Backoff,
            last_failure: Some(Box::new(AttemptFailure::Panicked {
                panic: RetryPanic::StaticStr("cancelled operation panic"),
            })),
            context: Arc::new(RetryContext::new(1, 2)),
        },
        "retry_cancelled",
        "event-bus retry was cancelled during backoff after 1 attempt(s); last attempt failed: attempt panicked: cancelled operation panic",
    );
}

#[test]
fn test_retry_callback_failed_public_contract() {
    assert_retry_error_contract(
        EventBusError::RetryCallbackFailed {
            callback: RetryCallbackFailure::new(
                RetryCallbackKind::Observer,
                0,
                RetryCallbackPhase::AttemptFailed,
                RetryPanic::StaticStr("observer panic"),
            ),
            last_failure: Some(Box::new(AttemptFailure::Panicked {
                panic: RetryPanic::StaticStr("operation panic"),
            })),
            context: Arc::new(RetryContext::new(1, 2)),
        },
        "retry_callback_failed",
        "event-bus retry callback failed (observer callback 0 panicked during attempt failed: observer panic) after 1 attempt(s); last attempt failed: attempt panicked: operation panic",
    );
}

#[test]
fn test_retry_infrastructure_failed_public_contract() {
    assert_retry_error_contract(
        EventBusError::RetryInfrastructureFailed {
            failure: RetryInfrastructureFailure::WorkerSpawn {
                message: "worker offline".into(),
            },
            last_failure: None,
            context: Arc::new(RetryContext::new(0, 2)),
        },
        "retry_infrastructure_failed",
        "event-bus retry infrastructure failed (worker spawn failed: worker offline) after 0 attempt(s)",
    );
}

#[test]
fn test_retry_error_clone_preserves_context_lineage() {
    let error = EventBusError::RetryCancelled {
        phase: RetryCancellationPhase::BeforeAttempt,
        last_failure: None,
        context: Arc::new(RetryContext::new(0, 1)),
    };
    let cloned = error.clone();

    let (
        EventBusError::RetryCancelled {
            context: original_context,
            ..
        },
        EventBusError::RetryCancelled {
            context: cloned_context,
            ..
        },
    ) = (&error, &cloned)
    else {
        panic!("the cloned error should retain its retry variant");
    };
    assert!(Arc::ptr_eq(original_context, cloned_context));
}
