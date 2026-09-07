use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::StdMonotonicClock;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_event_bus::EventBusError;
use qubit_event_bus::EventBusRetryRule;
use qubit_retry::AttemptFailure;
use qubit_retry::BackoffPolicy;
use qubit_retry::Retry;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryErrorReason;
use qubit_retry::RetryInfrastructureFailure;
use qubit_retry::RetryObserver;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

struct RegistrationFailureTimer {
    clock: StdMonotonicClock,
}

impl Timer for RegistrationFailureTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        &self.clock
    }

    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        deadline.validate_domain(self.clock.domain())?;
        Err(TimeError::InstantOverflow)
    }
}

#[test]
fn retry_conversion_timer_failure_keeps_business_error() {
    let retry = Retry::<EventBusError>::builder(
        RetryPolicy::builder()
            .max_attempts(2)
            .backoff(BackoffPolicy::fixed(Duration::from_millis(1)))
            .build()
            .expect("retry policy should build"),
    )
    .rule(|_: &AttemptFailure<EventBusError>, _: &RetryContext| RetryDecision::Retry)
    .build();
    let error = retry
        .sync()
        .timer(Arc::new(RegistrationFailureTimer {
            clock: StdMonotonicClock::new(),
        }))
        .run(|| Err::<(), _>(EventBusError::handler_failed("business")))
        .expect_err("backoff timer registration fails");

    let EventBusError::RetryInfrastructureFailed {
        failure,
        last_failure,
        context,
    } = EventBusError::from(error)
    else {
        panic!("infrastructure terminal must be retained");
    };
    assert!(matches!(failure, RetryInfrastructureFailure::Timer { .. }));
    assert_eq!(context.attempts(), 1);
    assert!(matches!(
        last_failure.as_deref(),
        Some(AttemptFailure::Error(EventBusError::HandlerFailed { message }))
            if message == "business"
    ));
}

#[test]
fn retry_conversion_timer_failure_keeps_structured_terminal() {
    let retry = Retry::<EventBusError>::builder(
        RetryPolicy::builder()
            .max_attempts(2)
            .backoff(BackoffPolicy::fixed(Duration::from_millis(1)))
            .build()
            .expect("retry policy should build"),
    )
    .rule(|_: &AttemptFailure<EventBusError>, _: &RetryContext| RetryDecision::Retry)
    .build();
    let error = retry
        .sync()
        .timer(Arc::new(RegistrationFailureTimer {
            clock: StdMonotonicClock::new(),
        }))
        .run(|| Err::<(), _>(EventBusError::handler_failed("business")))
        .expect_err("backoff timer registration fails");
    assert!(matches!(error.reason(), RetryErrorReason::Infrastructure { .. }));
}

struct CompletionPanic;

impl RetryObserver<EventBusError> for CompletionPanic {
    fn on_terminal_failure(&self, _: &RetryErrorReason, _: &RetryContext) {
        panic!("completion diagnostic");
    }
}

#[test]
fn test_retry_conversion_preserves_completion_diagnostics_and_terminal_rule() {
    for abort in [false, true] {
        let error = Retry::builder(RetryPolicy::builder().max_attempts(1).build().unwrap())
            .observer(CompletionPanic)
            .rule(move |_: &AttemptFailure<EventBusError>, _: &RetryContext| {
                if abort {
                    RetryDecision::Abort
                } else {
                    RetryDecision::Retry
                }
            })
            .build()
            .sync()
            .run(|| Err::<(), _>(EventBusError::handler_failed("business")))
            .unwrap_err();
        let expected = error.completion_callback_failures().to_vec();
        let mapped = EventBusError::from(error);
        assert_eq!(mapped.completion_callback_failures(), expected);
        assert_eq!(
            mapped.retry_completion_source(),
            Some(&EventBusError::handler_failed("business"))
        );
        assert_eq!(
            mapped.source().unwrap().downcast_ref::<EventBusError>(),
            mapped.retry_completion_source()
        );
        assert_eq!(mapped.kind(), "retry_completion_diagnostics");
        assert!(mapped.to_string().contains("business"));
        let cloned = mapped.clone();
        assert_eq!(mapped, cloned);
        let (
            EventBusError::RetryCompletionDiagnostics { context, .. },
            EventBusError::RetryCompletionDiagnostics {
                context: cloned_context,
                ..
            },
        ) = (&mapped, &cloned)
        else {
            panic!("nonempty diagnostics require a wrapper");
        };
        assert!(Arc::ptr_eq(context, cloned_context));
        assert_eq!(context.attempts(), 1);
        assert_eq!(
            EventBusRetryRule.decide(&AttemptFailure::Error(mapped), &RetryContext::new(1, 2)),
            RetryDecision::Abort
        );
    }
}
