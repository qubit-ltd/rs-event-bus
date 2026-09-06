use std::sync::Arc;
use std::time::Duration;

use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::StdMonotonicClock;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_event_bus::EventBusError;
use qubit_retry::AttemptFailure;
use qubit_retry::BackoffPolicy;
use qubit_retry::Retry;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryFailure;
use qubit_retry::RetryInfrastructureFailure;
use qubit_retry::RetryPolicy;

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
    assert!(matches!(error.failure(), RetryFailure::Infrastructure { .. }));
}
