/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Subscriber interceptor chain handle.
// qubit-style: allow multiple-public-types
// qubit-style: allow coverage-cfg

use std::panic::{
    self,
    AssertUnwindSafe,
};
use std::sync::{
    Arc,
    Mutex,
};
use std::time::Duration;

#[cfg(coverage)]
use crate::Topic;
use crate::{
    EventBusError,
    EventBusResult,
    EventEnvelope,
};

type HandlerFn<T> = dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static;
pub(crate) type DownstreamErrorSlot = Arc<Mutex<Vec<DownstreamErrorRecord>>>;

/// Recorded downstream failure provenance for one interceptor invocation.
pub(crate) struct DownstreamErrorRecord {
    fingerprint: ErrorFingerprint,
}

/// Identity of a downstream error value returned from `proceed`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ErrorFingerprint {
    NotStarted,
    StartFailed(OwnedStringFingerprint),
    InvalidArgument {
        field: &'static str,
        message: OwnedStringFingerprint,
    },
    MissingField {
        field: &'static str,
    },
    HandlerFailed(OwnedStringFingerprint),
    HandlerPanicked,
    InterceptorFailed {
        phase: &'static str,
        message: OwnedStringFingerprint,
    },
    ErrorHandlerFailed {
        phase: &'static str,
        message: OwnedStringFingerprint,
    },
    DeadLetterFailed(OwnedStringFingerprint),
    ExecutionRejected(OwnedStringFingerprint),
    ShutdownTimedOut {
        timeout: Duration,
    },
    LockPoisoned {
        resource: &'static str,
    },
    TypeMismatch {
        expected: &'static str,
        actual: &'static str,
    },
    UnsupportedOperation {
        operation: &'static str,
    },
}

/// Allocation identity for owned string payloads inside an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OwnedStringFingerprint {
    ptr: usize,
    len: usize,
}

/// Chain handle passed to subscriber interceptors.
///
/// Calling [`proceed`](Self::proceed) invokes the next subscriber interceptor,
/// or the original subscriber handler when the current interceptor is the last
/// one in the chain.
///
/// The chain is a one-shot continuation. Calling `proceed` consumes the chain,
/// so an interceptor cannot accidentally invoke downstream processing twice.
///
/// ```compile_fail
/// # use qubit_event_bus::{
/// #     EventBusResult,
/// #     EventEnvelope,
/// #     SubscriberInterceptorChain,
/// # };
/// # fn interceptor(
/// #     event: EventEnvelope<String>,
/// #     chain: SubscriberInterceptorChain<String>,
/// # ) -> EventBusResult<()> {
/// let first = chain.proceed(event.clone());
/// let second = chain.proceed(event);
/// # first?;
/// # second
/// # }
/// ```
pub struct SubscriberInterceptorChain<T: Clone + Send + Sync + 'static> {
    next: Arc<HandlerFn<T>>,
    downstream_error: DownstreamErrorSlot,
}

/// Chain handle passed to global subscriber interceptors.
///
/// Calling [`proceed`](Self::proceed) invokes the next global interceptor,
/// typed interceptor, or original subscriber handler.
///
/// The chain is a one-shot continuation. Calling `proceed` consumes the chain,
/// so an interceptor cannot accidentally invoke downstream processing twice.
///
/// ```compile_fail
/// # use qubit_event_bus::{
/// #     EventBusResult,
/// #     SubscriberInterceptorAnyChain,
/// # };
/// # fn interceptor(chain: SubscriberInterceptorAnyChain) -> EventBusResult<()> {
/// let first = chain.proceed();
/// let second = chain.proceed();
/// # first?;
/// # second
/// # }
/// ```
pub struct SubscriberInterceptorAnyChain {
    next: Arc<dyn Fn() -> EventBusResult<()> + Send + Sync + 'static>,
    downstream_error: DownstreamErrorSlot,
}

impl SubscriberInterceptorAnyChain {
    /// Creates a chain handle sharing downstream error state with the wrapper.
    ///
    /// # Parameters
    /// - `next`: Handler or interceptor wrapper to invoke next.
    /// - `downstream_error`: Shared slot recording downstream failures.
    ///
    /// # Returns
    /// Chain handle for one global interceptor invocation.
    pub(crate) fn with_downstream_error(
        next: Arc<dyn Fn() -> EventBusResult<()> + Send + Sync + 'static>,
        downstream_error: DownstreamErrorSlot,
    ) -> Self {
        Self { next, downstream_error }
    }

    /// Continues subscriber processing, consuming this one-shot chain handle.
    ///
    /// # Returns
    /// `Ok(())` when downstream processing succeeds.
    ///
    /// # Errors
    /// Returns the downstream handler or interceptor error. If the downstream
    /// handler panics, the panic is isolated and returned as
    /// [`EventBusError::HandlerPanicked`].
    pub fn proceed(self) -> EventBusResult<()> {
        match panic::catch_unwind(AssertUnwindSafe(|| (self.next)())) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                record_downstream_error(&self.downstream_error, &error);
                Err(error)
            }
            Err(_) => {
                let error = EventBusError::handler_panicked();
                record_downstream_error(&self.downstream_error, &error);
                Err(error)
            }
        }
    }
}

impl<T> SubscriberInterceptorChain<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Creates a chain handle sharing downstream error state with the wrapper.
    ///
    /// # Parameters
    /// - `next`: Handler or interceptor wrapper to invoke next.
    /// - `downstream_error`: Shared slot recording downstream failures.
    ///
    /// # Returns
    /// Chain handle for one interceptor invocation.
    pub(crate) fn with_downstream_error(
        next: Arc<dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static>,
        downstream_error: DownstreamErrorSlot,
    ) -> Self {
        Self { next, downstream_error }
    }

    /// Continues subscriber processing, consuming this one-shot chain handle.
    ///
    /// # Parameters
    /// - `envelope`: Envelope to pass to the next chain element.
    ///
    /// # Returns
    /// `Ok(())` when downstream processing succeeds.
    ///
    /// # Errors
    /// Returns the downstream handler or interceptor error. If the downstream
    /// handler panics, the panic is isolated and returned as
    /// [`EventBusError::HandlerPanicked`].
    pub fn proceed(self, envelope: EventEnvelope<T>) -> EventBusResult<()> {
        match panic::catch_unwind(AssertUnwindSafe(|| (self.next)(envelope))) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                record_downstream_error(&self.downstream_error, &error);
                Err(error)
            }
            Err(_) => {
                let error = EventBusError::handler_panicked();
                record_downstream_error(&self.downstream_error, &error);
                Err(error)
            }
        }
    }
}

/// Creates shared state for one interceptor invocation.
///
/// # Returns
/// Empty downstream error slot.
pub(crate) fn create_downstream_error_slot() -> DownstreamErrorSlot {
    Arc::new(Mutex::new(Vec::new()))
}

/// Returns whether an error came from the downstream chain.
///
/// # Parameters
/// - `downstream_error`: Slot filled by [`SubscriberInterceptorChain::proceed`]
///   or [`SubscriberInterceptorAnyChain::proceed`].
/// - `error`: Error returned by an interceptor.
///
/// # Returns
/// `true` when `error` has the same provenance as a recorded downstream failure.
pub(crate) fn is_recorded_downstream_error(downstream_error: &DownstreamErrorSlot, error: &EventBusError) -> bool {
    let fingerprint = ErrorFingerprint::from_error(error);
    downstream_error
        .lock()
        .map(|recorded| recorded.iter().any(|record| record.fingerprint == fingerprint))
        .unwrap_or(false)
}

/// Records a downstream failure observed by a chain handle.
///
/// # Parameters
/// - `downstream_error`: Shared error slot.
/// - `error`: Downstream failure to record.
fn record_downstream_error(downstream_error: &DownstreamErrorSlot, error: &EventBusError) {
    let record = DownstreamErrorRecord::new(error);
    if let Ok(mut recorded) = downstream_error.lock()
        && !recorded
            .iter()
            .any(|existing| existing.fingerprint == record.fingerprint)
    {
        recorded.push(record);
    }
}

impl DownstreamErrorRecord {
    /// Creates a downstream provenance record from an error value.
    ///
    /// # Parameters
    /// - `error`: Error returned by downstream processing.
    ///
    /// # Returns
    /// Record used to recognize the same downstream error if it is propagated.
    fn new(error: &EventBusError) -> Self {
        Self {
            fingerprint: ErrorFingerprint::from_error(error),
        }
    }
}

impl ErrorFingerprint {
    /// Captures the internal identity of an event bus error.
    ///
    /// # Parameters
    /// - `error`: Error value to fingerprint.
    ///
    /// # Returns
    /// Fingerprint that preserves allocation provenance for owned messages.
    fn from_error(error: &EventBusError) -> Self {
        match error {
            EventBusError::NotStarted => Self::NotStarted,
            EventBusError::StartFailed { message } => Self::StartFailed(OwnedStringFingerprint::new(message)),
            EventBusError::InvalidArgument { field, message } => Self::InvalidArgument {
                field,
                message: OwnedStringFingerprint::new(message),
            },
            EventBusError::MissingField { field } => Self::MissingField { field },
            EventBusError::HandlerFailed { message } => Self::HandlerFailed(OwnedStringFingerprint::new(message)),
            EventBusError::HandlerPanicked => Self::HandlerPanicked,
            EventBusError::InterceptorFailed { phase, message } => Self::InterceptorFailed {
                phase,
                message: OwnedStringFingerprint::new(message),
            },
            EventBusError::ErrorHandlerFailed { phase, message } => Self::ErrorHandlerFailed {
                phase,
                message: OwnedStringFingerprint::new(message),
            },
            EventBusError::DeadLetterFailed { message } => Self::DeadLetterFailed(OwnedStringFingerprint::new(message)),
            EventBusError::ExecutionRejected { message } => {
                Self::ExecutionRejected(OwnedStringFingerprint::new(message))
            }
            EventBusError::ShutdownTimedOut { timeout } => Self::ShutdownTimedOut { timeout: *timeout },
            EventBusError::LockPoisoned { resource } => Self::LockPoisoned { resource },
            EventBusError::TypeMismatch { expected, actual } => Self::TypeMismatch { expected, actual },
            EventBusError::UnsupportedOperation { operation } => Self::UnsupportedOperation { operation },
        }
    }
}

impl OwnedStringFingerprint {
    /// Captures the allocation identity of a string slice.
    ///
    /// # Parameters
    /// - `message`: Owned string contents stored in an error.
    ///
    /// # Returns
    /// Pointer-and-length identity for the current allocation.
    fn new(message: &str) -> Self {
        Self {
            ptr: message.as_ptr() as usize,
            len: message.len(),
        }
    }
}

/// Exercises coverage-only defensive branches for subscriber interceptor chains.
///
/// # Returns
/// Errors observed while exercising downstream provenance recording.
#[cfg(coverage)]
pub fn coverage_exercise_subscriber_interceptor_chain_defensive_paths() -> Vec<EventBusError> {
    let mut errors = Vec::new();
    let empty_slot = create_downstream_error_slot();
    assert!(!is_recorded_downstream_error(
        &empty_slot,
        &EventBusError::not_started()
    ));

    let any_ok =
        SubscriberInterceptorAnyChain::with_downstream_error(Arc::new(|| Ok(())), create_downstream_error_slot());
    assert!(any_ok.proceed().is_ok());

    let any_error_slot = create_downstream_error_slot();
    let any_error = SubscriberInterceptorAnyChain::with_downstream_error(
        Arc::new(|| Err(EventBusError::start_failed("coverage any failed"))),
        Arc::clone(&any_error_slot),
    )
    .proceed()
    .expect_err("coverage any chain should fail");
    assert!(is_recorded_downstream_error(&any_error_slot, &any_error));
    errors.push(any_error);

    let any_panic_slot = create_downstream_error_slot();
    let any_panic = SubscriberInterceptorAnyChain::with_downstream_error(
        Arc::new(|| panic!("coverage any panic")),
        Arc::clone(&any_panic_slot),
    )
    .proceed()
    .expect_err("coverage any chain should report panic");
    assert!(is_recorded_downstream_error(&any_panic_slot, &any_panic));
    errors.push(any_panic);

    let topic = Topic::<String>::try_new("coverage-subscriber-chain").expect("coverage topic should build");
    let typed_ok = SubscriberInterceptorChain::with_downstream_error(
        Arc::new(|_event: EventEnvelope<String>| Ok(())),
        create_downstream_error_slot(),
    );
    assert!(
        typed_ok
            .proceed(EventEnvelope::create(topic.clone(), "ok".to_string()))
            .is_ok()
    );

    let typed_error_slot = create_downstream_error_slot();
    let typed_error = SubscriberInterceptorChain::with_downstream_error(
        Arc::new(|_event: EventEnvelope<String>| Err(EventBusError::handler_failed("coverage typed failed"))),
        Arc::clone(&typed_error_slot),
    )
    .proceed(EventEnvelope::create(topic.clone(), "typed-error".to_string()))
    .expect_err("coverage typed chain should fail");
    assert!(is_recorded_downstream_error(&typed_error_slot, &typed_error));
    errors.push(typed_error);

    let typed_panic_slot = create_downstream_error_slot();
    let typed_panic = SubscriberInterceptorChain::with_downstream_error(
        Arc::new(|_event: EventEnvelope<String>| panic!("coverage typed panic")),
        Arc::clone(&typed_panic_slot),
    )
    .proceed(EventEnvelope::create(topic, "typed-panic".to_string()))
    .expect_err("coverage typed chain should report panic");
    assert!(is_recorded_downstream_error(&typed_panic_slot, &typed_panic));
    errors.push(typed_panic);

    let direct_slot = create_downstream_error_slot();
    let direct_errors = vec![
        EventBusError::not_started(),
        EventBusError::invalid_argument("field", "invalid"),
        EventBusError::missing_field("field"),
        EventBusError::interceptor_failed("phase", "interceptor"),
        EventBusError::error_handler_failed("phase", "handler"),
        EventBusError::dead_letter_failed("dead letter"),
        EventBusError::execution_rejected("execution"),
        EventBusError::shutdown_timed_out(Duration::from_millis(1)),
        EventBusError::lock_poisoned("lock"),
        EventBusError::type_mismatch("expected", "actual"),
        EventBusError::unsupported_operation("operation"),
    ];
    for error in direct_errors {
        record_downstream_error(&direct_slot, &error);
        record_downstream_error(&direct_slot, &error);
        assert!(is_recorded_downstream_error(&direct_slot, &error));
        errors.push(error);
    }
    errors
}
