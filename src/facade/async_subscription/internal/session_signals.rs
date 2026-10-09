// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Stop, terminal-error and start-gate state shared by a subscription session.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::error::ReceiveError;
use crate::facade::async_event_bus::AsyncSignal;
use crate::model::EventId;
use crate::model::SubscriptionStopReason;
use crate::spi::ShutdownMode;

/// Coordinates cancellation and stop escalation for one subscription session.
pub(in crate::facade) struct SessionSignals {
    /// Whether the runner should stop receiving new messages.
    stopped: AtomicBool,
    /// Strongest requested stop mode, with Immediate taking precedence.
    stop_mode: Mutex<Option<ShutdownMode>>,
    /// Terminal asynchronous runner error to return after cleanup.
    terminal_error: Mutex<Option<ReceiveError>>,
    /// Immutable first receive failure, separate from consumable runner errors.
    terminal_failure: Mutex<Option<Arc<SubscriptionStopReason>>>,
    /// Serializes user-work admission against an Immediate stop.
    start_gate: Mutex<()>,
    /// Wakes receive, admission and session waiters after state changes.
    signal: AsyncSignal,
}

impl SessionSignals {
    /// Creates empty stop state for a new subscription session.
    ///
    /// # Returns
    ///
    /// A reference-counted signal coordinator with no stop request recorded.
    #[must_use = "the session signal coordinator must be retained"]
    pub(in crate::facade) fn new() -> Arc<Self> {
        Arc::new(Self {
            stopped: false.into(),
            stop_mode: Mutex::new(None),
            terminal_error: Mutex::new(None),
            terminal_failure: Mutex::new(None),
            start_gate: Mutex::new(()),
            signal: AsyncSignal::default(),
        })
    }

    /// Returns the first receive cause, or None while receiving is healthy.
    pub(in crate::facade) fn terminal_failure(&self) -> Option<Arc<SubscriptionStopReason>> {
        self.terminal_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Reports whether a graceful stop permits already accepted user work.
    ///
    /// # Returns
    ///
    /// `true` when the active stop mode is graceful; otherwise, `false`.
    #[must_use = "Use the returned query result."]
    pub(in crate::facade) fn stopping_gracefully(&self) -> bool {
        matches!(
            *self.stop_mode.lock().unwrap_or_else(PoisonError::into_inner),
            Some(ShutdownMode::Graceful { .. })
        )
    }

    /// Reads whether stop has been requested.
    ///
    /// # Returns
    ///
    /// `true` after a stop request; otherwise, `false`.
    #[must_use]
    #[inline]
    pub(in crate::facade) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Borrows the wake registry used by stop-aware asynchronous waits.
    ///
    /// # Returns
    ///
    /// The session's asynchronous wake registry.
    #[must_use = "Use the returned signal."]
    #[inline]
    pub(in crate::facade) fn signal(&self) -> &AsyncSignal {
        &self.signal
    }

    /// Returns the retained receive cause, or takes a one-shot delivery error.
    ///
    /// # Returns
    ///
    /// Receive stops return the same Arc on every call. Other terminal errors
    /// are taken once; None means no cause remains to report.
    pub(in crate::facade) fn take_terminal_error(&self) -> Option<ReceiveError> {
        if let Some(reason) = self.terminal_failure() {
            return Some(ReceiveError::Stopped(reason));
        }
        self.terminal_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// Requests a stop, retaining Immediate when modes race.
    ///
    /// # Parameters
    ///
    /// - `mode`: Stop mode requested by the caller.
    ///
    /// # Side Effects
    ///
    /// Records the strongest requested mode, marks the session stopped, and
    /// wakes waiters.
    pub(in crate::facade) fn stop(&self, mode: ShutdownMode) {
        let _start_gate = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        let mut stop_mode = self.stop_mode.lock().unwrap_or_else(PoisonError::into_inner);
        if !matches!(*stop_mode, Some(ShutdownMode::Immediate)) || mode == ShutdownMode::Immediate {
            *stop_mode = Some(mode);
        }
        drop(stop_mode);
        self.stopped.store(true, Ordering::Release);
        self.signal.notify();
    }

    /// Caches the first receive failure and stops unstarted user work.
    ///
    /// # Returns
    ///
    /// `true` if this call records the first cause; otherwise, `false`.
    #[must_use]
    pub(in crate::facade) fn fail_receive(&self, reason: SubscriptionStopReason) -> bool {
        let _start_gate = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        let mut stored = self.terminal_failure.lock().unwrap_or_else(PoisonError::into_inner);
        let first = stored.is_none();
        if first {
            *stored = Some(Arc::new(reason));
        }
        drop(stored);
        *self.stop_mode.lock().unwrap_or_else(PoisonError::into_inner) = Some(ShutdownMode::Immediate);
        self.stopped.store(true, Ordering::Release);
        self.signal.notify();
        first
    }

    /// Stores a forwarding failure and requests immediate stop of this source.
    ///
    /// # Parameters
    ///
    /// - `event_id`: Identity of the event that could not be forwarded.
    /// - `message`: Description of the forwarding failure.
    ///
    /// # Side Effects
    ///
    /// Stores a terminal receive error, requests an immediate stop, and wakes
    /// waiters.
    pub(in crate::facade) fn fail_dead_letter_forward(&self, event_id: EventId, message: Box<str>) {
        *self.terminal_error.lock().unwrap_or_else(PoisonError::into_inner) =
            Some(ReceiveError::DeadLetterForwardFailed { event_id, message });
        self.stop(ShutdownMode::Immediate);
    }

    /// Linearizes the start of user code against an Immediate stop.
    ///
    /// # Parameters
    ///
    /// - `started`: Flag set when user work is allowed to begin.
    ///
    /// # Returns
    ///
    /// `true` if work may start; otherwise, `false`.
    ///
    /// # Side Effects
    ///
    /// Sets `started` to `true` when the start is admitted.
    #[must_use]
    pub(in crate::facade) fn mark_started(&self, started: &AtomicBool) -> bool {
        let _start_gate = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        if self.stopped.load(Ordering::Acquire) && !self.stopping_gracefully() {
            return false;
        }
        started.store(true, Ordering::Release);
        true
    }

    /// Admits one actual handler factory invocation against the stop boundary.
    ///
    /// # Returns
    ///
    /// True when this invocation linearizes before Immediate or terminal stop,
    /// or while Graceful permits accepted deliveries to drain. The gate is
    /// released before invoking user code; admission never covers later
    /// retries.
    #[must_use = "Only admitted handler invocations may enter user code."]
    pub(in crate::facade::async_subscription) fn admit_handler(&self) -> bool {
        let _start_gate = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        !self.stopped.load(Ordering::Acquire) || self.stopping_gracefully()
    }
}
