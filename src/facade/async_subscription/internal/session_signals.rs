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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use super::super::super::async_event_bus::AsyncSignal;
use crate::error::ReceiveError;
use crate::spi::ShutdownMode;

/// Coordinates cancellation and stop escalation for one subscription session.
pub(in crate::facade) struct SessionSignals {
    /// Whether the runner should stop receiving new messages.
    stopped: AtomicBool,
    /// Strongest requested stop mode, with Immediate taking precedence.
    stop_mode: Mutex<Option<ShutdownMode>>,
    /// Terminal asynchronous runner error to return after cleanup.
    terminal_error: Mutex<Option<ReceiveError>>,
    /// Serializes user-work admission against an Immediate stop.
    start_gate: Mutex<()>,
    /// Wakes receive, admission and session waiters after state changes.
    signal: AsyncSignal,
}

impl SessionSignals {
    /// Creates empty stop state for a new subscription session.
    pub(in crate::facade) fn new() -> Arc<Self> {
        Arc::new(Self {
            stopped: false.into(),
            stop_mode: Mutex::new(None),
            terminal_error: Mutex::new(None),
            start_gate: Mutex::new(()),
            signal: AsyncSignal::default(),
        })
    }

    /// Requests a stop, retaining Immediate when modes race.
    pub(in crate::facade) fn stop(&self, mode: ShutdownMode) {
        let _start_gate = self
            .start_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut stop_mode = self.stop_mode.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*stop_mode, Some(ShutdownMode::Immediate)) || mode == ShutdownMode::Immediate {
            *stop_mode = Some(mode);
        }
        drop(stop_mode);
        self.stopped.store(true, Ordering::Release);
        self.signal.notify();
    }

    /// Stores a forwarding failure and requests immediate stop of this source.
    pub(in crate::facade) fn fail_dead_letter_forward(&self, event_id: crate::model::EventId, message: Box<str>) {
        *self
            .terminal_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(ReceiveError::DeadLetterForwardFailed { event_id, message });
        self.stop(ShutdownMode::Immediate);
    }

    /// Takes the terminal error exactly once after the run loop exits.
    pub(in crate::facade) fn take_terminal_error(&self) -> Option<ReceiveError> {
        self.terminal_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// Linearizes the start of user code against an Immediate stop.
    pub(in crate::facade) fn mark_started(&self, started: &AtomicBool) -> bool {
        let _start_gate = self
            .start_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.stopped.load(Ordering::Acquire) && !self.stopping_gracefully() {
            return false;
        }
        started.store(true, Ordering::Release);
        true
    }

    /// Reports whether a graceful stop permits already accepted user work.
    pub(in crate::facade) fn stopping_gracefully(&self) -> bool {
        matches!(
            *self.stop_mode.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
            Some(ShutdownMode::Graceful { .. })
        )
    }

    /// Reads whether stop has been requested.
    pub(in crate::facade) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Borrows the wake registry used by stop-aware asynchronous waits.
    pub(in crate::facade) fn signal(&self) -> &AsyncSignal {
        &self.signal
    }
}
