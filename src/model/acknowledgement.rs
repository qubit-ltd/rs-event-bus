// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One-way acknowledgement state shared across handler clones.

use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

use super::AcknowledgementError;
use super::AcknowledgementState;

const PENDING: u8 = 0;
const ACKED: u8 = 1;
const NACKED: u8 = 2;

/// A cloneable, atomic acknowledgement handle. Clones share one terminal state.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::Acknowledgement;
///
/// let acknowledgement = Acknowledgement::new();
/// acknowledgement.ack().unwrap();
/// assert!(acknowledgement.is_acked());
/// ```
#[derive(Clone, Debug, Default)]
pub struct Acknowledgement {
    /// Atomic state shared by every clone of this acknowledgement handle.
    state: Arc<AtomicU8>,
}

impl Acknowledgement {
    /// Creates a pending handle.
    ///
    /// # Returns
    /// A handle whose shared state is [`AcknowledgementState::Pending`].
    #[must_use = "use the acknowledgement handle to complete or inspect the outcome"]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    /// Returns the current state from the atomic handle.
    ///
    /// # Returns
    /// The latest observed state, using acquire ordering.
    #[must_use = "Use the returned state."]
    #[inline]
    pub fn state(&self) -> AcknowledgementState {
        match self.state.load(Ordering::Acquire) {
            ACKED => AcknowledgementState::Acknowledged,
            NACKED => AcknowledgementState::NegativelyAcknowledged,
            _ => AcknowledgementState::Pending,
        }
    }
    /// Returns whether an ACK won the first completion race.
    ///
    /// # Returns
    /// `true` if the current state is [`AcknowledgementState::Acknowledged`].
    #[must_use]
    #[inline]
    pub fn is_acked(&self) -> bool {
        self.state() == AcknowledgementState::Acknowledged
    }
    /// Returns whether a NACK won the first completion race.
    ///
    /// # Returns
    /// `true` if the current state is
    /// [`AcknowledgementState::NegativelyAcknowledged`].
    #[must_use]
    #[inline]
    pub fn is_nacked(&self) -> bool {
        self.state() == AcknowledgementState::NegativelyAcknowledged
    }
    /// Returns whether either terminal decision has been made.
    ///
    /// # Returns
    /// `true` unless the current state is [`AcknowledgementState::Pending`].
    #[must_use]
    #[inline]
    pub fn is_completed(&self) -> bool {
        self.state() != AcknowledgementState::Pending
    }

    /// Acknowledges once; repeated ACK succeeds and NACK after ACK conflicts.
    ///
    /// # Returns
    /// `Ok(())` when this handle was already acknowledged or ACK wins the
    /// completion race.
    ///
    /// # Errors
    /// Returns [`AcknowledgementError::AlreadyCompleted`] if a NACK completed
    /// the handle first.
    #[inline]
    pub fn ack(&self) -> Result<(), AcknowledgementError> {
        self.complete(ACKED)
    }

    /// Negatively acknowledges once; repeated NACK succeeds and ACK after NACK
    /// conflicts.
    ///
    /// # Returns
    /// `Ok(())` when this handle was already negatively acknowledged or NACK
    /// wins the completion race.
    ///
    /// # Errors
    /// Returns [`AcknowledgementError::AlreadyCompleted`] if an ACK completed
    /// the handle first.
    #[inline]
    pub fn nack(&self) -> Result<(), AcknowledgementError> {
        self.complete(NACKED)
    }

    /// Atomically completes a pending handle with `decision`.
    ///
    /// # Parameters
    /// - `decision`: terminal ACK or NACK state to store.
    ///
    /// # Returns
    /// `Ok(())` when the decision wins or matches the existing state.
    ///
    /// # Errors
    /// Returns [`AcknowledgementError::AlreadyCompleted`] for a conflicting
    /// terminal decision.
    fn complete(&self, decision: u8) -> Result<(), AcknowledgementError> {
        match self
            .state
            .compare_exchange(PENDING, decision, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(current) if current == decision => Ok(()),
            Err(_) => Err(AcknowledgementError::AlreadyCompleted),
        }
    }
}
