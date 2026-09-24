// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! One-way acknowledgement state shared across handler clones.

use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

const PENDING: u8 = 0;
const ACKED: u8 = 1;
const NACKED: u8 = 2;

/// The terminal ACK/NACK decision for one delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AcknowledgementState {
    /// Neither decision has been made.
    Pending,
    /// The handler acknowledged the delivery.
    Acknowledged,
    /// The handler negatively acknowledged the delivery.
    NegativelyAcknowledged,
}

/// A conflicting acknowledgement decision.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AcknowledgementError {
    /// A different terminal decision has already completed this delivery.
    #[error("acknowledgement already completed with a conflicting decision")]
    AlreadyCompleted,
}

/// A cloneable, atomic acknowledgement handle. Clones share one terminal state.
#[derive(Clone, Debug, Default)]
pub struct Acknowledgement {
    state: Arc<AtomicU8>,
}

impl Acknowledgement {
    /// Creates a pending handle.
    pub fn new() -> Self {
        Self::default()
    }
    /// Acknowledges once; repeated ACK succeeds and NACK after ACK conflicts.
    pub fn ack(&self) -> Result<(), AcknowledgementError> {
        self.complete(ACKED)
    }
    /// Negatively acknowledges once; repeated NACK succeeds and ACK after NACK
    /// conflicts.
    pub fn nack(&self) -> Result<(), AcknowledgementError> {
        self.complete(NACKED)
    }
    /// Returns the current state from the atomic handle.
    pub fn state(&self) -> AcknowledgementState {
        match self.state.load(Ordering::Acquire) {
            ACKED => AcknowledgementState::Acknowledged,
            NACKED => AcknowledgementState::NegativelyAcknowledged,
            _ => AcknowledgementState::Pending,
        }
    }
    /// Returns whether an ACK won the first completion race.
    pub fn is_acked(&self) -> bool {
        self.state() == AcknowledgementState::Acknowledged
    }
    /// Returns whether a NACK won the first completion race.
    pub fn is_nacked(&self) -> bool {
        self.state() == AcknowledgementState::NegativelyAcknowledged
    }
    /// Returns whether either terminal decision has been made.
    pub fn is_completed(&self) -> bool {
        self.state() != AcknowledgementState::Pending
    }

    /// Atomically completes a pending handle with `decision`.
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
