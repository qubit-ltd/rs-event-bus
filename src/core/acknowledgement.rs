// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared acknowledgement handle.

use std::sync::Arc;
use std::sync::atomic::{
    AtomicU8,
    Ordering,
};

const PENDING: u8 = 0;
const ACKED: u8 = 1;
const NACKED: u8 = 2;

/// Handle used by subscriber code to acknowledge an event.
///
/// The handle is cheap to clone. All clones share the same completion state, so
/// a handler can move one clone into helper code while tests or error handlers
/// still observe the final ACK/NACK decision.
#[derive(Debug, Clone)]
pub struct Acknowledgement {
    state: Arc<AtomicU8>,
}

impl Acknowledgement {
    /// Creates a new pending acknowledgement handle.
    ///
    /// # Returns
    /// A handle whose state is neither ACKED nor NACKED.
    pub fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(PENDING)),
        }
    }

    /// Marks the event as successfully handled.
    ///
    /// This method has no blocking side effects. If called after `nack`, the
    /// latest call wins because the in-process backend only records state.
    pub fn ack(&self) {
        self.state.store(ACKED, Ordering::SeqCst);
    }

    /// Marks the event as failed.
    ///
    /// This method has no blocking side effects. If called after `ack`, the
    /// latest call wins because the in-process backend only records state.
    pub fn nack(&self) {
        self.state.store(NACKED, Ordering::SeqCst);
    }

    /// Returns whether the event has been ACKED.
    ///
    /// # Returns
    /// `true` if the latest acknowledgement decision is ACK.
    pub fn is_acked(&self) -> bool {
        self.state.load(Ordering::SeqCst) == ACKED
    }

    /// Returns whether the event has been NACKED.
    ///
    /// # Returns
    /// `true` if the latest acknowledgement decision is NACK.
    pub fn is_nacked(&self) -> bool {
        self.state.load(Ordering::SeqCst) == NACKED
    }

    /// Returns whether the event has any terminal acknowledgement state.
    ///
    /// # Returns
    /// `true` after either [`ack`](Self::ack) or [`nack`](Self::nack).
    pub fn is_completed(&self) -> bool {
        self.state.load(Ordering::SeqCst) != PENDING
    }
}

impl Default for Acknowledgement {
    /// Creates a pending acknowledgement handle.
    fn default() -> Self {
        Self::new()
    }
}
