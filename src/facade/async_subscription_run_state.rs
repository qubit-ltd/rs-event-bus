// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Observable lifecycle state for caller-driven asynchronous subscriptions.

/// Current lifecycle state of an asynchronous subscription runner.
///
/// The value is an instantaneous snapshot and can change immediately after it
/// is read. It describes the caller-driven runner, not provider queue depth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[repr(u8)]
pub enum AsyncSubscriptionRunState {
    /// The `run` future has not been polled far enough to acquire the session.
    Unstarted = 0,
    /// A caller is currently driving the session through `run`.
    Running = 1,
    /// A previously started `run` future was dropped and can be resumed.
    Paused = 2,
    /// The subscription has ended and cannot process more messages.
    Stopped = 3,
}

impl AsyncSubscriptionRunState {
    /// Decodes the stable atomic representation used by the control object.
    pub(super) fn from_atomic(value: u8) -> Self {
        match value {
            0 => Self::Unstarted,
            1 => Self::Running,
            2 => Self::Paused,
            3 => Self::Stopped,
            _ => unreachable!("invalid async subscription run state"),
        }
    }
}
