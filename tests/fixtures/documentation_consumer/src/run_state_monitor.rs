// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Compiled run-state monitoring example used by both user guides.

use qubit_event_bus::AsyncSubscriptionRunState;

pub fn monitor_run_state(
    state: AsyncSubscriptionRunState,
    startup_grace_elapsed: bool,
    mut alert: impl FnMut(&'static str),
) {
    match state {
        AsyncSubscriptionRunState::Unstarted if !startup_grace_elapsed => {}
        AsyncSubscriptionRunState::Unstarted => alert("subscription runner did not start"),
        AsyncSubscriptionRunState::Running => {}
        AsyncSubscriptionRunState::Paused => alert("subscription runner is paused"),
        AsyncSubscriptionRunState::Stopped => alert("subscription runner stopped"),
        _ => alert("subscription runner reported an unknown state"),
    }
}
