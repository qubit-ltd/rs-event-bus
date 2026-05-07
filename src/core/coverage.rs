/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Coverage-only helpers for core modules.

use std::time::Duration;

use super::SubscriptionState;

/// Exercises defensive branches for core private state helpers.
///
/// # Returns
/// Boolean observations proving the exercised paths ran.
pub fn coverage_exercise_core_defensive_paths() -> Vec<bool> {
    let state = SubscriptionState::active();
    let zero_delay_remained_active = state.wait_until_delay_elapsed_or_inactive(Duration::ZERO);
    let elapsed_delay_remained_active =
        state.wait_until_delay_elapsed_or_inactive(Duration::from_millis(1));
    let first_deactivate_changed_state = state.deactivate();
    let second_deactivate_was_idempotent = !state.deactivate();
    let inactive_delay_skipped =
        !state.wait_until_delay_elapsed_or_inactive(Duration::from_millis(1));
    let poisoned_state = SubscriptionState::active();
    poisoned_state.coverage_poison_delay_mutex();
    let poisoned_delay_remained_active =
        poisoned_state.wait_until_delay_elapsed_or_inactive(Duration::from_millis(1));
    let poisoned_deactivate_changed_state = poisoned_state.deactivate();

    vec![
        zero_delay_remained_active,
        elapsed_delay_remained_active,
        first_deactivate_changed_state,
        second_deactivate_was_idempotent,
        inactive_delay_skipped,
        poisoned_delay_remained_active,
        poisoned_deactivate_changed_state,
    ]
}
