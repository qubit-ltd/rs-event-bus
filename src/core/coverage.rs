// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Coverage-only helpers for core modules.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::SubscriptionState;

fn coverage_noop_cancel() {}

/// Exercises defensive branches for core private state helpers.
///
/// # Returns
/// Boolean observations proving the exercised paths ran.
pub fn coverage_exercise_core_defensive_paths() -> Vec<bool> {
    let state = SubscriptionState::active();
    let zero_delay_remained_active = state.wait_until_delay_elapsed_or_inactive(Duration::ZERO);
    let elapsed_delay_remained_active = state.wait_until_delay_elapsed_or_inactive(Duration::from_millis(1));
    let first_deactivate_changed_state = state.deactivate();
    let second_deactivate_was_idempotent = !state.deactivate();
    let inactive_delay_skipped = !state.wait_until_delay_elapsed_or_inactive(Duration::from_millis(1));
    let poisoned_state = SubscriptionState::active();
    poisoned_state.coverage_poison_delay_mutex();
    let poisoned_delay_remained_active = poisoned_state.wait_until_delay_elapsed_or_inactive(Duration::from_millis(1));
    let poisoned_deactivate_changed_state = poisoned_state.deactivate();

    let cancellation_count = Arc::new(AtomicUsize::new(0));
    let registered_state = SubscriptionState::active();
    coverage_noop_cancel();
    let registration_id = registered_state
        .register_delay_cancellation(coverage_noop_cancel)
        .expect("active subscription should register cancellation");
    registered_state.unregister_delay_cancellation(registration_id);
    let removed_registration_was_not_called = cancellation_count.load(Ordering::SeqCst) == 0;
    let second_count = Arc::clone(&cancellation_count);
    registered_state
        .register_delay_cancellation(move || {
            second_count.fetch_add(1, Ordering::SeqCst);
        })
        .expect("active subscription should register cancellation");
    let registered_deactivate_changed_state = registered_state.deactivate();
    let deactivate_called_registered_callback = cancellation_count.load(Ordering::SeqCst) == 1;

    let inactive_cancellation_count = Arc::new(AtomicUsize::new(0));
    let inactive_state = SubscriptionState::active();
    inactive_state.deactivate();
    let inactive_count = Arc::clone(&inactive_cancellation_count);
    let inactive_registration = inactive_state.register_delay_cancellation(move || {
        inactive_count.fetch_add(1, Ordering::SeqCst);
    });
    let inactive_registration_called_immediately =
        inactive_registration.is_none() && inactive_cancellation_count.load(Ordering::SeqCst) == 1;

    let poisoned_cancellations_state = SubscriptionState::active();
    poisoned_cancellations_state.coverage_poison_delay_cancellations();
    let poisoned_registration = poisoned_cancellations_state.register_delay_cancellation(|| {});
    let poisoned_cancellations_deactivate_changed_state = poisoned_cancellations_state.deactivate();

    vec![
        zero_delay_remained_active,
        elapsed_delay_remained_active,
        first_deactivate_changed_state,
        second_deactivate_was_idempotent,
        inactive_delay_skipped,
        poisoned_delay_remained_active,
        poisoned_deactivate_changed_state,
        removed_registration_was_not_called,
        registered_deactivate_changed_state,
        deactivate_called_registered_callback,
        inactive_registration_called_immediately,
        poisoned_registration.is_some(),
        poisoned_cancellations_deactivate_changed_state,
    ]
}
