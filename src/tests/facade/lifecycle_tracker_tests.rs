// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal lifecycle tracker timeout tests.

use std::time::Duration;

use crate::facade::LifecycleTracker;
use crate::facade::WaitOutcome;

#[test]
fn test_wait_for_idle_zero_timeout_checks_completion_first() {
    let tracker = LifecycleTracker::new();
    let delivery = tracker.track_delivery("orders");

    assert_eq!(
        tracker.wait_for_idle("orders", Some(Duration::ZERO)),
        WaitOutcome::TimedOut
    );

    drop(delivery);
    assert_eq!(tracker.wait_for_idle("orders", Some(Duration::ZERO)), WaitOutcome::Idle);
}

#[test]
fn test_wait_for_workers_zero_timeout_checks_completion_first() {
    let tracker = LifecycleTracker::new();
    tracker.worker_started();

    assert!(!tracker.wait_for_workers(Some(Duration::ZERO)));

    tracker.worker_finished();
    assert!(tracker.wait_for_workers(Some(Duration::ZERO)));
}
