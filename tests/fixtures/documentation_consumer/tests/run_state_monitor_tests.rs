// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use event_bus_documentation_consumer::run_state_monitor::monitor_run_state;
use qubit_event_bus::AsyncSubscriptionRunState;

#[test]
fn test_run_state_monitor_alerts_only_when_required() {
    let mut alerts = Vec::new();
    monitor_run_state(AsyncSubscriptionRunState::Unstarted, false, |m| alerts.push(m));
    monitor_run_state(AsyncSubscriptionRunState::Running, true, |m| alerts.push(m));
    assert!(alerts.is_empty());
    monitor_run_state(AsyncSubscriptionRunState::Unstarted, true, |m| alerts.push(m));
    monitor_run_state(AsyncSubscriptionRunState::Paused, true, |m| alerts.push(m));
    monitor_run_state(AsyncSubscriptionRunState::Stopped, true, |m| alerts.push(m));
    assert_eq!(
        alerts,
        vec![
            "subscription runner did not start",
            "subscription runner is paused",
            "subscription runner stopped",
        ]
    );
}
