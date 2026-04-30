/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Coverage-only tests for defensive internal paths.

use qubit_event_bus::coverage_support;

#[test]
fn test_exercise_local_event_bus_paths() {
    let diagnostics = coverage_support::exercise_local_event_bus_paths();

    assert!(
        diagnostics
            .iter()
            .any(|message| message.contains("type mismatch"))
    );
    assert!(diagnostics.len() >= 4);
}
