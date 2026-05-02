/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
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
