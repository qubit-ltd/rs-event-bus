// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use qubit_event_bus::coverage_exercise_local_event_bus_inner_defensive_paths;

/// Verifies coverage paths exercise processing-task completion accounting.
#[test]
fn test_processing_task_is_exercised_by_local_coverage_paths() {
    let errors = coverage_exercise_local_event_bus_inner_defensive_paths();

    assert!(!errors.is_empty());
}
