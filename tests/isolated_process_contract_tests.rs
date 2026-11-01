// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Contracts for isolated test process selection and cleanup.

mod support;

use std::env::var;
use std::panic::catch_unwind;
use std::time::Duration;

#[test]
fn test_run_case_rejects_a_missing_exact_test_name() {
    let result = catch_unwind(|| {
        support::isolated_process::run_case(
            "this_test_name_does_not_exist",
            "missing-test-contract",
        );
    });

    assert!(result.is_err(), "a missing child test must fail the helper");
}

#[test]
fn test_run_case_executes_the_selected_child_test() {
    if let Ok(case) = var("QUBIT_EVENT_BUS_ISOLATED_CASE") {
        assert_eq!(case, "selected-test-contract");
        return;
    }

    support::isolated_process::run_case(
        "test_run_case_executes_the_selected_child_test",
        "selected-test-contract",
    );
}

#[test]
fn test_run_case_reports_a_failing_child() {
    if var("QUBIT_EVENT_BUS_ISOLATED_CASE").is_ok() {
        panic!("intentional isolated child failure");
    }

    let result = catch_unwind(|| {
        support::isolated_process::run_case(
            "test_run_case_reports_a_failing_child",
            "failing-test-contract",
        );
    });

    assert!(result.is_err(), "a failing child must fail the helper");
}

#[test]
fn test_run_case_with_timeout_reaps_a_stuck_child() {
    if var("QUBIT_EVENT_BUS_ISOLATED_CASE").is_ok() {
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    let result = catch_unwind(|| {
        support::isolated_process::run_case_with_timeout(
            "test_run_case_with_timeout_reaps_a_stuck_child",
            "stuck-test-contract",
            Duration::from_millis(100),
        );
    });

    assert!(result.is_err(), "a stuck child must be killed and reported");
}
