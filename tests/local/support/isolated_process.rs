// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runs a potentially blocking regression case in a bounded child process.

use std::env::current_exe;
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

/// Runs one test case in a child test process with a deadlock watchdog.
///
/// # Parameters
/// - `test_name`: exact test name selected in the child process.
/// - `case`: value passed through `QUBIT_EVENT_BUS_ISOLATED_CASE`.
///
/// # Panics
/// Panics if the child process cannot be started, exits unsuccessfully, or
/// exceeds the default ten-second watchdog timeout.
pub(crate) fn run_case(test_name: &str, case: &str) {
    run_case_with_timeout(test_name, case, Duration::from_secs(10));
}

/// Runs one child test case with the specified deadlock watchdog timeout.
///
/// # Parameters
/// - `test_name`: exact test name selected in the child process.
/// - `case`: value passed through `QUBIT_EVENT_BUS_ISOLATED_CASE`.
/// - `timeout`: maximum child lifetime before it is killed and reaped.
///
/// # Panics
/// Panics if the child process cannot be started or polled, exits
/// unsuccessfully, or exceeds `timeout` and cannot be killed and reaped.
pub(crate) fn run_case_with_timeout(test_name: &str, case: &str, timeout: Duration) {
    let mut child = Command::new(current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env("QUBIT_EVENT_BUS_ISOLATED_CASE", case)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "isolated case {case} failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("isolated case {case} exceeded its deadlock watchdog");
        }
        sleep(Duration::from_millis(5));
    }
}
