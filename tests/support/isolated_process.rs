// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runs a potentially blocking regression case in a bounded child process.

use std::process::Command;
use std::time::Duration;
use std::time::Instant;

/// Runs one test case in a child test process with a deadlock watchdog.
///
/// # Parameters
/// - `test_name`: exact test name selected in the child process.
/// - `case`: value passed through `QUBIT_EVENT_BUS_ISOLATED_CASE`.
pub(crate) fn run_case(test_name: &str, case: &str) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env("QUBIT_EVENT_BUS_ISOLATED_CASE", case)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
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
        std::thread::sleep(Duration::from_millis(5));
    }
}
