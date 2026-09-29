// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Deterministic regression seeds for bounded public local SPI state
//! transitions.

use super::internal::local_state_machine;

/// Encodes operations into the same fixed-width input consumed by libFuzzer.
fn seed(operations: &[[u8; 3]]) -> Vec<u8> {
    let mut input = Vec::with_capacity(operations.len() * 64);
    for operation in operations {
        input.extend_from_slice(operation);
        input.resize(input.len() + 61, 0);
    }
    input
}

/// Checks outstanding capacity is released by Accept and immediately reusable.
#[test]
fn test_local_state_machine_capacity_release_seed() {
    local_state_machine::run(&seed(&[
        [0, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [2, 0, 0],
        [3, 0, 0],
        [1, 0, 0],
    ]));
}

/// Checks Retry retains capacity and duplicate or conflicting token settlement.
#[test]
fn test_local_state_machine_retry_idempotence_seed() {
    local_state_machine::run(&seed(&[
        [0, 0, 0],
        [1, 0, 0],
        [2, 0, 0],
        [4, 0, 0],
        [4, 0, 0],
        [3, 0, 0],
        [2, 0, 0],
        [3, 0, 1],
        [3, 0, 1],
        [5, 0, 1],
    ]));
}

/// Checks a foreign receiver cannot settle or release another receiver's token.
#[test]
fn test_local_state_machine_foreign_token_seed() {
    local_state_machine::run(&seed(&[
        [0, 0, 0],
        [0, 1, 0],
        [1, 0, 0],
        [2, 0, 0],
        [2, 1, 0],
        [6, 1, 0],
        [3, 0, 0],
        [5, 1, 0],
        [1, 0, 0],
    ]));
}

/// Checks cancelled empty receive, repeated close, receiver Drop, and fresh
/// admission.
#[test]
fn test_local_state_machine_cancel_close_drop_seed() {
    local_state_machine::run(&seed(&[
        [0, 0, 0],
        [7, 0, 0],
        [1, 0, 0],
        [2, 0, 0],
        [8, 0, 0],
        [3, 0, 0],
        [8, 0, 0],
        [2, 0, 0],
        [9, 0, 0],
        [0, 0, 0],
        [1, 0, 0],
        [2, 0, 0],
        [5, 0, 0],
    ]));
}

/// Checks shutdown rejects later admissions and keeps receiver close
/// idempotent.
#[test]
fn test_local_state_machine_shutdown_seed() {
    local_state_machine::run(&seed(&[
        [0, 0, 0],
        [1, 0, 0],
        [10, 0, 1],
        [1, 0, 0],
        [0, 1, 0],
        [2, 0, 0],
        [8, 0, 0],
        [10, 0, 1],
    ]));
}

/// Checks oversized inputs are truncated and an empty operation stream is
/// valid.
#[test]
fn test_local_state_machine_input_bound_seed() {
    local_state_machine::run(&[]);
    local_state_machine::run(&vec![0; 4097]);
}

/// Checks dropping unsettled receivers restores the entire shared budget.
#[test]
fn test_local_state_machine_drop_restores_total_budget_seed() {
    local_state_machine::run(&seed(&[
        [0, 0, 0],
        [0, 1, 0],
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [2, 0, 0],
        [2, 1, 0],
        [9, 0, 0],
        [9, 1, 0],
        [0, 0, 0],
        [0, 1, 0],
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
    ]));
}
