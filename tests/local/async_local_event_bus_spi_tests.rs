// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Deterministic regression seeds for bounded public local SPI state
//! transitions.

use super::internal::local_state_machine_tests;
use super::internal::weight_budget_contract;

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
    local_state_machine_tests::run(&seed(&[
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
    local_state_machine_tests::run(&seed(&[
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
    local_state_machine_tests::run(&seed(&[
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
    local_state_machine_tests::run(&seed(&[
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
    local_state_machine_tests::run(&seed(&[
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
    local_state_machine_tests::run(&[]);
    local_state_machine_tests::run(&vec![0; 4097]);
}

/// Checks dropping unsettled receivers restores the entire shared budget.
#[test]
fn test_local_state_machine_drop_restores_total_budget_seed() {
    local_state_machine_tests::run(&seed(&[
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

/// Checks receive order only within one ordering lane.
#[test]
fn test_local_state_machine_same_key_fifo_seed() {
    local_state_machine_tests::run(&seed(&[[0, 0, 0], [1, 0, 0], [1, 0, 0], [2, 0, 0], [2, 0, 0]]));
}

/// Checks Retry places the same-key event ahead of its pending successor.
#[test]
fn test_local_state_machine_retry_front_seed() {
    local_state_machine_tests::run(&seed(&[
        [0, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [2, 0, 0],
        [4, 0, 0],
        [2, 0, 0],
    ]));
}

/// Checks interleaved lanes independently preserve their own queue heads.
#[test]
fn test_local_state_machine_independent_lanes_seed() {
    local_state_machine_tests::run(&seed(&[
        [0, 0, 0],
        [1, 0, 0],
        [1, 0, 1],
        [1, 0, 0],
        [1, 0, 1],
        [2, 0, 0],
        [2, 0, 0],
    ]));
}

/// Checks weighted exact capacity and the distinct count-only admission mode.
#[test]
fn test_local_state_machine_weight_exact_and_reject_seed() {
    local_state_machine_tests::run(&seed(&[[0, 0, 0], [1, 0, 28], [1, 0, 12], [1, 0, 0]]));
}

/// Checks each fanout destination consumes its own weight share.
#[test]
fn test_local_state_machine_weighted_partial_fanout_seed() {
    local_state_machine_tests::run(&seed(&[[0, 0, 0], [0, 1, 0], [1, 0, 28]]));
}

/// Checks terminal settlement releases weight for immediate reuse.
#[test]
fn test_local_state_machine_terminal_release_seed() {
    local_state_machine_tests::run(&seed(&[[0, 0, 0], [1, 0, 28], [2, 0, 0], [3, 0, 0], [1, 0, 28]]));
    local_state_machine_tests::run(&seed(&[[0, 0, 0], [1, 0, 28], [2, 0, 0], [5, 0, 0], [1, 0, 28]]));
}

/// Checks repeated, conflicting, and foreign settlements do not release extra
/// budget.
#[test]
fn test_local_state_machine_no_double_release_seed() {
    local_state_machine_tests::run(&seed(&[
        [0, 0, 0],
        [0, 1, 0],
        [1, 0, 28],
        [2, 0, 0],
        [3, 0, 0],
        [3, 0, 0],
        [5, 0, 0],
        [6, 1, 0],
        [1, 0, 28],
    ]));
}

/// Checks close, Drop, and shutdown discard provider-owned pending work.
#[test]
fn test_local_state_machine_cleanup_release_seed() {
    local_state_machine_tests::run(&seed(&[[0, 0, 0], [1, 0, 28], [8, 0, 0], [0, 1, 0], [1, 1, 28]]));
    local_state_machine_tests::run(&seed(&[[0, 0, 0], [1, 0, 28], [9, 0, 0], [0, 0, 0], [1, 0, 28]]));
    local_state_machine_tests::run(&seed(&[[0, 0, 0], [1, 0, 28], [10, 0, 1], [1, 0, 28]]));
}

/// Checks native weight missing weight against this provider.
#[test]
fn test_native_weight_missing_weight() {
    weight_budget_contract::missing_weight(true);
}

/// Checks native weight partial fanout against this provider.
#[test]
fn test_native_weight_partial_fanout() {
    weight_budget_contract::partial_fanout(true);
}

/// Checks native weight settlement against this provider.
#[test]
fn test_native_weight_settlement() {
    weight_budget_contract::settlement(true);
}

/// Checks native weight cleanup against this provider.
#[test]
fn test_native_weight_cleanup() {
    weight_budget_contract::cleanup(true);
}

/// Checks native weight disabled against this provider.
#[test]
fn test_native_weight_disabled() {
    weight_budget_contract::disabled(true);
}
