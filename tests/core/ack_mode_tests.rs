// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_event_bus::AckMode;

#[test]
fn test_ack_mode_default_is_auto() {
    assert_eq!(AckMode::default(), AckMode::Auto);
}

#[test]
fn test_ack_mode_manual_is_distinct_from_auto() {
    assert_ne!(AckMode::Manual, AckMode::Auto);
}
