// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../../tests/local/internal/local_state_machine.rs"]
mod local_state_machine;

fuzz_target!(|input: &[u8]| {
    local_state_machine::run(input);
});
