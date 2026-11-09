// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Implementation primitives shared by otherwise independent internal modules.

pub(crate) mod finite_wait;
pub(crate) mod panic_payload;
pub(crate) mod sync;
#[cfg(test)]
pub(crate) mod test_wait_probe;
