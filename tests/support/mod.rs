// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared integration test helpers.

pub(crate) mod fake_spi;
pub(crate) mod manual_async;
#[allow(dead_code)]
mod panic_hook;

#[allow(unused_imports)]
pub(crate) use panic_hook::PanicHookGuard;
