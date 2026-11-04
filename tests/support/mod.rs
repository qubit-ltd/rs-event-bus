// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared integration test helpers.

#[allow(dead_code)]
pub(crate) mod fake_spi;
#[allow(dead_code)]
pub(crate) mod flume_spi;
#[allow(dead_code)]
pub(crate) mod isolated_process;
#[allow(dead_code)]
pub(crate) mod manual_async;
#[allow(dead_code)]
mod panic_hook;
#[cfg(feature = "conformance")]
#[allow(dead_code)]
pub(crate) mod pending_close_shutdown_fixture;
#[allow(dead_code)]
pub(crate) mod provider_shapes;

#[allow(dead_code)]
pub(crate) mod settlement_probe;

#[allow(dead_code)]
pub(crate) mod sync_bus_cleanup;

#[cfg(feature = "conformance")]
#[allow(dead_code)]
pub(crate) mod settlement_conformance;
