// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Opt-in helpers for reporting provider SPI conformance checks.
//!
//! Provider projects can use [`ConformanceHooks`] to attach checks that need
//! provider-specific fixtures. Missing optional checks are reported as
//! skipped, so callers can distinguish them from successful checks.
//!
//! ```
//! use std::sync::Arc;
//! use qubit_event_bus::spi::EventBusSpi;
//! use qubit_event_bus::spi::conformance::{ConformanceHooks, run_sync};
//!
//! fn verify_provider(factory: impl Fn() -> Arc<dyn EventBusSpi>) {
//!     let report = run_sync(factory, &ConformanceHooks::default());
//!     report.assert_all_passed();
//! }
//! ```

#[path = "async.rs"]
mod async_runner;
mod conformance_case;
mod conformance_hooks;
mod conformance_report;
mod sync;

pub use async_runner::run_async;
pub use conformance_case::ConformanceCase;
pub use conformance_hooks::ConformanceHooks;
pub use conformance_report::ConformanceReport;
pub use sync::run_sync;
