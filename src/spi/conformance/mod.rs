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

pub use async_conformance_hooks::AsyncConformanceCheck;
pub use async_conformance_hooks::AsyncConformanceHooks;
pub use async_runner::run_async;
pub use async_runner::run_async_with_profile;
pub use conformance_case::ConformanceCase;
pub use conformance_hooks::ConformanceHooks;
pub use conformance_profile::ConformanceProfile;
pub use conformance_report::ConformanceReport;
pub use conformance_skip_reason::ConformanceSkipReason;
pub use sync::run_sync;
pub use sync::run_sync_with_profile;

mod async_conformance_hooks;
#[path = "async.rs"]
mod async_runner;
mod conformance_case;
mod conformance_hooks;
mod conformance_profile;
mod conformance_report;
mod conformance_skip_reason;
mod sync;
