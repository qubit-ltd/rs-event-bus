// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Strictness selection for SPI conformance runs.

/// Determines how missing conformance fixtures affect a report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConformanceProfile {
    /// Lightweight structural smoke checks; missing provider fixtures are
    /// skipped.
    #[default]
    Structural,
    /// Treat every missing required fixture as a conformance failure.
    Strict,
}
