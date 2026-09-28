// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Typed explanations for checks that could not run.

/// Typed explanation for a conformance check the runner could not execute.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConformanceSkipReason {
    /// The provider explicitly lacks a capability required only by an optional
    /// check.
    UnsupportedCapability {
        /// The capability the provider does not implement.
        capability: &'static str,
    },
    /// The runner could not execute a required check because setup or a fixture
    /// was missing.
    MissingFixture {
        /// Setup detail needed to make the check executable.
        detail: String,
    },
}

impl std::fmt::Display for ConformanceSkipReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedCapability { capability } => {
                write!(formatter, "unsupported capability: {capability}")
            }
            Self::MissingFixture { detail } => write!(formatter, "missing fixture: {detail}"),
        }
    }
}
