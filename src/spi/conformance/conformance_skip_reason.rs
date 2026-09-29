// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Typed explanations for checks that could not run.

/// Typed explanation for a conformance check the runner could not execute.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::conformance::ConformanceSkipReason;
///
/// let reason = ConformanceSkipReason::UnsupportedCapability { capability: "replay" };
/// assert!(reason.to_string().contains("replay"));
/// ```
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
    /// The check does not apply to the provider's API or declared mode.
    NotApplicable {
        /// Why this check is outside the provider contract.
        reason: &'static str,
    },
}

impl std::fmt::Display for ConformanceSkipReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedCapability { capability } => {
                write!(formatter, "unsupported capability: {capability}")
            }
            Self::MissingFixture { detail } => write!(formatter, "missing fixture: {detail}"),
            Self::NotApplicable { reason } => write!(formatter, "not applicable: {reason}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConformanceSkipReason;

    #[test]
    fn skip_reasons_format_each_explanation_without_losing_context() {
        assert_eq!(
            ConformanceSkipReason::UnsupportedCapability { capability: "replay" }.to_string(),
            "unsupported capability: replay"
        );
        assert_eq!(
            ConformanceSkipReason::MissingFixture {
                detail: "receive hook".into()
            }
            .to_string(),
            "missing fixture: receive hook"
        );
        assert_eq!(
            ConformanceSkipReason::NotApplicable { reason: "sync API" }.to_string(),
            "not applicable: sync API"
        );
    }
}
