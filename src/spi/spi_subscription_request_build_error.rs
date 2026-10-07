// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Error returned when a provider request builder omits a required field.

use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;

/// Identifies a required field omitted from a subscription request builder.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::SpiSubscriptionRequest;
///
/// let error = SpiSubscriptionRequest::builder()
///     .build()
///     .err()
///     .expect("all provider request fields are required");
/// assert_eq!(error.missing_field(), "subscription_id");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpiSubscriptionRequestBuildError {
    /// Name of the first required field absent from the builder.
    missing_field: &'static str,
}

impl SpiSubscriptionRequestBuildError {
    /// Creates an error for the first required field absent from the builder.
    #[inline]
    pub(crate) const fn new(missing_field: &'static str) -> Self {
        Self { missing_field }
    }

    /// Returns the name of the first required field absent from the builder.
    #[must_use]
    #[inline]
    pub const fn missing_field(self) -> &'static str {
        self.missing_field
    }
}

impl Display for SpiSubscriptionRequestBuildError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        write!(
            formatter,
            "missing subscription request field `{}`",
            self.missing_field
        )
    }
}

impl Error for SpiSubscriptionRequestBuildError {}
