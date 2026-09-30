// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Error returned when a capability builder is missing a required dimension.

use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;

/// Identifies a required capability field omitted from a builder.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::EventBusCapabilities;
///
/// let error = EventBusCapabilities::builder()
///     .build()
///     .expect_err("all capability fields are required");
/// assert_eq!(error.missing_field(), "payload_modes");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventBusCapabilitiesBuildError {
    /// Name of the first required field absent from the builder.
    missing_field: &'static str,
}

impl EventBusCapabilitiesBuildError {
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

impl Display for EventBusCapabilitiesBuildError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        write!(formatter, "missing capability field `{}`", self.missing_field)
    }
}

impl Error for EventBusCapabilitiesBuildError {}
