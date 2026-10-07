// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated event identifier.

use qubit_id::IdGenerationError;
use qubit_id::UuidV4Generator;

use crate::error::ConfigurationError;
use crate::error::EventIdGenerationError;
use crate::util::validated_text::is_nonblank_without_controls;

/// A validated event identifier carried across providers.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::EventId;
///
/// let id = EventId::new("order-event-42").unwrap();
/// assert_eq!(id.as_str(), "order-event-42");
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[must_use]
pub struct EventId(
    /// Owned portable identifier validated at construction or generated as a
    /// UUID.
    Box<str>,
);

impl EventId {
    /// Validates and owns an event identifier.
    ///
    /// Empty identifiers, leading or trailing whitespace, control characters,
    /// and identifiers longer than 128 UTF-8 bytes return
    /// [`ConfigurationError::InvalidEventId`].
    ///
    /// # Parameters
    /// - `value`: text to validate and retain as the event identifier.
    ///
    /// # Returns
    /// An owned event identifier when `value` meets the portable rules.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidEventId`] for empty, oversized,
    /// whitespace-padded, or control-containing values.
    pub fn new(value: impl AsRef<str>) -> Result<Self, ConfigurationError> {
        let value = value.as_ref();
        if !(1..=128).contains(&value.len()) || !is_nonblank_without_controls(value) {
            return Err(ConfigurationError::invalid_event_id(value));
        }
        Ok(Self(value.into()))
    }

    /// Generates a globally portable UUID v4 event identifier.
    ///
    /// # Returns
    /// A validated identifier generated from the operating-system random
    /// source.
    ///
    /// # Errors
    /// Returns [`EventIdGenerationError`] if the operating-system random source
    /// cannot provide the bytes needed by the UUID generator.
    pub fn generate() -> Result<Self, EventIdGenerationError> {
        Self::generate_with(|| {
            UuidV4Generator::new()
                .generate()
                .map(|uuid| uuid.to_string())
        })
    }

    /// Returns the original event identifier.
    ///
    /// # Returns
    /// The validated identifier borrowed for the lifetime of `self`.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Adapts a fallible identifier source to the validated event ID type.
    ///
    /// # Type Parameters
    /// - `F`: one-shot callable type for producing the encoded identifier.
    ///
    /// # Parameters
    /// - `generator`: source that creates the UUID string.
    ///
    /// # Returns
    /// A validated identifier or a wrapper around the generator failure.
    ///
    /// # Errors
    /// Returns [`EventIdGenerationError`] when `generator` fails.
    fn generate_with<F>(generator: F) -> Result<Self, EventIdGenerationError>
    where
        F: FnOnce() -> Result<String, IdGenerationError>,
    {
        let value = generator().map_err(EventIdGenerationError::new)?;
        // UUID v4 has a fixed, valid representation under EventId's portable rules.
        Ok(Self(value.into_boxed_str()))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::EventId;
    use super::IdGenerationError;

    /// Confirms generator failures stay recoverable and preserve their source.
    #[test]
    fn test_generate_with_preserves_generator_failure_source() {
        let result =
            EventId::generate_with(|| Err(IdGenerationError::HostOutOfRange { host: 1, max: 0 }));
        let error = result.expect_err("the injected generator should fail");
        let source = Error::source(&error).expect("wrapper should expose generator error");
        assert!(source.to_string().contains("host id 1 is out of range"));
    }
}
