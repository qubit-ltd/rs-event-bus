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

/// A validated event identifier carried across providers.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[must_use]
pub struct EventId(Box<str>);

impl EventId {
    /// Generates a globally portable UUID v4 event identifier.
    ///
    /// # Errors
    /// Returns [`EventIdGenerationError`] if the operating-system random source
    /// cannot provide the bytes needed by the UUID generator.
    pub fn generate() -> Result<Self, EventIdGenerationError> {
        Self::generate_with(|| UuidV4Generator::new().generate().map(|uuid| uuid.to_string()))
    }

    /// Adapts a fallible identifier source to the validated event ID type.
    fn generate_with<F>(generator: F) -> Result<Self, EventIdGenerationError>
    where
        F: FnOnce() -> Result<String, IdGenerationError>,
    {
        let value = generator().map_err(EventIdGenerationError::new)?;
        // UUID v4 has a fixed, valid representation under EventId's portable rules.
        Ok(Self(value.into_boxed_str()))
    }

    /// Validates and owns an event identifier.
    ///
    /// Empty identifiers, leading or trailing whitespace, control characters,
    /// and identifiers longer than 128 UTF-8 bytes return
    /// [`ConfigurationError::InvalidEventId`].
    pub fn new(value: impl AsRef<str>) -> Result<Self, ConfigurationError> {
        let value = value.as_ref();
        if !(1..=128).contains(&value.len()) || value.trim() != value || value.chars().any(char::is_control) {
            return Err(ConfigurationError::invalid_event_id(value));
        }
        Ok(Self(value.into()))
    }

    /// Returns the original event identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
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
        let result = EventId::generate_with(|| Err(IdGenerationError::HostOutOfRange { host: 1, max: 0 }));
        let error = result.expect_err("the injected generator should fail");
        let source = Error::source(&error).expect("wrapper should expose generator error");
        assert!(source.to_string().contains("host id 1 is out of range"));
    }
}
