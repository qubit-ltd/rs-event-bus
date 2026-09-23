//! Validated type-erased topic address.

use crate::error::ConfigurationError;

/// Topic identity carried across a type-erased transport boundary.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TopicAddress(Box<str>);

impl TopicAddress {
    /// Creates a topic address after validating its portable name.
    ///
    /// Returns [`ConfigurationError::InvalidField`] for an empty or oversized
    /// name, surrounding whitespace, or control characters.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        if !(1..=255).contains(&value.len())
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(ConfigurationError::InvalidField {
                field: "topic",
                message: "must be 1..=255 bytes without surrounding whitespace or controls".into(),
            });
        }
        Ok(Self(value.into()))
    }

    /// Returns the topic name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
