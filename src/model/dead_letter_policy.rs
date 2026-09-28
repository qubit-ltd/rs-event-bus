// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Destination and acceptance requirement for dead-letter publication.

use super::DeadLetterAdmissionPolicy;
use crate::error::ConfigurationError;

/// Policy used when forwarding a failed delivery to a dead-letter topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterPolicy {
    topic: Box<str>,
    admission: DeadLetterAdmissionPolicy,
}

impl DeadLetterPolicy {
    /// Creates a policy that accepts provider-level transport acknowledgement.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] when `name` is blank,
    /// has surrounding whitespace, or contains a control character.
    pub fn topic(name: &str) -> Result<Self, ConfigurationError> {
        Self::with_admission(name, DeadLetterAdmissionPolicy::TransportAccepted)
    }

    /// Creates a policy that requires a known destination to accept the event.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] when `name` is blank,
    /// has surrounding whitespace, or contains a control character.
    pub fn known_destination(name: &str) -> Result<Self, ConfigurationError> {
        Self::with_admission(name, DeadLetterAdmissionPolicy::KnownDestination)
    }

    /// Creates a policy with an explicit topic and admission requirement.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] when `name` is blank,
    /// has surrounding whitespace, or contains a control character.
    pub fn with_admission(name: &str, admission: DeadLetterAdmissionPolicy) -> Result<Self, ConfigurationError> {
        if name.is_empty() || name.trim() != name || name.chars().any(char::is_control) {
            return Err(ConfigurationError::InvalidField {
                field: "dead_letter",
                message: "topic must be nonblank and contain no controls".into(),
            });
        }
        Ok(Self {
            topic: name.into(),
            admission,
        })
    }

    /// Returns the topic name receiving dead-letter events.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        &self.topic
    }

    /// Returns the evidence required before the source is settled as forwarded.
    #[must_use]
    pub fn admission_policy(&self) -> DeadLetterAdmissionPolicy {
        self.admission
    }
}
