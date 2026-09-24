// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Receipt for one publication attempt.

use super::EventId;
use super::PublishAcknowledgement;
use crate::error::ConfigurationError;

/// A validated provider identifier.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProviderId(Box<str>);

impl ProviderId {
    /// Validates a nonblank provider identifier.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
            return Err(ConfigurationError::InvalidField {
                field: "provider_id",
                message: "must be nonblank and contain no controls".into(),
            });
        }
        Ok(Self(value.into()))
    }
    /// Returns the provider identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Publication admission, not subscriber handler completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishReceipt {
    input_event_id: EventId,
    dispatched_event_id: Option<EventId>,
    provider_id: ProviderId,
    acknowledgement: PublishAcknowledgement,
}

impl PublishReceipt {
    /// Creates a receipt after interception and provider admission.
    pub fn new(
        input_event_id: EventId,
        dispatched_event_id: Option<EventId>,
        provider_id: ProviderId,
        acknowledgement: PublishAcknowledgement,
    ) -> Self {
        Self {
            input_event_id,
            dispatched_event_id,
            provider_id,
            acknowledgement,
        }
    }
    /// Returns the original event ID before publisher interception.
    pub fn input_event_id(&self) -> &EventId {
        &self.input_event_id
    }
    /// Returns the dispatched event ID, or `None` when interception dropped it.
    pub fn dispatched_event_id(&self) -> Option<&EventId> {
        self.dispatched_event_id.as_ref()
    }
    /// Returns the provider that produced the admission result.
    pub fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }
    /// Returns provider admission information; handlers may still be pending.
    pub fn acknowledgement(&self) -> &PublishAcknowledgement {
        &self.acknowledgement
    }
}
