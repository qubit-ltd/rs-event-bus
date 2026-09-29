// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Destination and acceptance requirement for dead-letter publication.

use super::DeadLetterAdmissionPolicy;
use super::Topic;
use crate::error::ConfigurationError;
use crate::util::validated_text::is_nonblank_without_controls;

/// Policy used when forwarding a failed delivery to a dead-letter topic.
///
/// A policy pairs the destination topic with the admission evidence the
/// facade must observe before it settles the original delivery as forwarded.
/// The topic name is validated once at construction, so a stored policy is
/// always publishable without further checks. Values are immutable after
/// construction; cloning copies the topic name.
///
/// The policy is attached to a subscription through
/// [`SubscribeOptions::dead_letter`](super::SubscribeOptions::dead_letter) or
/// [`SubscribeRequestBuilder::dead_letter`](super::SubscribeRequestBuilder::dead_letter).
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::DeadLetterAdmissionPolicy;
/// use qubit_event_bus::model::DeadLetterPolicy;
/// use qubit_event_bus::model::Topic;
///
/// let policy = DeadLetterPolicy::with_known_destination("orders.dlq").unwrap();
/// assert_eq!(policy.topic_name(), "orders.dlq");
/// assert_eq!(policy.admission_policy(), DeadLetterAdmissionPolicy::KnownDestination);
///
/// let topic = Topic::<String>::new("orders.dlq").unwrap();
/// assert_eq!(DeadLetterPolicy::with_topic(&topic).topic_name(), "orders.dlq");
///
/// assert!(DeadLetterPolicy::with_topic_name(" orders.dlq").is_err());
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterPolicy {
    /// Validated dead-letter topic name: nonblank, without surrounding
    /// whitespace or control characters. Stored as an owned string whose
    /// contents never change after construction.
    topic_name: Box<str>,
    /// Evidence the dead-letter publication must produce before the source
    /// delivery counts as forwarded rather than lost.
    admission: DeadLetterAdmissionPolicy,
}

impl DeadLetterPolicy {
    /// Creates a policy that accepts provider-level transport acknowledgement.
    ///
    /// This is the lenient default: the source delivery is settled as
    /// forwarded as soon as the provider reports the dead-letter publication
    /// as accepted, even when no concrete destination is known. Use
    /// [`Self::with_known_destination`] when a stronger guarantee is required.
    ///
    /// # Parameters
    /// - `name`: the dead-letter topic name; copied into the policy.
    ///
    /// # Returns
    /// A policy targeting `name` with
    /// [`DeadLetterAdmissionPolicy::TransportAccepted`].
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] when `name` is blank,
    /// has surrounding whitespace, or contains a control character.
    pub fn with_topic_name(name: &str) -> Result<Self, ConfigurationError> {
        Self::with_admission(name, DeadLetterAdmissionPolicy::TransportAccepted)
    }

    /// Creates a policy from an already validated topic.
    ///
    /// The destination is copied from `topic`. Admission matches
    /// [`Self::with_topic_name`]: the source delivery is settled as forwarded
    /// as soon as the provider reports the dead-letter publication as
    /// accepted. A [`Topic`] name is already validated, so this constructor
    /// does not fail.
    ///
    /// # Type Parameters
    /// - `T`: the payload type bound to `topic`. Only the name is stored, so
    ///   `T` need not be the dead-letter payload type.
    ///
    /// # Parameters
    /// - `topic`: the destination whose name is copied into the policy.
    ///
    /// # Returns
    /// A policy targeting `topic`'s name with
    /// [`DeadLetterAdmissionPolicy::TransportAccepted`].
    #[must_use]
    pub fn with_topic<T: 'static>(topic: &Topic<T>) -> Self {
        Self {
            topic_name: topic.name().into(),
            admission: DeadLetterAdmissionPolicy::TransportAccepted,
        }
    }

    /// Creates a policy that requires a known destination to accept the event.
    ///
    /// The source delivery is settled as forwarded only after at least one
    /// reported destination accepts the dead-letter publication; an opaque
    /// transport acknowledgement alone is not sufficient.
    ///
    /// # Parameters
    /// - `topic_name`: the dead-letter topic name; copied into the policy.
    ///
    /// # Returns
    /// A policy targeting `topic_name` with
    /// [`DeadLetterAdmissionPolicy::KnownDestination`].
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] when `topic_name` is
    /// blank, has surrounding whitespace, or contains a control character.
    pub fn with_known_destination(topic_name: &str) -> Result<Self, ConfigurationError> {
        Self::with_admission(topic_name, DeadLetterAdmissionPolicy::KnownDestination)
    }

    /// Creates a policy with an explicit topic and admission requirement.
    ///
    /// [`Self::with_topic_name`] and [`Self::with_known_destination`] forward
    /// to this constructor; call it directly when the admission requirement is
    /// chosen at runtime. Validation happens once here, so later accessors
    /// never fail. [`Self::with_topic`] copies an already validated [`Topic`]
    /// name and does not call this constructor.
    ///
    /// # Parameters
    /// - `topic_name`: the dead-letter topic name; copied into the policy.
    /// - `admission`: the evidence required before the source delivery is
    ///   settled as forwarded.
    ///
    /// # Returns
    /// A policy targeting `topic_name` with the given `admission` requirement.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] with field `dead_letter`
    /// when `topic_name` is blank, has surrounding whitespace, or contains a
    /// control character.
    pub fn with_admission(topic_name: &str, admission: DeadLetterAdmissionPolicy) -> Result<Self, ConfigurationError> {
        if !is_nonblank_without_controls(topic_name) {
            return Err(ConfigurationError::InvalidField {
                field: "dead_letter",
                message: "topic must be nonblank and contain no controls".into(),
            });
        }
        Ok(Self {
            topic_name: topic_name.into(),
            admission,
        })
    }

    /// Returns the topic name receiving dead-letter events.
    ///
    /// # Returns
    /// The validated topic name, borrowed for the lifetime of the policy. It
    /// is guaranteed nonblank, free of surrounding whitespace and control
    /// characters, so it can be handed to a provider without re-validation.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns the evidence required before the source is settled as forwarded.
    ///
    /// # Returns
    /// A copy of the configured [`DeadLetterAdmissionPolicy`]; the enum is
    /// `Copy`, so no allocation or borrow is involved.
    #[must_use = "Use the returned admission policy."]
    #[inline]
    pub fn admission_policy(&self) -> DeadLetterAdmissionPolicy {
        self.admission
    }
}
