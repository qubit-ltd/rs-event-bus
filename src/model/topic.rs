// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Typed topic identity.

use std::any::TypeId;
use std::any::type_name;
use std::borrow::Cow;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use super::schema_id::SchemaId;
use crate::codec::EventCodec;
use crate::error::ConfigurationError;
use crate::util::validated_text::is_valid_topic_name;

/// A topic bound to one Rust payload type.
///
/// # Type Parameters
/// - `T`: the payload type published to and received from this topic. It must
///   be `'static` because providers may retain payloads beyond the caller's
///   stack frame.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::Topic;
///
/// let topic = Topic::<String>::new("orders.created").unwrap();
/// assert_eq!(topic.name(), "orders.created");
/// assert!(topic.payload_type_name().contains("String"));
/// ```
pub struct Topic<T: 'static> {
    /// The validated routing name, borrowed when static and owned otherwise.
    name: Cow<'static, str>,
    /// The optional codec used to serialize payloads for encoded providers.
    codec: Option<Arc<dyn EventCodec<T>>>,
}

impl<T: 'static> Topic<T> {
    /// Creates a topic without configuring a topic-level codec, from a static
    /// name without allocating. A facade can resolve a codec from its registry
    /// when publishing through an encoded provider.
    ///
    /// # Parameters
    /// - `name`: the static topic name to validate and borrow.
    ///
    /// # Returns
    /// A topic that borrows `name` and has no topic-level codec configured.
    ///
    /// # Panics
    /// Panics during constant evaluation, or at runtime, when the name is
    /// empty, longer than 255 UTF-8 bytes, has surrounding Unicode
    /// whitespace, or contains a control character.
    ///
    /// ```compile_fail
    /// use qubit_event_bus::model::Topic;
    /// const INVALID_TOPIC: Topic<String> = Topic::new_static(" orders.created");
    /// ```
    #[must_use]
    pub const fn new_static(name: &'static str) -> Self {
        assert!(is_valid_topic_name(name), "invalid topic name");
        Self {
            name: Cow::Borrowed(name),
            codec: None,
        }
    }

    /// Creates a topic without configuring a topic-level codec after
    /// validating its name. A facade can resolve a codec from its registry
    /// when publishing through an encoded provider.
    ///
    /// # Parameters
    /// - `name`: the topic name to validate and copy.
    ///
    /// # Returns
    /// A topic that owns `name` and has no topic-level codec configured.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] for `topic` when the name
    /// is empty, longer than 255 UTF-8 bytes, has surrounding Unicode
    /// whitespace, or contains a control character.
    pub fn new(name: &str) -> Result<Self, ConfigurationError> {
        if !is_valid_topic_name(name) {
            return Err(ConfigurationError::InvalidField {
                field: "topic",
                message: "must be 1..=255 bytes without surrounding whitespace or controls".into(),
            });
        }
        Ok(Self {
            name: Cow::Owned(name.into()),
            codec: None,
        })
    }

    /// Returns the validated topic name.
    ///
    /// # Returns
    /// The name borrowed for the lifetime of this topic.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the Rust payload type identity.
    ///
    /// # Returns
    /// The process-local [`TypeId`] of `T`.
    #[must_use]
    #[inline]
    pub fn payload_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    /// Returns the Rust payload type name for diagnostics.
    ///
    /// # Returns
    /// The compiler-provided type name of `T`, suitable for diagnostics and
    /// not guaranteed to be stable across compiler versions.
    #[must_use]
    #[inline]
    pub fn payload_type_name(&self) -> &'static str {
        type_name::<T>()
    }

    /// Returns the topic-level codec, or `None` when none was configured.
    /// A facade may still resolve a codec from its registry for an encoded
    /// provider.
    ///
    /// # Returns
    /// A shared codec reference when one was configured, otherwise `None`;
    /// `None` does not prevent facade registry fallback.
    #[inline]
    pub fn codec(&self) -> Option<&Arc<dyn EventCodec<T>>> {
        self.codec.as_ref()
    }

    /// Returns the codec schema ID, or `None` when none was supplied.
    ///
    /// # Returns
    /// The schema identifier borrowed from the configured codec, or `None`.
    #[inline]
    pub fn schema_id(&self) -> Option<&SchemaId> {
        self.codec.as_ref().and_then(|codec| codec.schema_id())
    }

    /// Attaches an owned codec for encoded backends.
    ///
    /// # Type Parameters
    /// - `C`: the codec implementation associated with payload type `T`.
    ///
    /// # Parameters
    /// - `codec`: the codec used by providers that require encoded payloads.
    ///
    /// # Returns
    /// `self` with the codec shared through an `Arc`.
    #[must_use]
    pub fn with_codec<C>(mut self, codec: C) -> Self
    where
        C: EventCodec<T> + 'static,
    {
        self.codec = Some(Arc::new(codec));
        self
    }

    /// Attaches an already shared or registered codec for encoded backends.
    ///
    /// # Parameters
    /// - `codec`: the shared codec used by encoded providers.
    ///
    /// # Returns
    /// `self` retaining the supplied shared codec.
    #[must_use]
    pub fn with_shared_codec(mut self, codec: Arc<dyn EventCodec<T>>) -> Self {
        self.codec = Some(codec);
        self
    }
}

impl<T: 'static> Clone for Topic<T> {
    /// Clones the topic name and shared codec handle.
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<T: 'static> PartialEq for Topic<T> {
    /// Compares the routing name and payload type, ignoring codec identity.
    ///
    /// # Parameters
    /// - `other`: topic being compared with this routing address.
    ///
    /// # Returns
    /// `true` when both topics have the same name and payload type.
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.payload_type_id() == other.payload_type_id()
    }
}
impl<T: 'static> Eq for Topic<T> {}
impl<T: 'static> Hash for Topic<T> {
    /// Hashes the same name and payload type identity used by equality.
    ///
    /// # Type Parameters
    /// - `H`: hasher that receives the topic identity components.
    ///
    /// # Parameters
    /// - `state`: hasher to receive the name and payload type identity.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.payload_type_id().hash(state);
    }
}
impl<T: 'static> fmt::Debug for Topic<T> {
    /// Formats the routing name and payload type name without exposing codec
    /// internals.
    ///
    /// # Parameters
    /// - `formatter`: output formatter receiving the debug representation.
    ///
    /// # Returns
    /// The formatter's result.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Topic")
            .field("name", &self.name)
            .field("payload_type_name", &type_name::<T>())
            .finish()
    }
}
