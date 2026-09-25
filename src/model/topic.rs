// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Typed topic identity and portable codec metadata.

use std::any::TypeId;
use std::any::type_name;
use std::borrow::Cow;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use super::validated_text::is_nonblank_without_controls;
use super::validated_text::is_valid_topic_name;
use crate::codec::EventCodec;
use crate::error::ConfigurationError;

/// A validated MIME content type used by a codec.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::ContentType;
///
/// let content_type = ContentType::new("application/json").unwrap();
/// assert_eq!(content_type.as_str(), "application/json");
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ContentType(Box<str>);

impl ContentType {
    /// Creates a nonblank content type without surrounding whitespace.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        let valid = value
            .split_once('/')
            .is_some_and(|(kind, subtype)| valid_mime_token(kind) && valid_mime_token(subtype));
        if !valid {
            return Err(ConfigurationError::InvalidField {
                field: "content_type",
                message: "expected a MIME type".into(),
            });
        }
        Ok(Self(value.into()))
    }

    /// Returns the content type string.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Checks the ASCII token syntax accepted for each MIME type component.
fn valid_mime_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'+' | b'.'))
}

/// A validated schema identifier supplied by an application codec.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::SchemaId;
///
/// let schema_id = SchemaId::new("order-v1").unwrap();
/// assert_eq!(schema_id.as_str(), "order-v1");
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SchemaId(Cow<'static, str>);

impl SchemaId {
    /// Creates a schema identifier from a static string without allocating.
    ///
    /// # Panics
    /// Panics during constant evaluation, or at runtime, if the value is empty,
    /// has surrounding Unicode whitespace, or contains a control character.
    ///
    /// ```compile_fail
    /// use qubit_event_bus::model::SchemaId;
    /// const INVALID_SCHEMA: SchemaId = SchemaId::new_static("schema-v1\n");
    /// ```
    pub const fn new_static(value: &'static str) -> Self {
        assert!(is_nonblank_without_controls(value), "invalid schema ID");
        Self(Cow::Borrowed(value))
    }

    /// Creates a nonblank schema identifier.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        if !is_nonblank_without_controls(value) {
            return Err(ConfigurationError::InvalidField {
                field: "schema_id",
                message: "must be nonblank and contain no controls".into(),
            });
        }
        Ok(Self(Cow::Owned(value.into())))
    }

    /// Returns the original schema identifier.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

/// A topic bound to one Rust payload type.
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
    name: Cow<'static, str>,
    codec: Option<Arc<dyn EventCodec<T>>>,
}

impl<T: 'static> Topic<T> {
    /// Creates a native topic from a static name without allocating.
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
    pub const fn new_static(name: &'static str) -> Self {
        assert!(is_valid_topic_name(name), "invalid topic name");
        Self {
            name: Cow::Borrowed(name),
            codec: None,
        }
    }

    /// Creates a native-payload topic after validating its name.
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

    /// Creates a topic with a codec for encoded backends.
    pub fn new_with_codec<C>(name: &str, codec: C) -> Result<Self, ConfigurationError>
    where
        C: EventCodec<T>,
    {
        Self::new_with_shared_codec(name, Arc::new(codec))
    }

    /// Creates a topic using an already shared or registered codec.
    pub fn new_with_shared_codec(name: &str, codec: Arc<dyn EventCodec<T>>) -> Result<Self, ConfigurationError> {
        let mut topic = Self::new(name)?;
        topic.codec = Some(codec);
        Ok(topic)
    }

    /// Returns the validated topic name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the Rust payload type identity.
    #[must_use]
    #[inline]
    pub fn payload_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    /// Returns the Rust payload type name for diagnostics.
    #[must_use]
    #[inline]
    pub fn payload_type_name(&self) -> &'static str {
        type_name::<T>()
    }

    /// Returns the configured codec, or `None` for a native-only topic.
    #[must_use]
    #[inline]
    pub fn codec(&self) -> Option<&Arc<dyn EventCodec<T>>> {
        self.codec.as_ref()
    }

    /// Returns the codec schema ID, or `None` when none was supplied.
    #[must_use]
    #[inline]
    pub fn schema_id(&self) -> Option<&SchemaId> {
        self.codec.as_ref().and_then(|codec| codec.schema_id())
    }
}

impl<T: 'static> Clone for Topic<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<T: 'static> PartialEq for Topic<T> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.payload_type_id() == other.payload_type_id()
    }
}
impl<T: 'static> Eq for Topic<T> {}
impl<T: 'static> Hash for Topic<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.payload_type_id().hash(state);
    }
}
impl<T: 'static> fmt::Debug for Topic<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Topic")
            .field("name", &self.name)
            .field("payload_type_name", &type_name::<T>())
            .finish()
    }
}
