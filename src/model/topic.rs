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
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use crate::codec::EventCodec;
use crate::error::ConfigurationError;

/// A validated MIME content type used by a codec.
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
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SchemaId(Box<str>);

impl SchemaId {
    /// Creates a nonblank schema identifier.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
            return Err(ConfigurationError::InvalidField {
                field: "schema_id",
                message: "must be nonblank and contain no controls".into(),
            });
        }
        Ok(Self(value.into()))
    }

    /// Returns the original schema identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A topic bound to one Rust payload type.
pub struct Topic<T: 'static> {
    name: Box<str>,
    payload_type_id: TypeId,
    payload_type_name: &'static str,
    codec: Option<Arc<dyn EventCodec<T>>>,
}

impl<T: 'static> Topic<T> {
    /// Creates a native-payload topic after validating its name.
    pub fn new(name: &str) -> Result<Self, ConfigurationError> {
        if !(1..=255).contains(&name.len()) || name.trim() != name || name.chars().any(char::is_control) {
            return Err(ConfigurationError::InvalidField {
                field: "topic",
                message: "must be 1..=255 bytes without surrounding whitespace or controls".into(),
            });
        }
        Ok(Self {
            name: name.into(),
            payload_type_id: TypeId::of::<T>(),
            payload_type_name: type_name::<T>(),
            codec: None,
        })
    }

    /// Creates a topic with a codec for encoded backends.
    pub fn with_codec<C>(name: &str, codec: C) -> Result<Self, ConfigurationError>
    where
        C: EventCodec<T>,
    {
        Self::with_shared_codec(name, Arc::new(codec))
    }

    /// Creates a topic using an already shared or registered codec.
    pub fn with_shared_codec(name: &str, codec: Arc<dyn EventCodec<T>>) -> Result<Self, ConfigurationError> {
        let mut topic = Self::new(name)?;
        topic.codec = Some(codec);
        Ok(topic)
    }

    /// Returns the validated topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the Rust payload type identity.
    pub fn payload_type_id(&self) -> TypeId {
        self.payload_type_id
    }

    /// Returns the Rust payload type name for diagnostics.
    pub fn payload_type_name(&self) -> &'static str {
        self.payload_type_name
    }

    /// Returns the configured codec, or `None` for a native-only topic.
    pub fn codec(&self) -> Option<&Arc<dyn EventCodec<T>>> {
        self.codec.as_ref()
    }

    /// Returns the codec schema ID, or `None` when none was supplied.
    pub fn schema_id(&self) -> Option<&SchemaId> {
        self.codec.as_ref().and_then(|codec| codec.schema_id())
    }
}

impl<T: 'static> Clone for Topic<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            payload_type_id: self.payload_type_id,
            payload_type_name: self.payload_type_name,
            codec: self.codec.clone(),
        }
    }
}

impl<T: 'static> PartialEq for Topic<T> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.payload_type_id == other.payload_type_id
    }
}
impl<T: 'static> Eq for Topic<T> {}
impl<T: 'static> Hash for Topic<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.payload_type_id.hash(state);
    }
}
impl<T: 'static> fmt::Debug for Topic<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Topic")
            .field("name", &self.name)
            .field("payload_type_name", &self.payload_type_name)
            .finish()
    }
}
