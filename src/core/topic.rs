/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Type-safe event topics.

use std::any::{
    TypeId,
    type_name,
};
use std::fmt::{
    self,
    Display,
    Formatter,
};
use std::hash::{
    Hash,
    Hasher,
};
use std::marker::PhantomData;

use crate::{
    EventBusError,
    EventBusResult,
    TopicKey,
};

/// Type-safe event topic.
///
/// `T` is the payload type associated with the topic. Two topics are equal only
/// when both the topic name and payload type match.
#[derive(Debug)]
pub struct Topic<T: 'static> {
    name: String,
    payload_type_id: TypeId,
    payload_type_name: &'static str,
    marker: PhantomData<fn() -> T>,
}

impl<T: 'static> Topic<T> {
    /// Creates a topic after validating its name.
    ///
    /// # Parameters
    /// - `name`: Non-blank topic name.
    ///
    /// # Returns
    /// A type-safe topic bound to `T`.
    ///
    /// # Errors
    /// Returns [`EventBusError::InvalidArgument`] when `name` is blank.
    pub fn try_new(name: impl Into<String>) -> EventBusResult<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(EventBusError::invalid_argument("name", "topic name must not be blank"));
        }
        Ok(Self {
            name,
            payload_type_id: TypeId::of::<T>(),
            payload_type_name: type_name::<T>(),
            marker: PhantomData,
        })
    }

    /// Returns the topic name.
    ///
    /// # Returns
    /// The immutable topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the payload [`TypeId`].
    ///
    /// # Returns
    /// Type identifier for payload `T`.
    pub fn payload_type_id(&self) -> TypeId {
        self.payload_type_id
    }

    /// Returns the Rust payload type name.
    ///
    /// # Returns
    /// Fully qualified payload type name.
    pub fn payload_type_name(&self) -> &'static str {
        self.payload_type_name
    }

    /// Returns a type-erased key for internal maps.
    ///
    /// # Returns
    /// A key containing the topic name and payload type.
    pub fn key(&self) -> TopicKey {
        TopicKey::new(self.name.clone(), self.payload_type_id)
    }
}

impl<T: 'static> Clone for Topic<T> {
    /// Clones the topic metadata without requiring `T: Clone`.
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            payload_type_id: self.payload_type_id,
            payload_type_name: self.payload_type_name,
            marker: PhantomData,
        }
    }
}

impl<T: 'static> PartialEq for Topic<T> {
    /// Compares topic name and payload type.
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.payload_type_id == other.payload_type_id
    }
}

impl<T: 'static> Eq for Topic<T> {}

impl<T: 'static> Hash for Topic<T> {
    /// Hashes topic name and payload type.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.payload_type_id.hash(state);
    }
}

impl<T: 'static> Display for Topic<T> {
    /// Formats the topic as `<payload_type>.<topic_name>`.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.payload_type_name, self.name)
    }
}
