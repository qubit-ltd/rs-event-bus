/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Typed staged event envelope.

use std::any::{
    Any,
    TypeId,
};

use crate::{
    EventEnvelope,
    EventEnvelopeMetadata,
    PublishOptions,
    StagedEvent,
};

/// Typed event and publish options staged inside a transaction.
#[derive(Clone)]
pub struct StagedEventEnvelope<T: Clone + Send + Sync + 'static> {
    envelope: EventEnvelope<T>,
    options: PublishOptions<T>,
}

impl<T> StagedEventEnvelope<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Creates a typed staged event.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to publish on commit.
    /// - `options`: Publish options for this event.
    ///
    /// # Returns
    /// Staged event retaining typed envelope and options.
    pub fn new(envelope: EventEnvelope<T>, options: PublishOptions<T>) -> Self {
        Self { envelope, options }
    }

    /// Returns the typed event envelope.
    ///
    /// # Returns
    /// Immutable staged envelope.
    pub fn envelope(&self) -> &EventEnvelope<T> {
        &self.envelope
    }

    /// Returns publish options for this staged event.
    ///
    /// # Returns
    /// Immutable staged publish options.
    pub fn options(&self) -> &PublishOptions<T> {
        &self.options
    }

    /// Consumes this staged event into typed parts.
    ///
    /// # Returns
    /// Staged envelope and publish options.
    pub fn into_parts(self) -> (EventEnvelope<T>, PublishOptions<T>) {
        (self.envelope, self.options)
    }
}

impl<T> StagedEvent for StagedEventEnvelope<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn metadata(&self) -> EventEnvelopeMetadata {
        self.envelope.metadata()
    }

    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}
