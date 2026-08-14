// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Transactional publisher abstraction.

use crate::EventBusResult;
use crate::EventEnvelope;
use crate::PublishOptions;
use crate::StagedEvent;
use crate::StagedEventEnvelope;
use crate::Topic;

/// Publisher that buffers events until a transaction is committed.
///
/// Implementations are expected to be short-lived and transaction scoped. This
/// crate currently exposes the abstraction so backends with transactional
/// support can implement it without changing the public API later.
pub trait TransactionalPublisher {
    /// Stages a payload for transactional publishing.
    ///
    /// # Parameters
    /// - `topic`: Target topic.
    /// - `payload`: Event payload.
    ///
    /// # Returns
    /// `Ok(())` after the event is staged.
    ///
    /// # Errors
    /// Returns backend-specific staging errors.
    fn publish<T>(&mut self, topic: &Topic<T>, payload: T) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope(EventEnvelope::create(topic.clone(), payload))
    }

    /// Stages an envelope for transactional publishing.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to stage.
    ///
    /// # Returns
    /// `Ok(())` after the event is staged.
    ///
    /// # Errors
    /// Returns backend-specific staging errors.
    fn publish_envelope<T>(
        &mut self,
        envelope: EventEnvelope<T>,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope_with_options(envelope, PublishOptions::empty())
    }

    /// Stages an envelope with explicit publish options.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to stage.
    /// - `options`: Publish options for this event.
    ///
    /// # Returns
    /// `Ok(())` after the event is staged.
    ///
    /// # Errors
    /// Returns backend-specific staging errors.
    fn publish_envelope_with_options<T>(
        &mut self,
        envelope: EventEnvelope<T>,
        options: PublishOptions<T>,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_staged(Box::new(StagedEventEnvelope::new(
            envelope, options,
        )))
    }

    /// Stages a type-erased event for transactional publishing.
    ///
    /// # Parameters
    /// - `event`: Type-erased staged event.
    ///
    /// # Returns
    /// `Ok(())` after the event is staged.
    ///
    /// # Errors
    /// Returns backend-specific staging errors.
    fn publish_staged(
        &mut self,
        event: Box<dyn StagedEvent>,
    ) -> EventBusResult<()>;

    /// Stages multiple type-erased events.
    ///
    /// # Parameters
    /// - `events`: Events to stage in order.
    ///
    /// # Returns
    /// `Ok(())` when all events are staged.
    ///
    /// # Errors
    /// Returns the first backend-specific staging error.
    fn publish_all_staged(
        &mut self,
        events: Vec<Box<dyn StagedEvent>>,
    ) -> EventBusResult<()> {
        for event in events {
            self.publish_staged(event)?;
        }
        Ok(())
    }

    /// Commits the transaction and publishes all staged events atomically.
    ///
    /// # Returns
    /// `Ok(())` when all staged events are committed.
    ///
    /// # Errors
    /// Returns backend-specific commit errors.
    fn commit(&mut self) -> EventBusResult<()>;

    /// Rolls back the transaction and clears staged events.
    ///
    /// # Returns
    /// `Ok(())` when staged events are discarded.
    ///
    /// # Errors
    /// Returns backend-specific rollback errors.
    fn rollback(&mut self) -> EventBusResult<()>;
}
