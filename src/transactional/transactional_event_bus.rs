/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Transactional event bus abstraction.

use crate::{
    EventBus,
    EventBusResult,
    EventEnvelope,
    PublishOptions,
    StagedEvent,
    StagedEventEnvelope,
    TransactionalPublisher,
};

/// Event bus extension for transactional publishing backends.
///
/// The type-erased staged batch is the core contract. Typed convenience
/// methods lower into staged events so backends can support heterogeneous
/// transaction contents without duplicating generic entry points.
pub trait TransactionalEventBus: EventBus {
    /// Transactional publisher created by this bus.
    type Publisher: TransactionalPublisher;

    /// Creates a transaction-scoped publisher.
    ///
    /// # Returns
    /// Publisher that stages events until commit.
    ///
    /// # Errors
    /// Returns backend-specific errors or unsupported-operation errors.
    fn create_transactional_publisher(&self) -> EventBusResult<Self::Publisher>;

    /// Publishes a typed batch atomically.
    ///
    /// # Parameters
    /// - `envelopes`: Envelopes to publish atomically.
    /// - `options`: Publish options applied to the atomic batch.
    ///
    /// # Returns
    /// `Ok(())` only when the whole batch is published.
    ///
    /// # Errors
    /// Returns backend-specific atomic publishing errors.
    fn publish_batch_atomically<T>(
        &self,
        envelopes: Vec<EventEnvelope<T>>,
        options: PublishOptions<T>,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        let staged = envelopes
            .into_iter()
            .map(|envelope| {
                Box::new(StagedEventEnvelope::new(envelope, options.clone()))
                    as Box<dyn StagedEvent>
            })
            .collect::<Vec<_>>();
        self.publish_batch_atomically_staged(staged)
    }

    /// Publishes a heterogeneous staged batch atomically.
    ///
    /// # Parameters
    /// - `events`: Type-erased staged events to publish atomically.
    ///
    /// # Returns
    /// `Ok(())` only when the whole batch is published.
    ///
    /// # Errors
    /// Returns backend-specific atomic publishing errors.
    fn publish_batch_atomically_staged(
        &self,
        events: Vec<Box<dyn StagedEvent>>,
    ) -> EventBusResult<()>;
}
