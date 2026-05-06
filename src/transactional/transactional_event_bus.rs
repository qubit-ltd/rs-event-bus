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
    TransactionalPublisher,
};

/// Event bus extension for transactional publishing backends.
///
/// The typed batch method models the Java transactional contract for a single
/// payload type. Backends that need heterogeneous batches can add a type-erased
/// adapter without changing this core trait.
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
        T: Clone + Send + Sync + 'static;
}
