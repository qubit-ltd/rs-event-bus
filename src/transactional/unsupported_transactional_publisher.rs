/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Unsupported transactional publisher placeholder.

use crate::{EventBusError, EventBusResult, EventEnvelope, TransactionalPublisher};

/// Placeholder transactional publisher used by unsupported transactional buses.
#[derive(Debug, Default)]
pub struct UnsupportedTransactionalPublisher;

impl UnsupportedTransactionalPublisher {
    /// Creates an unsupported transactional publisher placeholder.
    ///
    /// # Returns
    /// Placeholder value whose operations return unsupported-operation errors.
    pub const fn new() -> Self {
        Self
    }
}

impl TransactionalPublisher for UnsupportedTransactionalPublisher {
    /// Returns an unsupported-operation error.
    fn publish_envelope<T>(&mut self, _envelope: EventEnvelope<T>) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        Err(EventBusError::unsupported_operation(
            "transactional_publish",
        ))
    }

    /// Returns an unsupported-operation error.
    fn commit(&mut self) -> EventBusResult<()> {
        Err(EventBusError::unsupported_operation("transactional_commit"))
    }

    /// Clears no state and returns `Ok(())`.
    fn rollback(&mut self) -> EventBusResult<()> {
        Ok(())
    }
}
