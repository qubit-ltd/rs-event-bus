// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Unsupported transactional backend placeholders.

use std::time::Duration;

use super::unsupported_transactional_publisher::UnsupportedTransactionalPublisher;
use crate::EventBus;
use crate::EventBusError;
use crate::EventBusResult;
use crate::EventEnvelope;
use crate::IntoEventBusResult;
use crate::PublishOptions;
use crate::StagedEvent;
use crate::SubscribeOptions;
use crate::Subscription;
use crate::Topic;
use crate::TransactionalEventBus;

/// Placeholder transactional bus used by factories without transaction support.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnsupportedTransactionalEventBus;

impl UnsupportedTransactionalEventBus {
    /// Creates an unsupported transactional event bus placeholder.
    ///
    /// # Returns
    /// Placeholder value whose operations return unsupported-operation errors.
    pub const fn new() -> Self {
        Self
    }
}

impl EventBus for UnsupportedTransactionalEventBus {
    /// Unsupported placeholders never start.
    fn start(&self) -> EventBusResult<bool> {
        Ok(false)
    }

    /// Unsupported placeholders are never running.
    fn shutdown(&self) -> bool {
        false
    }

    /// Returns an unsupported-operation error.
    fn publish_envelope_with_options<T>(
        &self,
        _envelope: EventEnvelope<T>,
        _options: PublishOptions<T>,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        Err(EventBusError::unsupported_operation("publish"))
    }

    /// Returns an unsupported-operation error.
    fn subscribe_with_options<T, S, F, R>(
        &self,
        _subscriber_id: S,
        _topic: &Topic<T>,
        _handler: F,
        _options: SubscribeOptions<T>,
    ) -> EventBusResult<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        Err(EventBusError::unsupported_operation("subscribe"))
    }

    /// Returns an unsupported-operation error.
    fn wait_for_idle<T>(&self, _topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static,
    {
        Err(EventBusError::unsupported_operation("wait_for_idle"))
    }

    /// Returns an unsupported-operation error.
    fn wait_for_idle_timeout<T>(&self, _topic: &Topic<T>, _timeout: Duration) -> EventBusResult<bool>
    where
        T: 'static,
    {
        Err(EventBusError::unsupported_operation("wait_for_idle_timeout"))
    }
}

impl TransactionalEventBus for UnsupportedTransactionalEventBus {
    type Publisher = UnsupportedTransactionalPublisher;

    /// Returns an unsupported-operation error.
    fn create_transactional_publisher(&self) -> EventBusResult<Self::Publisher> {
        Err(EventBusError::unsupported_operation("create_transactional_publisher"))
    }

    /// Returns an unsupported-operation error.
    fn publish_batch_atomically_staged(&self, _events: Vec<Box<dyn StagedEvent>>) -> EventBusResult<()> {
        Err(EventBusError::unsupported_operation("publish_batch_atomically"))
    }
}
