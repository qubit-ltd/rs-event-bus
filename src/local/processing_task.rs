// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation-aware local subscriber processing task.

use std::sync::Arc;

use super::local_event_bus_inner::LocalEventBusInner;
use crate::EventBusError;
use crate::TopicKey;

/// Identifies one accepted subscriber delivery in background failures.
pub(crate) struct DeliveryContext {
    pub(crate) event_id: String,
    pub(crate) topic_name: String,
    pub(crate) subscriber_id: String,
}

/// Subscriber processing task with cancellation-aware idle accounting.
pub(crate) struct ProcessingTask {
    bus: Arc<LocalEventBusInner>,
    topic_key: TopicKey,
    task: Option<Box<dyn FnOnce() + Send + 'static>>,
    finished: bool,
    delivery_context: Option<DeliveryContext>,
}

impl ProcessingTask {
    /// Creates a processing task.
    ///
    /// # Parameters
    /// - `bus`: Shared bus state that owns processing counters.
    /// - `topic_key`: Topic whose active count was incremented.
    /// - `task`: Handler work to run.
    ///
    /// # Returns
    /// Processing task that finishes accounting on run or drop.
    pub(crate) fn new<F>(bus: Arc<LocalEventBusInner>, topic_key: TopicKey, task: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self {
            bus,
            topic_key,
            task: Some(Box::new(task)),
            finished: false,
            delivery_context: None,
        }
    }

    /// Creates a delivery task with context for asynchronous rejection reports.
    pub(crate) fn with_delivery_context<F>(
        bus: Arc<LocalEventBusInner>,
        topic_key: TopicKey,
        delivery_context: DeliveryContext,
        task: F,
    ) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        let mut processing_task = Self::new(bus, topic_key, task);
        processing_task.delivery_context = Some(delivery_context);
        processing_task
    }

    /// Reports a rejected delayed delivery, then releases its idle accounting.
    pub(crate) fn reject(self, cause: &EventBusError) {
        let context = self.delivery_context.as_ref();
        let message = match context {
            Some(context) => format!(
                "delayed delivery rejected: event_id={}, topic={}, subscriber_id={}: {cause}",
                context.event_id, context.topic_name, context.subscriber_id,
            ),
            None => format!("delayed delivery rejected: {cause}"),
        };
        self.bus.observe_error(&EventBusError::execution_rejected(message));
    }

    /// Runs the processing task exactly once.
    pub(crate) fn run(mut self) {
        if let Some(task) = self.task.take() {
            task();
        }
        self.finish();
    }

    /// Finishes active processing accounting.
    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.bus.finish_processing(&self.topic_key);
    }
}

impl Drop for ProcessingTask {
    /// Finishes processing if the task is cancelled before it runs.
    fn drop(&mut self) {
        self.finish();
    }
}
