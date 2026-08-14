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
use crate::TopicKey;

/// Subscriber processing task with cancellation-aware idle accounting.
pub(crate) struct ProcessingTask {
    bus: Arc<LocalEventBusInner>,
    topic_key: TopicKey,
    task: Option<Box<dyn FnOnce() + Send + 'static>>,
    finished: bool,
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
    pub(crate) fn new<F>(
        bus: Arc<LocalEventBusInner>,
        topic_key: TopicKey,
        task: F,
    ) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self {
            bus,
            topic_key,
            task: Some(Box::new(task)),
            finished: false,
        }
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
