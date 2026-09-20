// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Processing tracker for active local event-bus work.

use std::collections::HashMap;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use crate::EventBusError;
use crate::EventBusResult;
use crate::TopicKey;
/// Tracks active handler work per topic.
pub(super) struct ProcessingTracker {
    counts: Mutex<HashMap<TopicKey, usize>>,
    condvar: Condvar,
}

impl ProcessingTracker {
    /// Creates an empty processing tracker.
    ///
    /// # Returns
    /// Tracker with zero active work.
    pub(super) fn new() -> Self {
        Self {
            counts: Mutex::new(HashMap::new()),
            condvar: Condvar::new(),
        }
    }

    /// Increments active work for a topic.
    ///
    /// # Parameters
    /// - `topic_key`: Topic receiving new handler work.
    ///
    /// # Returns
    /// `Ok(())` after incrementing the count.
    pub(super) fn start(&self, topic_key: &TopicKey) -> EventBusResult<()> {
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        *counts.entry(topic_key.clone()).or_insert(0) += 1;
        Ok(())
    }

    /// Decrements active work for a topic.
    ///
    /// # Parameters
    /// - `topic_key`: Topic whose handler work finished.
    pub(super) fn finish(&self, topic_key: &TopicKey) {
        if let Ok(mut counts) = self.counts.lock() {
            if let Some(count) = counts.get_mut(topic_key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(topic_key);
                }
            }
            self.condvar.notify_all();
        }
    }

    /// Waits until a topic has zero active work.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to wait for.
    ///
    /// # Returns
    /// `Ok(())` once the topic is idle.
    pub(super) fn wait_for_idle(&self, topic_key: &TopicKey) -> EventBusResult<()> {
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        while counts.get(topic_key).copied().unwrap_or(0) > 0 {
            counts = match self.condvar.wait(counts) {
                Ok(counts) => counts,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        Ok(())
    }

    /// Waits until a topic has zero active work or the timeout elapses.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to wait for.
    /// - `timeout`: Maximum duration to wait.
    ///
    /// # Returns
    /// `Ok(true)` once the topic is idle, or `Ok(false)` when the timeout
    /// elapses first.
    pub(super) fn wait_for_idle_timeout(&self, topic_key: &TopicKey, timeout: Duration) -> EventBusResult<bool> {
        let started_at = Instant::now();
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        while counts.get(topic_key).copied().unwrap_or(0) > 0 {
            let Some(remaining) = remaining_timeout(started_at, timeout) else {
                return Ok(false);
            };
            let (next_counts, timeout_result) = match self.condvar.wait_timeout(counts, remaining) {
                Ok(result) => result,
                Err(poisoned) => poisoned.into_inner(),
            };
            counts = next_counts;
            if timeout_result.timed_out() && counts.get(topic_key).copied().unwrap_or(0) > 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Waits until all topics have zero active work.
    ///
    /// # Returns
    /// `Ok(())` once all tracked topics are idle.
    pub(super) fn wait_for_all_idle(&self) -> EventBusResult<()> {
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        while !counts.is_empty() {
            counts = match self.condvar.wait(counts) {
                Ok(counts) => counts,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        Ok(())
    }

    /// Waits until all topics have zero active work or the timeout elapses.
    ///
    /// # Parameters
    /// - `timeout`: Maximum duration to wait.
    ///
    /// # Returns
    /// `Ok(true)` once all tracked topics are idle, or `Ok(false)` when the
    /// timeout elapses first.
    pub(super) fn wait_for_all_idle_timeout(&self, timeout: Duration) -> EventBusResult<bool> {
        let started_at = Instant::now();
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        while !counts.is_empty() {
            let Some(remaining) = remaining_timeout(started_at, timeout) else {
                return Ok(false);
            };
            let (next_counts, timeout_result) = match self.condvar.wait_timeout(counts, remaining) {
                Ok(result) => result,
                Err(poisoned) => poisoned.into_inner(),
            };
            counts = next_counts;
            if timeout_result.timed_out() && !counts.is_empty() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Returns whether any topic still has active or queued processing work.
    pub(super) fn has_active(&self) -> EventBusResult<bool> {
        Ok(!self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?
            .is_empty())
    }
}

/// Returns the remaining time before a timeout elapses.
///
/// # Parameters
/// - `started_at`: Time when the wait began.
/// - `timeout`: Total timeout budget.
///
/// # Returns
/// Remaining duration, or `None` when the timeout has elapsed.
fn remaining_timeout(started_at: Instant, timeout: Duration) -> Option<Duration> {
    timeout.checked_sub(started_at.elapsed())
}
