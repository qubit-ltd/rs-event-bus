// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! A single cancelable asynchronous shutdown waker registration.
use std::task::Context;
use std::task::Poll;

use super::ShutdownResult;
use crate::facade::shutdown_coordinator::ShutdownCoordinator;

/// Owns the waker registration for one asynchronous shutdown observer.
///
/// The registration borrows its coordinator for `'ticket` and is scoped to one
/// shutdown generation. It does not own or release the shutdown ticket or its
/// result. Dropping it unregisters only its own waker, if polling registered
/// one.
///
/// # Type Parameters
///
/// * `'ticket` — lifetime of the borrowed shutdown coordinator.
#[must_use = "poll the shutdown registration or drop it to cancel its waker"]
pub(in crate::facade) struct ShutdownRegistration<'ticket> {
    /// Coordinator that owns this generation's shutdown state and wakers.
    coordinator: &'ticket ShutdownCoordinator,
    /// Exact shutdown generation observed by this registration.
    generation: u64,
    /// Registration token, populated after the first pending poll.
    token: Option<u64>,
}
impl<'ticket> ShutdownRegistration<'ticket> {
    /// Creates an unregistered observer for one exact shutdown generation.
    ///
    /// The observer borrows the coordinator for `'ticket`; it does not acquire
    /// or release a shutdown ticket. A waker is registered only if `poll`
    /// finds that this generation is still active and has no result.
    ///
    /// # Parameters
    ///
    /// * `coordinator` — state owner used to inspect completion and register
    ///   this observer's waker.
    /// * `generation` — generation whose shutdown result this observer awaits.
    ///
    /// # Returns
    ///
    /// An observer with no waker registration yet.
    #[inline]
    pub(in crate::facade) fn new(coordinator: &'ticket ShutdownCoordinator, generation: u64) -> Self {
        Self {
            coordinator,
            generation,
            token: None,
        }
    }
    /// Checks this generation's completion and registers or refreshes its
    /// waker.
    ///
    /// A completed generation returns its cloned result. If the generation is
    /// no longer active and has no retained result, this returns `Ready(None)`.
    /// Otherwise, the current task's waker is stored and this returns
    /// `Pending`; a later poll may replace that registration's waker.
    ///
    /// # Parameters
    ///
    /// * `cx` — task context supplying the waker to notify when shutdown
    ///   completes.
    ///
    /// # Returns
    ///
    /// * `Ready(Some(result))` when this generation has completed.
    /// * `Ready(None)` when this generation is no longer active and has no
    ///   retained result.
    /// * `Pending` while this generation remains active without a result.
    pub(in crate::facade) fn poll(&mut self, cx: &Context<'_>) -> Poll<Option<ShutdownResult>> {
        self.coordinator.poll_result(self.generation, &mut self.token, cx)
    }
}
impl Drop for ShutdownRegistration<'_> {
    /// Removes this observer's waker registration, if one was created.
    fn drop(&mut self) {
        if let Some(token) = self.token {
            self.coordinator.unregister(self.generation, token);
        }
    }
}
