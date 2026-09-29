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

use crate::facade::shutdown_coordinator::ShutdownCoordinator;
use crate::facade::shutdown_coordinator::ShutdownResult;

/// Owns only a future's waker, leaving ticket/result ownership unchanged.
pub(in crate::facade) struct ShutdownRegistration<'ticket> {
    coordinator: &'ticket ShutdownCoordinator,
    generation: u64,
    token: Option<u64>,
}
impl<'ticket> ShutdownRegistration<'ticket> {
    /// Creates an unregistered observer for the ticket's exact generation.
    pub(in crate::facade) fn new(coordinator: &'ticket ShutdownCoordinator, generation: u64) -> Self {
        Self {
            coordinator,
            generation,
            token: None,
        }
    }
    /// Checks completion and atomically installs/updates this observer's waker.
    pub(in crate::facade) fn poll(&mut self, cx: &Context<'_>) -> Poll<Option<ShutdownResult>> {
        self.coordinator.poll_result(self.generation, &mut self.token, cx)
    }
}
impl Drop for ShutdownRegistration<'_> {
    fn drop(&mut self) {
        if let Some(token) = self.token {
            self.coordinator.unregister(self.generation, token);
        }
    }
}
