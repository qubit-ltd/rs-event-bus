// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private state for sharing a subscription worker join result.

use std::thread::JoinHandle;

/// Tracks worker registration, join ownership, and the cached exit result.
pub(super) enum WorkerJoinState {
    /// No worker handle has been registered.
    Unregistered,
    /// A worker handle is ready for one caller to join.
    Ready {
        /// Thread handle retained for the unique join owner.
        worker: JoinHandle<()>,
    },
    /// One caller is currently joining the worker.
    Joining,
    /// The worker has been joined and its panic classification is cached.
    Joined {
        /// Whether the worker thread panicked.
        panicked: bool,
    },
}
