// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private state shared by notification publisher modules.

pub(in crate::notification) use worker_completion_guard::WorkerCompletionGuard;
pub(in crate::notification) use worker_exit::WorkerExit;
pub(in crate::notification) use worker_state::WorkerState;

mod worker_completion_guard;
mod worker_exit;
mod worker_state;
