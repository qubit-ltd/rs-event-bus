// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private state shared by notification publisher modules.

mod worker_state;

pub(in crate::notification) use worker_state::WorkerState;
