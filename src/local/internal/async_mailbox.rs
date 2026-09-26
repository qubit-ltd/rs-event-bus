// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Queue shared by the bus and one asynchronous subscription.

use std::sync::Arc;

use super::super::state::LocalQueue;

pub(in crate::local) struct AsyncMailbox {
    pub(in crate::local) queue: Arc<LocalQueue>,
}
