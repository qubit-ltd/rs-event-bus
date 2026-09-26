// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Default capacity for bounded notification publisher queues.

/// Default number of notifications accepted ahead of the worker.
pub(crate) const DEFAULT_QUEUE_CAPACITY: usize = 256;
