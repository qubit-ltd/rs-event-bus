// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Type-erased, shared storage for per-payload middleware lists.

use std::any::Any;
use std::sync::Arc;

/// Thread-safe shared type-erased middleware storage indexed by payload ID.
pub(in crate::facade) type ErasedMiddlewareList = Arc<dyn Any + Send + Sync>;
