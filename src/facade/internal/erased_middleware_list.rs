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

/// Shared handle to a middleware list whose concrete type is erased.
///
/// The `Arc` lets facade storage clone the handle without copying the list,
/// while the `Any + Send + Sync` bounds allow its concrete list type to be
/// recovered by downcasting and shared across threads. This alias does not
/// itself store a payload ID; callers associate the handle with the relevant
/// payload.
pub(in crate::facade) type ErasedMiddlewareList = Arc<dyn Any + Send + Sync>;
