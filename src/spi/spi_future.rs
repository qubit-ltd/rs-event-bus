// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral future type used by asynchronous backends.

use std::future::Future;
use std::pin::Pin;

/// A sendable boxed future without a dependency on a particular executor.
///
/// # Type Parameters
/// - `'a`: lifetime of data borrowed by the operation.
/// - `T`: value produced when the operation completes.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::SpiFuture;
///
/// fn ready_value<'a>() -> SpiFuture<'a, usize> {
///     Box::pin(async { 42 })
/// }
///
/// let _future = ready_value();
/// ```
pub type SpiFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
