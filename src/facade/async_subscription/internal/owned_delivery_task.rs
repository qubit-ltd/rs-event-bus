// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Handler tasks that return delivery ownership to the session owner.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use super::PendingDelivery;

/// A handler/retry task that does not own the provider receiver.
///
/// # Type Parameters
/// - `T`: Delivery payload retained by the owned handler future; the `T:
///   'static` bound prevents the task from borrowing payload data from its
///   caller.
pub(in crate::facade) struct OwnedDeliveryTask<T: 'static> {
    /// Produces the same pending delivery after handler work completes.
    pub(in crate::facade) future: Pin<Box<dyn Future<Output = PendingDelivery<T>> + Send>>,
    /// Marks whether user code started before an immediate stop.
    pub(in crate::facade) started: Arc<AtomicBool>,
}
