// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Type-erased subscription entry trait.

use std::any::Any;
use std::sync::Arc;

use super::local_event_bus_inner::LocalEventBusInner;
use crate::EventBusResult;

/// Result of admitting one subscription delivery.
pub(crate) enum DispatchAdmission {
    Accepted,
    Filtered,
}

/// Type-erased subscription entry stored in the local event bus.
pub(crate) trait ErasedSubscription: Send + Sync {
    /// Returns subscription ID.
    fn id(&self) -> usize;

    /// Returns the application subscriber identifier.
    fn subscriber_id(&self) -> &str;

    /// Returns subscription priority.
    fn priority(&self) -> i32;

    /// Marks the subscription inactive without removing it from storage.
    fn deactivate(&self);

    /// Dispatches a boxed envelope to the subscription.
    fn dispatch(
        &self,
        envelope: Box<dyn Any + Send>,
        bus: Arc<LocalEventBusInner>,
        allow_stopping: bool,
    ) -> EventBusResult<DispatchAdmission>;
}
