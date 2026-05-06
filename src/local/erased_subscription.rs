/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Type-erased subscription entry trait.

use std::any::Any;
use std::sync::Arc;

use crate::EventBusResult;

use super::local_event_bus_inner::LocalEventBusInner;

/// Type-erased subscription entry stored in the local event bus.
pub(crate) trait ErasedSubscription: Send + Sync {
    /// Returns subscription ID.
    fn id(&self) -> usize;

    /// Returns subscription priority.
    fn priority(&self) -> i32;

    /// Dispatches a boxed envelope to the subscription.
    fn dispatch(
        &self,
        envelope: Box<dyn Any + Send>,
        bus: Arc<LocalEventBusInner>,
    ) -> EventBusResult<()>;
}
