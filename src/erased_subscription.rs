/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Type-erased subscription entry trait.

use std::any::Any;
use std::sync::Arc;

use crate::EventBusResult;
use crate::local_event_bus_inner::LocalEventBusInner;

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
