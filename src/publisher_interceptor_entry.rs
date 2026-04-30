/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Type-erased publisher interceptor entry trait.

use std::any::{Any, TypeId};

use crate::EventBusResult;

/// Type-erased publisher interceptor entry stored in the local event bus.
pub(crate) trait PublisherInterceptorEntry: Send + Sync {
    /// Returns the payload type handled by this interceptor.
    fn payload_type_id(&self) -> TypeId;

    /// Applies the interceptor to a boxed envelope.
    fn intercept(
        &self,
        envelope: Box<dyn Any + Send>,
    ) -> EventBusResult<Option<Box<dyn Any + Send>>>;
}
