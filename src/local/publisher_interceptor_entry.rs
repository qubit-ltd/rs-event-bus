/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
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
