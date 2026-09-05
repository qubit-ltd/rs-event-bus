// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Type-erased subscriber interceptor entry trait.

use std::any::Any;
use std::any::TypeId;

use crate::EventBusResult;

/// Type-erased subscriber interceptor entry stored in the local event bus.
pub(crate) trait SubscriberInterceptorEntry: Send + Sync {
    /// Returns the payload type handled by this interceptor.
    fn payload_type_id(&self) -> TypeId;

    /// Wraps a boxed subscriber handler with this interceptor.
    fn wrap_handler(&self, handler: Box<dyn Any + Send + Sync>) -> EventBusResult<Box<dyn Any + Send + Sync>>;
}
