/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Type-erased events staged for transactional publishing.

use std::any::{
    Any,
    TypeId,
};

use crate::EventEnvelopeMetadata;

/// Type-erased event staged inside a transaction.
pub trait StagedEvent: Send + 'static {
    /// Returns type-erased event metadata.
    ///
    /// # Returns
    /// Metadata copied from the staged envelope.
    fn metadata(&self) -> EventEnvelopeMetadata;

    /// Returns the payload [`TypeId`].
    ///
    /// # Returns
    /// Runtime type ID of the staged payload.
    fn payload_type_id(&self) -> TypeId;

    /// Returns the staged event as [`Any`] for typed downcasting.
    ///
    /// # Returns
    /// Type-erased reference to the concrete staged event.
    fn as_any(&self) -> &dyn Any;

    /// Converts the staged event into [`Any`] for typed ownership recovery.
    ///
    /// # Returns
    /// Type-erased boxed concrete staged event.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send>;
}
