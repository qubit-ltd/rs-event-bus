// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Payload forms accepted by transport backends.

use std::any::Any;
use std::sync::Arc;

use super::EncodedPayload;

/// Type-erased native or encoded event payload.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use qubit_event_bus::spi::TransportPayload;
///
/// let payload = TransportPayload::Native(Arc::new(String::from("order-42")));
/// assert!(matches!(payload, TransportPayload::Native(_)));
/// ```
#[non_exhaustive]
#[must_use]
pub enum TransportPayload {
    /// In-process value that a facade can downcast to the topic payload type.
    Native(
        /// Shared native payload allocation.
        Arc<dyn Any + Send + Sync>,
    ),
    /// Serialized bytes and codec metadata for a portable transport.
    Encoded(
        /// Encoded bytes and their media/schema metadata.
        EncodedPayload,
    ),
}
