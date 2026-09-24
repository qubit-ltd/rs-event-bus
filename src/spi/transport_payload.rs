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
#[non_exhaustive]
pub enum TransportPayload {
    /// In-process value that a facade can downcast to the topic payload type.
    Native(Arc<dyn Any + Send + Sync>),
    /// Serialized bytes and codec metadata for a portable transport.
    Encoded(EncodedPayload),
}
