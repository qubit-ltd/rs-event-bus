// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Type-erased topic key.

use std::any::TypeId;

/// Hashable key used internally to separate topics by name and payload type.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TopicKey {
    pub(crate) name: String,
    pub(crate) payload_type_id: TypeId,
}

impl TopicKey {
    /// Creates a topic key.
    ///
    /// # Parameters
    /// - `name`: Topic name.
    /// - `payload_type_id`: Rust [`TypeId`] of the payload.
    ///
    /// # Returns
    /// A new key suitable for hash maps.
    pub(crate) fn new(name: String, payload_type_id: TypeId) -> Self {
        Self { name, payload_type_id }
    }
}
