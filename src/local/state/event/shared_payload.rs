// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shareable native payload storage used by local queues.

use std::sync::Arc;

use crate::spi::TransportPayload;

/// Cloneable transport payload forms used by the local queue.
#[derive(Clone)]
pub(in crate::local) enum SharedPayload {
    /// Native value shared without a payload clone or serialization.
    Native(
        /// Type-erased native allocation shared by queued and in-flight copies.
        Arc<dyn std::any::Any + Send + Sync>,
    ),
}

impl SharedPayload {
    /// Stores a native provider payload in its shareable local form.
    ///
    /// This clones the payload's `Arc` handle; it does not clone or serialize
    /// the underlying value. Encoded payloads are rejected by the local SPI's
    /// native-payload contract.
    ///
    /// # Parameters
    /// - `payload`: borrowed SPI payload, which must contain a native value.
    ///
    /// # Returns
    /// Shared native payload storage.
    ///
    /// # Panics
    /// Panics if `payload` is encoded, violating the local SPI's native-payload
    /// contract.
    #[must_use = "use the shared payload in local queue state"]
    #[inline]
    pub(in crate::local) fn from_transport(payload: &TransportPayload) -> Self {
        match payload {
            TransportPayload::Native(value) => Self::Native(value.clone()),
            TransportPayload::Encoded(_) => {
                unreachable!("encoded payloads are rejected by local SPI")
            }
        }
    }

    /// Reconstructs an SPI payload while retaining the same native allocation.
    ///
    /// # Returns
    /// A native SPI payload sharing the original allocation.
    #[must_use = "use the reconstructed SPI payload"]
    #[inline]
    pub(in crate::local) fn to_transport(&self) -> TransportPayload {
        match self {
            Self::Native(value) => TransportPayload::Native(value.clone()),
        }
    }
}
