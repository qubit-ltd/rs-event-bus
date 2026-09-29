// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Focused owner for local provider state.

use std::sync::Arc;

use crate::spi::TransportPayload;

/// Cloneable transport payload forms used by the local queue.

#[derive(Clone)]
pub(in crate::local) enum SharedPayload {
    /// Native value shared without a payload clone or serialization.
    Native(Arc<dyn std::any::Any + Send + Sync>),
}

impl SharedPayload {
    /// Copies a supported provider payload into its shareable local form.
    ///
    /// # Parameters
    /// - `payload`: payload accepted by the local provider.
    ///
    /// # Returns
    /// Shared native payload storage.
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
    pub(in crate::local) fn to_transport(&self) -> TransportPayload {
        match self {
            Self::Native(value) => TransportPayload::Native(value.clone()),
        }
    }
}
