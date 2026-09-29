// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared source adapter for cached provider failures.

use std::sync::Arc;

use crate::error::SpiError;

/// Shares an original provider error without formatting away its source chain.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub(super) struct SharedSpiError {
    /// Original cached failure shared by all callers without source conversion.
    #[source]
    error: Arc<SpiError>,
}

impl SharedSpiError {
    /// Retains the complete cached provider error as an error-chain source.
    ///
    /// # Parameters
    /// - `error`: shared cached failure whose source chain must remain intact.
    ///
    /// # Returns
    /// A source adapter sharing the original failure without converting it.
    pub(super) fn new(error: Arc<SpiError>) -> Self {
        Self { error }
    }
}
