// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Clones a cached SPI error while retaining its provider context.

use std::sync::Arc;

use super::shared_spi_error::SharedSpiError;
use crate::error::SpiError;

/// Rebuilds a provider error for another shutdown caller.
///
/// The original provider failure is shared so its complete source chain remains
/// available to every caller.
///
/// # Parameters
/// - `error`: cached shutdown failure to copy.
///
/// # Returns
/// An equivalent error with copied operation metadata and source chain.
pub(in crate::facade) fn clone_spi_error(error: Arc<SpiError>) -> SpiError {
    match error.as_ref() {
        SpiError::Publish {
            provider_id,
            resource,
            kind,
            retryable,
            effect,
            ..
        } => SpiError::Publish {
            provider_id: provider_id.clone(),
            resource: resource.clone(),
            kind,
            retryable: *retryable,
            effect: *effect,
            source: Box::new(SharedSpiError::new(Arc::clone(&error))),
        },
        SpiError::Operation {
            provider_id,
            operation,
            resource,
            kind,
            retryable,
            source: _,
        } => SpiError::Operation {
            provider_id: provider_id.clone(),
            operation,
            resource: resource.clone(),
            kind,
            retryable: *retryable,
            source: Box::new(SharedSpiError::new(Arc::clone(&error))),
        },
        SpiError::InvalidSettlementToken {
            provider_id,
            operation,
            resource,
            reason,
            retryable,
            source: _,
        } => SpiError::InvalidSettlementToken {
            provider_id: provider_id.clone(),
            operation,
            resource: resource.clone(),
            reason,
            retryable: *retryable,
            source: Box::new(SharedSpiError::new(Arc::clone(&error))),
        },
    }
}
