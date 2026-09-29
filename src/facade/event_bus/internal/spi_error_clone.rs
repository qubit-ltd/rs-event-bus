// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Clones a cached SPI error while retaining its provider context.

use std::sync::Arc;

use crate::error::SpiError;

/// Rebuilds a provider error for another shutdown caller.
///
/// The source chain is represented by its display text because provider error
/// trait objects are not cloneable.
///
/// # Parameters
/// - `error`: cached shutdown failure to copy.
///
/// # Returns
/// An equivalent error with copied operation metadata and source text.
pub(in crate::facade) fn clone_spi_error(error: Arc<SpiError>) -> SpiError {
    match error.as_ref() {
        SpiError::Operation {
            provider_id,
            operation,
            resource,
            kind,
            retryable,
            source,
        } => SpiError::Operation {
            provider_id: provider_id.clone(),
            operation,
            resource: resource.clone(),
            kind,
            retryable: *retryable,
            source: Box::new(std::io::Error::other(source.to_string())),
        },
        SpiError::InvalidSettlementToken {
            provider_id,
            operation,
            resource,
            reason,
            retryable,
            source,
        } => SpiError::InvalidSettlementToken {
            provider_id: provider_id.clone(),
            operation,
            resource: resource.clone(),
            reason,
            retryable: *retryable,
            source: Box::new(std::io::Error::other(source.to_string())),
        },
    }
}
