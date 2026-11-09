// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors for portable event headers exceeding configured boundaries.

/// A portable header collection exceeded a configured limit.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum HeaderLimitError {
    /// The header count exceeded the configured maximum.
    #[error("header count exceeded limit {limit}")]
    CountExceeded {
        /// Maximum allowed count.
        limit: usize,
    },
    /// The aggregate UTF-8 byte count exceeded the configured maximum.
    #[error("header bytes exceeded limit {limit}")]
    BytesExceeded {
        /// Maximum allowed bytes.
        limit: usize,
    },
}
