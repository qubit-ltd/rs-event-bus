// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider message settlement capabilities.

/// Settlement operations supported by a provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SettlementCapabilities {
    /// Messages cannot be settled.
    None,
    /// Only acknowledge/accept is supported.
    AcceptOnly,
    /// Accept, retry, and reject are supported.
    AcceptRetryReject,
}
