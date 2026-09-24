// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Payload representations accepted by a provider.

/// Payload representations a provider can send and receive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PayloadModes {
    /// Native Rust values only.
    Native,
    /// Encoded bytes only.
    Encoded,
    /// Both native values and encoded bytes.
    NativeAndEncoded,
}
