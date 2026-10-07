// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors returned while registering payload codecs.

/// Describes a codec registration that conflicts with an existing entry.
#[must_use]
#[derive(Debug, thiserror::Error)]
pub enum CodecRegistrationError {
    /// A codec has already been registered for the payload type.
    #[error("a codec is already registered for payload type `{type_name}`")]
    DuplicatePayloadType {
        /// Rust type name for which registration was attempted.
        type_name: &'static str,
    },
}
