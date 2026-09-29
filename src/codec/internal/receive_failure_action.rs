// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One receive policy shared by synchronous and asynchronous facade owners.
use crate::error::CodecError;

/// Whether a failure identifies a bad message or an unsafe receive contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReceiveFailureAction {
    /// Reject a deterministically malformed message and continue receiving.
    Reject,
    /// Stop this receiver while leaving its source token unsettled.
    StopUnsettled,
}

/// Classifies a codec failure without inspecting error text or payload bytes.
/// Decode failures reject; limits, metadata, native type and panic failures
/// stop.
pub(crate) fn receive_failure_action(error: &CodecError) -> ReceiveFailureAction {
    match error {
        CodecError::Decode { .. } => ReceiveFailureAction::Reject,
        _ => ReceiveFailureAction::StopUnsettled,
    }
}
