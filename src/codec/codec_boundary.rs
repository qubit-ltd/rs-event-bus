// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Panic boundary for user-provided codec callbacks.

use std::any::Any;
use std::panic::AssertUnwindSafe;

use crate::error::CodecError;

/// Runs a codec callback and converts a panic into a structured error.
///
/// This keeps user codec panics inside the facade's error boundary while
/// preserving ordinary codec errors unchanged.
///
/// # Type Parameters
/// - `R`: the successful result produced by the callback.
///
/// # Parameters
/// - `operation`: the static operation name recorded when the callback panics.
/// - `call`: the user codec operation to execute.
///
/// # Returns
/// The callback result, or a [`CodecError::Panicked`] error when it unwinds.
///
/// # Errors
/// Returns the callback's [`CodecError`] unchanged, or a structured panic
/// error containing the operation and a readable panic message.
#[must_use = "the codec result must be handled by the caller"]
pub(crate) fn call_codec<R>(
    operation: &'static str,
    call: impl FnOnce() -> Result<R, CodecError>,
) -> Result<R, CodecError> {
    match std::panic::catch_unwind(AssertUnwindSafe(call)) {
        Ok(result) => result,
        Err(payload) => Err(CodecError::Panicked {
            operation,
            message: panic_message(payload.as_ref()),
        }),
    }
}

/// Extracts a readable message from a panic payload.
///
/// String payloads are copied; other payload types use a fixed fallback so
/// arbitrary panic objects are never formatted or exposed.
///
/// # Parameters
/// - `payload`: the payload returned by `catch_unwind`.
///
/// # Returns
/// An owned panic message, or a fallback description for non-string payloads.
#[must_use = "the panic message must be used to report the callback failure"]
fn panic_message(payload: &(dyn Any + Send)) -> Box<str> {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str().into()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).into()
    } else {
        "non-string panic payload".into()
    }
}
