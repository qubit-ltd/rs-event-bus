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

fn panic_message(payload: &(dyn Any + Send)) -> Box<str> {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str().into()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).into()
    } else {
        "non-string panic payload".into()
    }
}
