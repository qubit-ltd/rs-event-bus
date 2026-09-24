// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Publisher interceptor adapters and portable metadata views.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use crate::error::PublishError;
use crate::model::PublishMetadata;

/// Return `true` to continue or `false` to drop publication before SPI.
pub(crate) type GlobalPublisherInterceptorFn =
    dyn Fn(&mut PublishMetadata) -> Result<bool, PublishError> + Send + Sync + 'static;

/// One global interceptor restricted to portable metadata.
#[derive(Clone)]
pub(crate) struct GlobalPublisherInterceptor(Arc<GlobalPublisherInterceptorFn>);

impl GlobalPublisherInterceptor {
    /// Wraps a global metadata interceptor.
    pub(crate) fn new<F>(callback: F) -> Self
    where
        F: Fn(&mut PublishMetadata) -> Result<bool, PublishError> + Send + Sync + 'static,
    {
        Self(Arc::new(callback))
    }

    /// Applies this interceptor and converts panic into a structured failure.
    pub(crate) fn apply(&self, metadata: &mut PublishMetadata) -> Result<bool, PublishError> {
        match std::panic::catch_unwind(AssertUnwindSafe(|| (self.0)(metadata))) {
            Ok(result) => result,
            Err(payload) => Err(PublishError::InterceptorPanicked {
                scope: "global",
                message: panic_message(payload.as_ref()).into(),
            }),
        }
    }
}

/// Converts a panic payload into stable diagnostic text.
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

#[cfg(test)]
mod tests {
    use super::GlobalPublisherInterceptor;
    use crate::model::PublishMetadata;

    #[test]
    fn interceptor_converts_string_panic_to_structured_error() {
        let interceptor =
            GlobalPublisherInterceptor::new(|_: &mut PublishMetadata| -> Result<bool, crate::error::PublishError> {
                panic!("publisher middleware failed")
            });
        let error = interceptor
            .apply(&mut PublishMetadata::default())
            .expect_err("panic should become a structured error");
        assert!(matches!(
            error,
            crate::error::PublishError::InterceptorPanicked {
                scope: "global",
                message
            } if message.as_ref() == "publisher middleware failed"
        ));
    }

    #[test]
    fn interceptor_handles_non_string_panic_payload() {
        let interceptor =
            GlobalPublisherInterceptor::new(|_: &mut PublishMetadata| -> Result<bool, crate::error::PublishError> {
                std::panic::panic_any(17_u8)
            });
        let error = interceptor
            .apply(&mut PublishMetadata::default())
            .expect_err("panic should become a structured error");
        assert!(matches!(
            error,
            crate::error::PublishError::InterceptorPanicked { message, .. }
                if message.as_ref() == "non-string panic payload"
        ));
    }
}
