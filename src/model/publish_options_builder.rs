// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Construction of reusable publication policy values.

use std::num::NonZeroUsize;
use std::sync::Arc;

use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use super::EventEnvelope;
use super::PublishFailureContext;
use super::PublishOptions;
use crate::error::PublishAttemptError;
use crate::error::PublishError;
use crate::error::PublishFailure;
use crate::model::DuplicateRiskPolicy;

/// Builds reusable publication policy independently of an individual request.
///
/// # Type Parameters
/// - `T`: payload type handled by the configured callbacks. It must be
///   `'static` because reusable options store callbacks that may observe this
///   payload type.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::PublishOptions;
///
/// let options = PublishOptions::<String>::builder().build();
/// assert!(options.retry_policy().is_none());
/// ```
#[must_use]
pub struct PublishOptionsBuilder<T: 'static> {
    /// Mutable retry and callback policy accumulated by builder methods.
    options: PublishOptions<T>,
}

impl<T: 'static> PublishOptionsBuilder<T> {
    /// Starts with default publication policy.
    ///
    /// # Returns
    /// A builder containing no retry policy or callbacks.
    #[inline]
    pub fn new() -> Self {
        Self {
            options: PublishOptions::default(),
        }
    }

    /// Sets whether configured retries may duplicate uncertain admission.
    ///
    /// # Parameters
    /// - `value`: whether configured retries may repeat uncertain admission.
    ///
    /// # Returns
    /// The updated builder; `Forbid` remains the default.
    #[inline]
    pub fn duplicate_risk_policy(mut self, value: DuplicateRiskPolicy) -> Self {
        self.options.duplicate_risk_policy = value;
        self
    }

    /// Replaces the retry policy from `qubit-retry`.
    ///
    /// # Parameters
    /// - `value`: policy that schedules and classifies publish attempts.
    ///
    /// # Returns
    /// The updated builder.
    #[inline]
    pub fn retry_policy(mut self, value: RetryPolicy) -> Self {
        self.options.retry_policy = Some(value);
        self
    }

    /// Replaces the typed retry rule from `qubit-retry`.
    ///
    /// # Type Parameters
    /// - `R`: retry rule implementation for publish attempt failures.
    ///
    /// # Parameters
    /// - `value`: rule used to classify attempt failures.
    ///
    /// # Returns
    /// The updated builder.
    pub fn retry_rule<R>(mut self, value: R) -> Self
    where
        R: RetryRule<PublishAttemptError>,
    {
        self.options.retry_rule = Some(Arc::new(value));
        self
    }

    /// Replaces the retry cancellation token.
    ///
    /// # Parameters
    /// - `value`: shared signal used to cancel retry waits.
    ///
    /// # Returns
    /// The updated builder.
    #[inline]
    pub fn retry_cancellation_token(mut self, value: RetryCancellationToken) -> Self {
        self.options.retry_cancellation_token = Some(value);
        self
    }

    /// Appends a terminal publish failure handler in registration order.
    ///
    /// The callback observes provider failures after retries become terminal.
    /// It does not run for request validation, capability, codec, or
    /// interceptor preflight errors. It cannot change the result. Panics
    /// are isolated and recorded while subsequent handlers still run.
    ///
    /// # Type Parameters
    /// - `F`: thread-safe terminal failure observer callable type.
    ///
    /// # Parameters
    /// - `handler`: callback receiving the failed event context and error.
    ///
    /// # Returns
    /// The updated builder.
    pub fn error_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(&PublishFailureContext<T>, &PublishFailure) + Send + Sync + 'static,
    {
        self.options.error_handlers.push(Arc::new(handler));
        self
    }

    /// Appends an interceptor after previously registered interceptors.
    ///
    /// # Type Parameters
    /// - `F`: thread-safe event transformation callable type.
    ///
    /// # Parameters
    /// - `value`: callback that may replace or drop the envelope.
    ///
    /// # Returns
    /// The updated builder.
    pub fn interceptor<F>(mut self, value: F) -> Self
    where
        F: Fn(EventEnvelope<T>) -> Result<Option<EventEnvelope<T>>, PublishError> + Send + Sync + 'static,
    {
        self.options.interceptors.push(Arc::new(value));
        self
    }

    /// Sets the callback declaring a native payload's weight in bytes.
    ///
    /// The callback runs once after publisher interceptors, before the first
    /// provider attempt. Retries reuse that value. Encoded-only providers skip
    /// this callback. A callback panic becomes a preflight
    /// [`PublishError::InterceptorPanicked`] with scope `payload_weight`.
    /// The declared weight is application metadata, not measured heap usage.
    ///
    /// # Type Parameters
    /// - `F`: thread-safe callback borrowing the final typed payload.
    ///
    /// # Parameters
    /// - `value`: callback returning a positive declared weight in bytes.
    ///
    /// # Returns
    /// The updated builder; clones of the resulting options share the callback.
    pub fn native_payload_weight<F>(mut self, value: F) -> Self
    where
        F: Fn(&T) -> NonZeroUsize + Send + Sync + 'static,
    {
        self.options.native_payload_weight = Some(Arc::new(value));
        self
    }

    /// Consumes the builder and returns the reusable publication policy.
    /// Request builders validate retry-policy combinations.
    ///
    /// # Returns
    /// The configured options value.
    #[inline]
    pub fn build(self) -> PublishOptions<T> {
        self.options
    }
}

impl<T: 'static> Default for PublishOptionsBuilder<T> {
    /// Creates a builder with default retry and callback settings.
    ///
    /// # Returns
    /// A builder with no retry policy or callbacks configured.
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}
