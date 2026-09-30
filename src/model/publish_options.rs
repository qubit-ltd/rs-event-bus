// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Per-publication retry policy and terminal failure observers.

use std::sync::Arc;

use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use super::EventEnvelope;
use super::PublishFailureContext;
use super::PublishOptionsBuilder;
use crate::error::PublishAttemptError;
use crate::error::PublishError;
use crate::error::PublishFailure;
use crate::model::DuplicateRiskPolicy;

/// A terminal publish failure observer; callbacks run in registration order.
///
/// # Type Parameters
/// - `T`: payload type attached to the failed event.
pub type PublishErrorHandler<T> = dyn Fn(&PublishFailureContext<T>, &PublishFailure) + Send + Sync + 'static;
/// A typed publisher interceptor that may transform or drop an envelope.
///
/// # Type Parameters
/// - `T`: payload type processed by the interceptor.
pub type PublisherInterceptor<T> =
    dyn Fn(EventEnvelope<T>) -> Result<Option<EventEnvelope<T>>, PublishError> + Send + Sync + 'static;

/// Immutable options applied to one publication request.
///
/// # Type Parameters
/// - `T`: payload type carried by the publication.
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
pub struct PublishOptions<T: 'static> {
    /// Whether retry may repeat uncertain provider admission.
    pub(crate) duplicate_risk_policy: DuplicateRiskPolicy,
    /// Retry schedule, absent when application retries are disabled.
    pub(crate) retry_policy: Option<RetryPolicy>,
    /// Optional custom rule for classifying attempt failures.
    pub(crate) retry_rule: Option<Arc<dyn RetryRule<PublishAttemptError>>>,
    /// Shared cancellation signal for retry waits.
    pub(crate) retry_cancellation_token: Option<RetryCancellationToken>,
    /// Terminal failure observers in registration order.
    pub(crate) error_handlers: Vec<Arc<PublishErrorHandler<T>>>,
    /// Typed publication transformations in registration order.
    pub(crate) interceptors: Vec<Arc<PublisherInterceptor<T>>>,
}

impl<T: 'static> Default for PublishOptions<T> {
    /// Creates options without retry policies or callbacks.
    fn default() -> Self {
        Self {
            duplicate_risk_policy: DuplicateRiskPolicy::Forbid,
            retry_policy: None,
            retry_rule: None,
            retry_cancellation_token: None,
            error_handlers: Vec::new(),
            interceptors: Vec::new(),
        }
    }
}

impl<T: 'static> Clone for PublishOptions<T> {
    /// Clones policy values and shares callback allocations.
    fn clone(&self) -> Self {
        Self {
            duplicate_risk_policy: self.duplicate_risk_policy,
            retry_policy: self.retry_policy.clone(),
            retry_rule: self.retry_rule.clone(),
            retry_cancellation_token: self.retry_cancellation_token.clone(),
            error_handlers: self.error_handlers.clone(),
            interceptors: self.interceptors.clone(),
        }
    }
}

impl<T: 'static> PublishOptions<T> {
    /// Returns a default options value with no application retry.
    ///
    /// # Returns
    /// A new value equivalent to [`Self::default`].
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    /// Starts a builder for reusable publication policy.
    ///
    /// # Returns
    /// A builder initialized with default publication options.
    #[inline]
    pub fn builder() -> PublishOptionsBuilder<T> {
        PublishOptionsBuilder::new()
    }
    /// Returns whether uncertain admission may enter configured automatic
    /// retries.
    ///
    /// # Returns
    /// The duplicate risk policy; `Forbid` is the default.
    #[must_use]
    #[inline]
    pub fn duplicate_risk_policy(&self) -> DuplicateRiskPolicy {
        self.duplicate_risk_policy
    }

    /// Returns retry policy, or `None` when retry is disabled.
    ///
    /// # Returns
    /// The retry schedule when configured, otherwise `None`.
    #[must_use = "Use the returned retry policy."]
    #[inline]
    pub fn retry_policy(&self) -> Option<&RetryPolicy> {
        self.retry_policy.as_ref()
    }
    /// Returns custom retry classification, or `None` for default
    /// classification.
    ///
    /// # Returns
    /// The custom retry rule when configured, otherwise `None`.
    #[must_use = "Use the returned retry rule."]
    #[inline]
    pub fn retry_rule(&self) -> Option<&Arc<dyn RetryRule<PublishAttemptError>>> {
        self.retry_rule.as_ref()
    }
    /// Returns a cancellation token, or `None` when absent.
    ///
    /// # Returns
    /// The shared cancellation token when configured, otherwise `None`.
    #[must_use = "Use the returned retry cancellation token."]
    #[inline]
    pub fn retry_cancellation_token(&self) -> Option<&RetryCancellationToken> {
        self.retry_cancellation_token.as_ref()
    }
    /// Returns terminal publish failure observers in registration order.
    ///
    /// # Returns
    /// The callbacks in their registration order.
    #[must_use]
    #[inline]
    pub fn error_handlers(&self) -> &[Arc<PublishErrorHandler<T>>] {
        &self.error_handlers
    }
    /// Returns the number of registered publish error callbacks.
    ///
    /// # Returns
    /// The number of terminal failure observers.
    #[must_use]
    #[inline]
    pub fn error_handler_count(&self) -> usize {
        self.error_handlers.len()
    }
    /// Returns typed publisher interceptors in registration order.
    ///
    /// # Returns
    /// The transformations in their registration order.
    #[must_use]
    #[inline]
    pub fn interceptors(&self) -> &[Arc<PublisherInterceptor<T>>] {
        &self.interceptors
    }
}
