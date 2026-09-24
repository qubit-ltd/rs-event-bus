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
use crate::error::PublishAttemptError;
use crate::error::PublishError;

/// A terminal publish failure observer; callbacks run in registration order.
pub type PublishErrorHandler<T> = dyn Fn(&PublishFailureContext<T>, &PublishError) + Send + Sync + 'static;
/// A typed publisher interceptor that may transform or drop an envelope.
pub type PublisherInterceptor<T> =
    dyn Fn(EventEnvelope<T>) -> Result<Option<EventEnvelope<T>>, PublishError> + Send + Sync + 'static;

/// Immutable options applied to one publication request.
pub struct PublishOptions<T: 'static> {
    pub(crate) retry_policy: Option<RetryPolicy>,
    pub(crate) retry_rule: Option<Arc<dyn RetryRule<PublishAttemptError>>>,
    pub(crate) retry_cancellation_token: Option<RetryCancellationToken>,
    pub(crate) error_handlers: Vec<Arc<PublishErrorHandler<T>>>,
    pub(crate) interceptors: Vec<Arc<PublisherInterceptor<T>>>,
}

impl<T: 'static> Default for PublishOptions<T> {
    fn default() -> Self {
        Self {
            retry_policy: None,
            retry_rule: None,
            retry_cancellation_token: None,
            error_handlers: Vec::new(),
            interceptors: Vec::new(),
        }
    }
}

impl<T: 'static> Clone for PublishOptions<T> {
    fn clone(&self) -> Self {
        Self {
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
    pub fn new() -> Self {
        Self::default()
    }
    /// Starts a builder for reusable publication policy.
    pub fn builder() -> PublishOptionsBuilder<T> {
        PublishOptionsBuilder::new()
    }
    /// Returns retry policy, or `None` when retry is disabled.
    pub fn retry_policy(&self) -> Option<&RetryPolicy> {
        self.retry_policy.as_ref()
    }
    /// Returns custom retry classification, or `None` for default
    /// classification.
    pub fn retry_rule(&self) -> Option<&Arc<dyn RetryRule<PublishAttemptError>>> {
        self.retry_rule.as_ref()
    }
    /// Returns a cancellation token, or `None` when absent.
    pub fn retry_cancellation_token(&self) -> Option<&RetryCancellationToken> {
        self.retry_cancellation_token.as_ref()
    }
    /// Returns terminal publish failure observers in registration order.
    pub fn error_handlers(&self) -> &[Arc<PublishErrorHandler<T>>] {
        &self.error_handlers
    }
    /// Returns the number of registered publish error callbacks.
    pub fn error_handler_count(&self) -> usize {
        self.error_handlers.len()
    }
    /// Returns typed publisher interceptors in registration order.
    pub fn interceptors(&self) -> &[Arc<PublisherInterceptor<T>>] {
        &self.interceptors
    }
}

/// Builds reusable publication policy independently of a request.
pub struct PublishOptionsBuilder<T: 'static> {
    options: PublishOptions<T>,
}

impl<T: 'static> PublishOptionsBuilder<T> {
    /// Starts with default publication policy.
    pub fn new() -> Self {
        Self {
            options: PublishOptions::default(),
        }
    }
    /// Replaces the retry policy from `qubit-retry`.
    pub fn retry_policy(mut self, value: RetryPolicy) -> Self {
        self.options.retry_policy = Some(value);
        self
    }
    /// Replaces the typed retry rule from `qubit-retry`.
    pub fn retry_rule<R: RetryRule<PublishAttemptError>>(mut self, value: R) -> Self {
        self.options.retry_rule = Some(Arc::new(value));
        self
    }
    /// Replaces the retry cancellation token.
    pub fn retry_cancellation_token(mut self, value: RetryCancellationToken) -> Self {
        self.options.retry_cancellation_token = Some(value);
        self
    }
    /// Appends a terminal publish failure handler in registration order.
    ///
    /// The handler runs when the SPI publish operation fails directly (with no
    /// retry policy) or when configured retries reach a terminal error. It does
    /// not run for request, capability, codec, or interceptor preflight errors.
    /// It receives the original payload and event metadata, returns no action,
    /// and cannot change or retry the publication outcome. A panic is isolated;
    /// later handlers still run and the returned publish error records it.
    pub fn error_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(&PublishFailureContext<T>, &PublishError) + Send + Sync + 'static,
    {
        self.options.error_handlers.push(Arc::new(handler));
        self
    }
    /// Appends an interceptor after previously registered interceptors.
    pub fn interceptor<F>(mut self, value: F) -> Self
    where
        F: Fn(EventEnvelope<T>) -> Result<Option<EventEnvelope<T>>, PublishError> + Send + Sync + 'static,
    {
        self.options.interceptors.push(Arc::new(value));
        self
    }
    /// Finishes construction; request builders validate policy combinations.
    pub fn build(self) -> PublishOptions<T> {
        self.options
    }
}

impl<T: 'static> Default for PublishOptionsBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}
