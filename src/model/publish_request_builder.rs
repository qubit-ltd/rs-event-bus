//! Complete publication request construction and validation.

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use super::EventEnvelope;
use super::EventId;
use super::FailureDirective;
use super::Headers;
use super::PublishOptions;
use super::PublishRequest;
use super::Topic;
use crate::error::PublishAttemptError;
use crate::error::PublishError;

/// Invalid publication request metadata or policy.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PublishRequestBuildError {
    /// A required request field was omitted.
    #[error("missing required publish request field: {0}")]
    MissingField(&'static str),
    /// A header key or value is invalid.
    #[error("invalid publish header {0:?}")]
    InvalidHeader(String),
    /// The ordering key is empty or contains controls.
    #[error("invalid publish ordering key")]
    InvalidOrderingKey,
    /// Retry classification or cancellation requires a retry policy.
    #[error("retry rule or cancellation token requires a retry policy")]
    InvalidRetryConfiguration,
}

/// Builds one publication request. Scalars use their last value, headers merge
/// by key, and handlers or interceptors append in call order. `options`
/// replaces policy at its call position; later policy calls then override or
/// append.
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::model::{PublishRequest, Topic};
/// let topic = Topic::<String>::new("orders.created")?;
/// let request = PublishRequest::builder()
///     .topic(topic)
///     .payload("one".to_owned())
///     .header("source", "first")
///     .headers([("source", "second")])
///     .build()?;
/// assert_eq!(request.header("source"), Some("second"));
/// # Ok(())
/// # }
/// ```
pub struct PublishRequestBuilder<T: 'static> {
    topic: Option<Topic<T>>,
    payload: Option<T>,
    event_id: Option<EventId>,
    headers: Headers,
    ordering_key: Option<String>,
    timestamp: Option<SystemTime>,
    delay: Option<Duration>,
    options: PublishOptions<T>,
}

impl<T: Send + Sync + 'static> PublishRequestBuilder<T> {
    /// Starts an empty builder requiring topic and payload.
    pub fn new() -> Self {
        Self {
            topic: None,
            payload: None,
            event_id: None,
            headers: Headers::new(),
            ordering_key: None,
            timestamp: None,
            delay: None,
            options: PublishOptions::default(),
        }
    }
    /// Replaces the typed topic.
    pub fn topic(mut self, value: Topic<T>) -> Self {
        self.topic = Some(value);
        self
    }
    /// Replaces the payload without requiring `T: Clone`.
    pub fn payload(mut self, value: T) -> Self {
        self.payload = Some(value);
        self
    }
    /// Replaces the event ID.
    pub fn event_id(mut self, value: EventId) -> Self {
        self.event_id = Some(value);
        self
    }
    /// Adds or replaces a header by key. Validation occurs in `build`.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
    /// Merges headers in iteration order, replacing earlier values by key.
    pub fn headers<I, K, V>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.headers
            .extend(values.into_iter().map(|(key, value)| (key.into(), value.into())));
        self
    }
    /// Replaces the ordering key. Validation occurs in `build`.
    pub fn ordering_key(mut self, value: impl Into<String>) -> Self {
        self.ordering_key = Some(value.into());
        self
    }
    /// Replaces the envelope creation timestamp.
    pub fn timestamp(mut self, value: SystemTime) -> Self {
        self.timestamp = Some(value);
        self
    }
    /// Replaces the requested delivery delay.
    pub fn delay(mut self, value: Duration) -> Self {
        self.delay = Some(value);
        self
    }
    /// Replaces the retry policy directly from `qubit-retry`.
    pub fn retry_policy(mut self, value: RetryPolicy) -> Self {
        self.options.retry_policy = Some(value);
        self
    }
    /// Replaces the typed retry rule directly from `qubit-retry`.
    pub fn retry_rule<R>(mut self, value: R) -> Self
    where
        R: RetryRule<PublishAttemptError>,
    {
        self.options.retry_rule = Some(Arc::new(value));
        self
    }
    /// Replaces the shared retry cancellation token.
    pub fn retry_cancellation_token(mut self, value: RetryCancellationToken) -> Self {
        self.options.retry_cancellation_token = Some(value);
        self
    }
    /// Appends a publish error handler after previously registered handlers.
    pub fn error_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(&EventEnvelope<T>, &PublishError) -> FailureDirective + Send + Sync + 'static,
    {
        self.options.error_handlers.push(Arc::new(handler));
        self
    }
    /// Appends a typed publisher interceptor in registration order.
    pub fn interceptor<F>(mut self, value: F) -> Self
    where
        F: Fn(EventEnvelope<T>) -> Result<Option<EventEnvelope<T>>, PublishError> + Send + Sync + 'static,
    {
        self.options.interceptors.push(Arc::new(value));
        self
    }
    /// Replaces all policy state; later policy calls override or append.
    pub fn options(mut self, value: PublishOptions<T>) -> Self {
        self.options = value;
        self
    }

    /// Validates required fields, headers and policy, then creates a request.
    pub fn build(self) -> Result<PublishRequest<T>, PublishRequestBuildError> {
        let topic = self.topic.ok_or(PublishRequestBuildError::MissingField("topic"))?;
        let payload = self.payload.ok_or(PublishRequestBuildError::MissingField("payload"))?;
        for (key, value) in &self.headers {
            if key.is_empty()
                || !key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
                || value.chars().any(char::is_control)
            {
                return Err(PublishRequestBuildError::InvalidHeader(key.clone()));
            }
        }
        if self
            .ordering_key
            .as_ref()
            .is_some_and(|key| key.is_empty() || key.trim() != key || key.chars().any(char::is_control))
        {
            return Err(PublishRequestBuildError::InvalidOrderingKey);
        }
        if self.options.retry_policy.is_none()
            && (self.options.retry_rule.is_some() || self.options.retry_cancellation_token.is_some())
        {
            return Err(PublishRequestBuildError::InvalidRetryConfiguration);
        }
        let mut envelope = EventEnvelope::new(topic, payload);
        if let Some(id) = self.event_id {
            envelope.id = id;
        }
        envelope.headers = self.headers;
        envelope.ordering_key = self.ordering_key.map(String::into_boxed_str);
        if let Some(timestamp) = self.timestamp {
            envelope.timestamp = timestamp;
        }
        envelope.delay = self.delay;
        Ok(PublishRequest::from_envelope(envelope).with_options(self.options))
    }
}

impl<T: Send + Sync + 'static> Default for PublishRequestBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}
