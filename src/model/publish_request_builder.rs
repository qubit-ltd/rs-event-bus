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
/// let request = PublishRequest::builder()
///     .topic(Topic::<String>::new("orders.old")?)
///     .topic(Topic::<String>::new("orders.created")?)
///     .payload("old".to_owned())
///     .payload("new".to_owned())
///     .header("source", "first")
///     .headers([("source", "second"), ("trace", "t-1")])
///     .header("source", "third")
///     .build()?;
/// assert_eq!(request.topic().name(), "orders.created");
/// assert_eq!(request.envelope().payload(), "new");
/// assert_eq!(request.header("source"), Some("third"));
/// assert_eq!(request.header("trace"), Some("t-1"));
/// # Ok(())
/// # }
/// ```
///
/// `options` discards earlier policy callbacks; callbacks added afterward are
/// appended to those in the replacement options, in call order:
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::sync::{Arc, Mutex};
/// use qubit_event_bus::PublishError;
/// use qubit_event_bus::model::{FailureDirective, PublishOptions, PublishRequest, Topic};
/// let calls = Arc::new(Mutex::new(Vec::new()));
/// let first = calls.clone();
/// let second = calls.clone();
/// let options = PublishOptions::<String>::builder()
///     .error_handler(move |_, _| {
///         first.lock().unwrap().push("options");
///         FailureDirective::Discard
///     })
///     .interceptor(move |event| {
///         second.lock().unwrap().push("interceptor-options");
///         Ok(Some(event))
///     })
///     .build();
/// let third = calls.clone();
/// let fourth = calls.clone();
/// let request = PublishRequest::builder()
///     .topic(Topic::<String>::new("orders.created")?)
///     .payload("one".to_owned())
///     .error_handler(|_, _| panic!("replaced by options"))
///     .interceptor(|_| panic!("replaced by options"))
///     .options(options)
///     .interceptor(move |event| {
///         third.lock().unwrap().push("interceptor-after");
///         Ok(Some(event))
///     })
///     .error_handler(move |_, _| {
///         fourth.lock().unwrap().push("after");
///         FailureDirective::Discard
///     })
///     .build()?;
/// let mut event = request.envelope().clone();
/// for interceptor in request.options().interceptors() {
///     event = interceptor(event)?.unwrap();
/// }
/// for handler in request.options().error_handlers() {
///     handler(&event, &PublishError::Closed);
/// }
/// assert_eq!(*calls.lock().unwrap(), [
///     "interceptor-options", "interceptor-after", "options", "after"
/// ]);
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

    /// Consumes the builder and creates a request, generating an event ID and
    /// timestamp when neither was supplied. Existing headers are merged by
    /// key; a later `options` call replaces earlier policy callbacks.
    ///
    /// # Errors
    /// Returns `MissingField` without a topic or payload, `InvalidHeader` for
    /// malformed header metadata, `InvalidOrderingKey` for a blank or control
    /// containing key, and `InvalidRetryConfiguration` when a rule or
    /// cancellation token has no retry policy. `Duration` is nonnegative, so
    /// zero delay is accepted.
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
