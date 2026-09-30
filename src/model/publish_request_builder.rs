// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Complete publication request construction and validation.

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use super::DEAD_LETTER_HEADER;
use super::EventEnvelope;
use super::EventId;
use super::Headers;
use super::PublishFailureContext;
use super::PublishOptions;
use super::PublishRequest;
use super::PublishRequestBuildError;
use super::Topic;
use crate::error::EventIdGenerationError;
use crate::error::PublishAttemptError;
use crate::error::PublishError;
use crate::error::PublishFailure;
use crate::model::DuplicateRiskPolicy;
use crate::util::validated_text::is_nonblank_without_controls;

/// Builds one publication request. Scalars use their last value, headers merge
/// by key, and handlers or interceptors append in call order. `options`
/// replaces policy at its call position; later policy calls then override or
/// append.
///
/// # Type Parameters
/// - `T`: payload type stored in the request.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::model::PublishRequest;
/// use qubit_event_bus::model::Topic;
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
/// use std::sync::Arc;
/// use std::sync::Mutex;
///
/// use qubit_event_bus::model::PublishOptions;
/// use qubit_event_bus::model::PublishRequest;
/// use qubit_event_bus::model::Topic;
/// let calls = Arc::new(Mutex::new(Vec::new()));
/// let first = calls.clone();
/// let second = calls.clone();
/// let options = PublishOptions::<String>::builder()
///     .error_handler(move |_, _| {
///         first.lock().unwrap().push("options");
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
///     })
///     .build()?;
/// let mut event = request.envelope().clone();
/// for interceptor in request.options().interceptors() {
///     event = interceptor(event)?.unwrap();
/// }
/// assert_eq!(*calls.lock().unwrap(), ["interceptor-options", "interceptor-after"]);
/// # Ok(())
/// # }
/// ```
#[must_use]
pub struct PublishRequestBuilder<T: 'static> {
    /// Topic selected for the event.
    topic: Option<Topic<T>>,
    /// Payload to place in the event envelope.
    payload: Option<T>,
    /// Explicit event identifier, when the caller supplied one.
    event_id: Option<EventId>,
    /// Header values merged by key while building the envelope.
    headers: Headers,
    /// Optional provider ordering key.
    ordering_key: Option<String>,
    /// Explicit envelope timestamp.
    timestamp: Option<SystemTime>,
    /// Requested provider delivery delay.
    delay: Option<Duration>,
    /// Retry, callback, and interceptor policy for this publish request.
    options: PublishOptions<T>,
}

impl<T: Send + Sync + 'static> PublishRequestBuilder<T> {
    /// Starts an empty builder requiring topic and payload.
    ///
    /// # Returns
    /// A builder with no topic or payload and default publish options.
    #[inline]
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
    /// Sets whether configured retries may duplicate uncertain admission.
    ///
    /// # Parameters
    /// - `value`: whether configured retries may repeat uncertain admission.
    ///
    /// # Returns
    /// The updated builder; `Forbid` remains the default.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn duplicate_risk_policy(mut self, value: DuplicateRiskPolicy) -> Self {
        self.options.duplicate_risk_policy = value;
        self
    }

    /// Replaces the typed topic.
    ///
    /// # Parameters
    /// - `value`: topic used by the request.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn topic(mut self, value: Topic<T>) -> Self {
        self.topic = Some(value);
        self
    }
    /// Replaces the payload without requiring `T: Clone`.
    ///
    /// # Parameters
    /// - `value`: payload to publish.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn payload(mut self, value: T) -> Self {
        self.payload = Some(value);
        self
    }
    /// Replaces the event ID.
    ///
    /// # Parameters
    /// - `value`: event identifier to place in the envelope.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn event_id(mut self, value: EventId) -> Self {
        self.event_id = Some(value);
        self
    }
    /// Adds or replaces a header by key. Validation occurs in `build`; reserved
    /// facade-owned headers are rejected there.
    ///
    /// # Parameters
    /// - `key`: header name.
    /// - `value`: header value.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
    /// Merges headers in iteration order, replacing earlier values by key.
    ///
    /// # Type Parameters
    /// - `I`: iterable collection of header pairs.
    /// - `K`: type convertible to a header key string.
    /// - `V`: type convertible to a header value string.
    ///
    /// # Parameters
    /// - `values`: header pairs to merge into the request.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
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
    ///
    /// # Parameters
    /// - `value`: key used to request ordered delivery.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    pub fn ordering_key(mut self, value: impl Into<String>) -> Self {
        self.ordering_key = Some(value.into());
        self
    }
    /// Replaces the envelope creation timestamp.
    ///
    /// # Parameters
    /// - `value`: timestamp to store in the event envelope.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn timestamp(mut self, value: SystemTime) -> Self {
        self.timestamp = Some(value);
        self
    }
    /// Replaces the requested delivery delay.
    ///
    /// # Parameters
    /// - `value`: delay before the provider should make the event available.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn delay(mut self, value: Duration) -> Self {
        self.delay = Some(value);
        self
    }
    /// Replaces the retry policy directly from `qubit-retry`.
    ///
    /// # Parameters
    /// - `value`: policy used to classify and schedule retry attempts.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn retry_policy(mut self, value: RetryPolicy) -> Self {
        self.options.retry_policy = Some(value);
        self
    }
    /// Replaces the typed retry rule directly from `qubit-retry`.
    ///
    /// # Type Parameters
    /// - `R`: retry rule implementation for publish-attempt errors.
    ///
    /// # Parameters
    /// - `value`: retry rule to use for this request.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    pub fn retry_rule<R>(mut self, value: R) -> Self
    where
        R: RetryRule<PublishAttemptError>,
    {
        self.options.retry_rule = Some(Arc::new(value));
        self
    }
    /// Replaces the shared retry cancellation token.
    ///
    /// # Parameters
    /// - `value`: token used to cancel retry waiting.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn retry_cancellation_token(mut self, value: RetryCancellationToken) -> Self {
        self.options.retry_cancellation_token = Some(value);
        self
    }
    /// Appends a terminal publish failure handler in registration order.
    ///
    /// The handler runs when the SPI publish operation fails directly (with no
    /// retry policy) or configured retries reach a terminal error. It is not
    /// called for request, capability, codec, or interceptor preflight errors.
    /// It returns no action and cannot change the outcome. Panics are isolated
    /// and do not prevent later handlers from running.
    ///
    /// # Type Parameters
    /// - `F`: thread-safe handler callable type.
    ///
    /// # Parameters
    /// - `handler`: callback invoked for terminal provider failures.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    pub fn error_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(&PublishFailureContext<T>, &PublishFailure) + Send + Sync + 'static,
    {
        self.options.error_handlers.push(Arc::new(handler));
        self
    }
    /// Appends a typed publisher interceptor in registration order.
    ///
    /// # Type Parameters
    /// - `F`: thread-safe event transformation callable type.
    ///
    /// # Parameters
    /// - `value`: interceptor appended to the typed publish chain.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    pub fn interceptor<F>(mut self, value: F) -> Self
    where
        F: Fn(EventEnvelope<T>) -> Result<Option<EventEnvelope<T>>, PublishError> + Send + Sync + 'static,
    {
        self.options.interceptors.push(Arc::new(value));
        self
    }
    /// Replaces all policy state; later policy calls override or append.
    ///
    /// # Parameters
    /// - `value`: complete publish policy to apply.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn options(mut self, value: PublishOptions<T>) -> Self {
        self.options = value;
        self
    }

    /// Consumes the builder and creates a request, generating an event ID and
    /// timestamp when neither was supplied. Existing headers are merged by
    /// key; a later `options` call replaces earlier policy callbacks.
    ///
    /// # Returns
    /// The validated publication request.
    ///
    /// # Errors
    /// Returns `MissingField` without a topic or payload, `InvalidHeader` for
    /// malformed header metadata, `InvalidOrderingKey` for a blank or control
    /// containing key, and `InvalidRetryConfiguration` when a rule or
    /// cancellation token has no retry policy. Returns `EventIdGeneration`
    /// when no explicit ID was supplied and UUID generation failed; its source
    /// retains the underlying generator error. `Duration` is nonnegative, so
    /// zero delay is accepted.
    pub fn build(self) -> Result<PublishRequest<T>, PublishRequestBuildError> {
        self.build_with_event_id_generator(EventId::generate)
    }

    /// Completes request validation while invoking the supplied ID source only
    /// when the caller did not provide an event ID.
    ///
    /// # Type Parameters
    /// - `F`: one-shot event ID generator callable type.
    ///
    /// # Parameters
    /// - `generate`: source called only when the request has no explicit ID.
    ///
    /// # Returns
    /// The validated request or the corresponding build failure.
    ///
    /// # Errors
    /// Returns the same validation errors as [`Self::build`], or wraps a
    /// failure returned by `generate`.
    fn build_with_event_id_generator<F>(self, generate: F) -> Result<PublishRequest<T>, PublishRequestBuildError>
    where
        F: FnOnce() -> Result<EventId, EventIdGenerationError>,
    {
        let topic = self.topic.ok_or(PublishRequestBuildError::MissingField("topic"))?;
        let payload = self.payload.ok_or(PublishRequestBuildError::MissingField("payload"))?;
        for (key, value) in &self.headers {
            if key.eq_ignore_ascii_case(DEAD_LETTER_HEADER)
                || key.is_empty()
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
            .is_some_and(|key| !is_nonblank_without_controls(key))
        {
            return Err(PublishRequestBuildError::InvalidOrderingKey);
        }
        if self.options.retry_policy.is_none()
            && (self.options.retry_rule.is_some() || self.options.retry_cancellation_token.is_some())
        {
            return Err(PublishRequestBuildError::InvalidRetryConfiguration);
        }
        let mut envelope = match self.event_id {
            Some(id) => EventEnvelope::with_id(topic, payload, id),
            None => EventEnvelope::with_id(
                topic,
                payload,
                generate().map_err(PublishRequestBuildError::EventIdGeneration)?,
            ),
        };
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
    /// Creates an empty builder with no topic or payload.
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use qubit_id::IdGenerationError;

    use super::PublishRequestBuilder;
    use crate::error::EventIdGenerationError;
    use crate::model::EventId;
    use crate::model::Topic;

    /// Confirms an explicit ID bypasses the potentially fallible random source.
    #[test]
    fn test_build_with_event_id_does_not_invoke_generator() {
        let request = PublishRequestBuilder::new()
            .topic(Topic::<String>::new("orders.created").expect("valid test topic"))
            .payload("payload".to_owned())
            .event_id(EventId::new("caller-event").expect("valid test ID"))
            .build_with_event_id_generator(|| panic!("explicit ID must bypass generation"))
            .expect("explicit ID should build without random generation");

        assert_eq!(request.envelope().id().as_str(), "caller-event");
    }

    /// Confirms builder errors preserve the random generator error chain.
    #[test]
    fn test_build_propagates_event_id_generation_error_source() {
        let result = PublishRequestBuilder::new()
            .topic(Topic::<String>::new("orders.created").expect("valid test topic"))
            .payload("payload".to_owned())
            .build_with_event_id_generator(|| {
                Err(EventIdGenerationError::new(IdGenerationError::HostOutOfRange {
                    host: 1,
                    max: 0,
                }))
            });
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("injected generation failure should be returned"),
        };

        let generation_error = Error::source(&error).expect("builder error should expose generation error");
        let generator_error = Error::source(generation_error).expect("generation error should expose source");
        assert!(generator_error.to_string().contains("host id 1 is out of range"));
    }
}
