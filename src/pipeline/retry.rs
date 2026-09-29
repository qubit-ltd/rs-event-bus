// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Publish-specific adapters for the `qubit-retry` execution API.

use std::sync::Arc;

mod catch_unwind_future;

use catch_unwind_future::CatchUnwindFuture;
use qubit_clock::Timer;
use qubit_retry::AsyncRetry;
use qubit_retry::AttemptFailure;
use qubit_retry::Retry;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryConfig;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryFallback;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use crate::error::PublishAttemptError;
use crate::error::PublishError;
use crate::error::SpiError;
use crate::model::PublishAcknowledgement;
use crate::spi::AsyncEventBusSpi;
use crate::spi::EventBusSpi;
use crate::spi::OutboundMessage;
use crate::spi::SpiFuture;

/// Invokes the provider once or through the configured same-thread retry.
///
/// # Type Parameters
/// - `F`: Factory that builds a fresh outbound message for each attempt.
///
/// # Parameters
/// - `spi`: Synchronous provider called for each attempt.
/// - `provider_id`: Provider identity used to classify SPI errors.
/// - `make_message`: Factory for the outbound message.
/// - `policy`: Optional retry budget and backoff policy.
/// - `rule`: Optional user retry decision rule.
/// - `cancellation`: Optional signal that cancels retry delays.
///
/// # Returns
/// The provider acknowledgement after success.
///
/// # Errors
/// Returns provider failure, retry exhaustion/cancellation, or invalid retry
/// configuration.
pub(crate) fn publish_sync<F>(
    spi: &dyn EventBusSpi,
    provider_id: &str,
    make_message: F,
    policy: Option<&RetryPolicy>,
    rule: Option<&Arc<dyn RetryRule<PublishAttemptError>>>,
    cancellation: Option<&RetryCancellationToken>,
) -> Result<PublishAcknowledgement, PublishError>
where
    F: Fn() -> OutboundMessage,
{
    let Some(policy) = policy else {
        return spi_publish_sync(spi, provider_id, make_message()).map_err(PublishError::from);
    };
    let config = retry_config(policy, rule)?;
    let mut retry = Retry::new(&config);
    if let Some(cancellation) = cancellation {
        retry = retry.cancellation_token(cancellation.clone());
    }
    retry
        .run(|| {
            spi_publish_sync(spi, provider_id, make_message())
                .map_err(|error| PublishAttemptError::new(error.kind(), error.retryable(), error))
        })
        .map(|success| success.value().clone())
        .map_err(|error| PublishError::Retry(Box::new(error)))
}

/// Invokes an async provider using runtime-neutral retry.
///
/// # Type Parameters
/// - `'a`: Lifetime shared by the provider, message factory, and attempt
///   futures.
/// - `F`: Factory that builds a fresh outbound message for each attempt.
///
/// # Parameters
/// - `spi`: Asynchronous provider called for each attempt.
/// - `provider_id`: Provider identity used to classify SPI errors.
/// - `make_message`: Factory for the outbound message.
/// - `policy`: Optional retry budget and backoff policy.
/// - `rule`: Optional user retry decision rule.
/// - `cancellation`: Optional signal that cancels retry delays.
/// - `timer`: Clock used for retry delays.
///
/// # Returns
/// The provider acknowledgement after success.
///
/// # Errors
/// Returns provider failure, retry exhaustion/cancellation, or invalid retry
/// configuration.
pub(crate) async fn publish_async<'a, F>(
    spi: &'a dyn AsyncEventBusSpi,
    provider_id: &'a str,
    make_message: F,
    policy: Option<&RetryPolicy>,
    rule: Option<&Arc<dyn RetryRule<PublishAttemptError>>>,
    cancellation: Option<&RetryCancellationToken>,
    timer: Arc<dyn Timer>,
) -> Result<PublishAcknowledgement, PublishError>
where
    F: Fn() -> OutboundMessage + 'a,
{
    let Some(policy) = policy else {
        return spi_publish(spi, provider_id, make_message())
            .await
            .map_err(PublishError::from);
    };
    let config = retry_config(policy, rule)?;
    let mut retry = AsyncRetry::new(&config);
    retry = retry.timer(timer);
    if let Some(cancellation) = cancellation {
        retry = retry.cancellation_token(cancellation.clone());
    }
    let operation = || -> SpiFuture<'a, Result<PublishAcknowledgement, PublishAttemptError>> {
        let message = make_message();
        let attempt = spi_publish(spi, provider_id, message);
        Box::pin(async move {
            attempt.await.map_err(|error| {
                let kind = error.kind();
                let retryable = error.retryable();
                PublishAttemptError::new(kind, retryable, error)
            })
        })
    };
    retry
        .run(operation)
        .await
        .map(|success| success.value().clone())
        .map_err(|error| PublishError::Retry(Box::new(error)))
}

/// Invokes one asynchronous provider operation behind SPI panic boundaries.
///
/// # Parameters
/// - `spi`: Provider implementation receiving the message.
/// - `provider_id`: Provider identity used in operation errors.
/// - `message`: Outbound message submitted to the provider.
///
/// # Returns
/// The provider acknowledgement.
///
/// # Errors
/// Returns synchronous or asynchronous SPI operation failure.
async fn spi_publish(
    spi: &dyn AsyncEventBusSpi,
    provider_id: &str,
    message: OutboundMessage,
) -> Result<PublishAcknowledgement, SpiError> {
    let resource = message.topic().as_str().to_owned();
    let future =
        crate::spi::panic_boundary::catch_spi_call(provider_id, "publish", Some(&resource), || spi.publish(message))?;
    match CatchUnwindFuture::new(future).await {
        Ok(result) => result,
        Err(payload) => Err(crate::spi::panic_boundary::provider_panic(
            provider_id,
            "publish",
            Some(&resource),
            payload,
        )),
    }
}

/// Invokes one synchronous provider operation behind the SPI panic boundary.
///
/// # Parameters
/// - `spi`: Provider implementation receiving the message.
/// - `provider_id`: Provider identity used in operation errors.
/// - `message`: Outbound message submitted to the provider.
///
/// # Returns
/// The provider acknowledgement.
///
/// # Errors
/// Returns provider operation failure or a caught provider panic.
fn spi_publish_sync(
    spi: &dyn EventBusSpi,
    provider_id: &str,
    message: OutboundMessage,
) -> Result<PublishAcknowledgement, SpiError> {
    let resource = message.topic().as_str().to_owned();
    crate::spi::panic_boundary::catch_spi_call(provider_id, "publish", Some(&resource), || spi.publish(message))
        .and_then(std::convert::identity)
}

/// Builds an abort-on-exhaustion retry configuration for publication attempts.
///
/// # Parameters
/// - `policy`: Retry budget and backoff settings.
/// - `rule`: Optional shared retry rule supplied by the caller.
///
/// # Returns
/// A configured retry policy for publish attempts.
///
/// # Errors
/// Returns a configuration error when the retry policy is invalid.
fn retry_config(
    policy: &RetryPolicy,
    rule: Option<&Arc<dyn RetryRule<PublishAttemptError>>>,
) -> Result<RetryConfig<PublishAttemptError>, PublishError> {
    let mut builder = RetryConfig::<PublishAttemptError>::builder()
        .policy(policy.clone())
        .fallback(RetryFallback::Abort);
    if let Some(rule) = rule {
        builder = builder.shared_rule(rule.clone());
    }
    builder = builder.rule(
        |failure: &AttemptFailure<PublishAttemptError>, _context: &RetryContext| match failure
            .as_error()
            .and_then(PublishAttemptError::retryable)
        {
            Some(true) => RetryDecision::Retry,
            Some(false) => RetryDecision::Abort,
            None => RetryDecision::UseDefault,
        },
    );
    builder.build().map_err(|source| {
        PublishError::Configuration(crate::error::ConfigurationError::InvalidField {
            field: "retry_policy",
            message: source.to_string().into(),
        })
    })
}
