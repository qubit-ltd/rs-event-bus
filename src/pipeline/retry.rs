// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Publish-specific adapters for the `qubit-retry` execution API.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use qubit_retry::AsyncRetry;
use qubit_retry::AttemptFailure;
use qubit_retry::Retry;
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
pub(crate) fn publish_sync<F>(
    spi: &dyn EventBusSpi,
    provider_id: &str,
    make_message: F,
    policy: Option<&RetryPolicy>,
    rule: Option<&Arc<dyn RetryRule<PublishAttemptError>>>,
    cancellation: Option<&qubit_retry::RetryCancellationToken>,
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
pub(crate) async fn publish_async<'a, F>(
    spi: &'a dyn AsyncEventBusSpi,
    provider_id: &'a str,
    make_message: F,
    policy: Option<&RetryPolicy>,
    rule: Option<&Arc<dyn RetryRule<PublishAttemptError>>>,
    cancellation: Option<&qubit_retry::RetryCancellationToken>,
    timer: Arc<dyn qubit_clock::Timer>,
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

async fn spi_publish(
    spi: &dyn AsyncEventBusSpi,
    provider_id: &str,
    message: OutboundMessage,
) -> Result<PublishAcknowledgement, SpiError> {
    let resource = message.topic().as_str().to_owned();
    let future = match std::panic::catch_unwind(AssertUnwindSafe(|| spi.publish(message))) {
        Ok(future) => future,
        Err(payload) => return Err(publish_panic_error(provider_id, resource, payload)),
    };
    match CatchUnwindFuture::new(future).await {
        Ok(result) => result,
        Err(payload) => Err(publish_panic_error(provider_id, resource, payload)),
    }
}

fn spi_publish_sync(
    spi: &dyn EventBusSpi,
    provider_id: &str,
    message: OutboundMessage,
) -> Result<PublishAcknowledgement, SpiError> {
    let resource = message.topic().as_str().to_owned();
    match std::panic::catch_unwind(AssertUnwindSafe(|| spi.publish(message))) {
        Ok(result) => result,
        Err(payload) => Err(publish_panic_error(provider_id, resource, payload)),
    }
}

fn publish_panic_error(provider_id: &str, resource: String, payload: Box<dyn std::any::Any + Send>) -> SpiError {
    SpiError::Operation {
        provider_id: provider_id.into(),
        operation: "publish",
        resource: Some(resource.into()),
        kind: "spi_panic",
        retryable: None,
        source: Box::new(std::io::Error::other(panic_message(payload.as_ref()))),
    }
}

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

/// Future adapter that catches a panic raised while polling the SPI future.
struct CatchUnwindFuture<F: Future> {
    future: Pin<Box<F>>,
}

impl<F: Future> CatchUnwindFuture<F> {
    fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
        }
    }
}

impl<F: Future> Future for CatchUnwindFuture<F> {
    type Output = Result<F::Output, Box<dyn std::any::Any + Send>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| this.future.as_mut().poll(context))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}
