// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous dead-letter publishing and retry handling.

use std::sync::Arc;

use qubit_retry::AsyncRetry;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryPolicy;

use crate::facade::async_event_bus::publishing::publish_pipeline_error;
use crate::facade::async_subscription::AsyncEventBusInner;
use crate::model::DeadLetterAdmissionPolicy;
use crate::model::DeadLetterEvent;
use crate::model::EventEnvelope;
use crate::model::PublishReceipt;
use crate::model::PublishRequest;
use crate::pipeline::DeadLetterForwardError;
use crate::pipeline::dead_letter_retry_config;
use crate::pipeline::dead_letter_was_accepted;

/// Publishes a dead-letter event with the configured retry policy.
///
/// # Type Parameters
/// - `T`: original event payload type.
///
/// # Parameters
/// - `inner`: bus state and provider pipeline.
/// - `envelope`: typed dead-letter event to publish.
/// - `retry_policy`: optional retry policy for dead-letter forwarding.
/// - `cancellation`: optional cancellation signal for retry waits.
/// - `admission_policy`: condition required for a forwarded event to count as
///   accepted.
///
/// # Returns
/// The publish receipt when forwarding is accepted.
///
/// # Errors
/// Returns a string describing pipeline, admission, or retry failure.
pub(in crate::facade) async fn publish_dead_letter_async<T: Send + Sync + 'static>(
    inner: &Arc<AsyncEventBusInner>,
    envelope: &EventEnvelope<DeadLetterEvent<T>>,
    retry_policy: Option<&RetryPolicy>,
    cancellation: Option<&RetryCancellationToken>,
    admission_policy: DeadLetterAdmissionPolicy,
) -> Result<PublishReceipt, String> {
    /// Publishes one dead-letter envelope and validates destination admission.
    ///
    /// # Type Parameters
    /// - `T`: original event payload type.
    ///
    /// # Parameters
    /// - `inner`: bus state and provider pipeline.
    /// - `envelope`: dead-letter event to publish.
    /// - `admission_policy`: required provider admission result.
    ///
    /// # Returns
    /// The receipt when the provider admitted the dead-letter event.
    ///
    /// # Errors
    /// Returns the pipeline or not-admitted failure.
    async fn attempt<T: Send + Sync + 'static>(
        inner: &Arc<AsyncEventBusInner>,
        envelope: EventEnvelope<DeadLetterEvent<T>>,
        admission_policy: DeadLetterAdmissionPolicy,
    ) -> Result<PublishReceipt, DeadLetterForwardError> {
        let event_id = envelope.id().clone();
        let receipt = inner
            .publisher
            .publish_async(
                inner.spi.as_ref(),
                PublishRequest::from_envelope(envelope),
                &[],
                &inner.observer_snapshot(),
                inner.timer.clone(),
            )
            .await
            .map_err(|failure| DeadLetterForwardError::Publish(publish_pipeline_error(event_id, failure)))?;
        if dead_letter_was_accepted(&receipt, inner.capabilities, admission_policy) {
            Ok(receipt)
        } else {
            Err(DeadLetterForwardError::NotAdmitted(receipt.admission_outcome()))
        }
    }

    let Some(policy) = retry_policy else {
        return attempt(inner, envelope.clone(), admission_policy)
            .await
            .map_err(|error| error.to_string());
    };
    let config = dead_letter_retry_config(policy).map_err(|error| error.to_string())?;
    let mut retry = AsyncRetry::new(&config).timer(inner.timer.clone());
    if let Some(cancellation) = cancellation {
        retry = retry.cancellation_token(cancellation.clone());
    }
    retry
        .run(|| attempt(inner, envelope.clone(), admission_policy))
        .await
        .map(|success| success.value().clone())
        .map_err(|error| error.to_string())
}
