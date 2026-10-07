// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous event bus publishing operations.

use crate::AsyncEventBus;
use crate::CapabilityError;
use crate::PublishError;
use crate::PublishFailure;
use crate::PublishMetricsSnapshot;
use crate::error::CheckedPublishError;
use crate::error::ConfigurationError;
use crate::error::EventBusError;
use crate::facade::PublishMetrics;
use crate::model::AdmissionRequirement;
use crate::model::BatchPublishResult;
use crate::model::EventId;
use crate::model::PublishEffect;
use crate::model::PublishReceipt;
use crate::model::PublishRequest;
use crate::model::Topic;
use crate::pipeline::PipelineFailure;
use crate::spi::PublishVisibility;

/// Counts a polled publish as cancelled if it is dropped before returning.
struct PublishCompletionGuard<'a> {
    metrics: &'a PublishMetrics,
    completed: bool,
}

impl<'a> PublishCompletionGuard<'a> {
    /// Starts tracking an in-flight asynchronous publish.
    fn new(metrics: &'a PublishMetrics) -> Self {
        Self {
            metrics,
            completed: false,
        }
    }

    /// Marks the publish as returned so dropping the guard does not count it.
    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for PublishCompletionGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.metrics.record_cancelled();
        }
    }
}

impl AsyncEventBus {
    /// Checks that the configured provider can publish this topic's payload.
    ///
    /// This synchronous configuration query checks codec availability only,
    /// using capabilities cached when the facade was created. It does not
    /// encode a value, call the provider, or change publish metrics.
    /// It does not verify that encoding, payload limits, transport
    /// availability, or message persistence will succeed.
    ///
    /// # Errors
    /// Returns [`CapabilityError::CodecRequired`] when the provider accepts
    /// only encoded payloads and neither the topic nor facade registry has
    /// a codec.
    pub fn check_publish_codec<T: Send + Sync + 'static>(
        &self,
        topic: &Topic<T>,
    ) -> Result<(), CapabilityError> {
        crate::codec::check_publish_codec(
            self.inner.capabilities.payload_modes(),
            topic,
            self.inner.facade_config.codec_registry(),
        )
    }

    /// Publishes once and requires the resulting receipt to satisfy
    /// `requirement`. Per-destination conditions fail without publishing when
    /// the provider hides destination admissions. Other admission failures
    /// retain the receipt; retrying may duplicate accepted deliveries.
    ///
    /// ```
    /// # use qubit_event_bus::AsyncEventBus;
    /// # use qubit_event_bus::CheckedPublishError;
    /// # use qubit_event_bus::model::{AdmissionRequirement, PublishRequest};
    /// # async fn checked(bus: &AsyncEventBus, request: PublishRequest<String>) -> Result<(), CheckedPublishError> {
    /// let _receipt = bus.publish_checked(request, AdmissionRequirement::AtLeastOneAccepted).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_checked<T: Send + Sync + 'static>(
        &self,
        request: PublishRequest<T>,
        requirement: AdmissionRequirement,
    ) -> Result<PublishReceipt, CheckedPublishError> {
        if matches!(
            self.inner.capabilities.publish_visibility(),
            PublishVisibility::Opaque
        ) && matches!(
            requirement,
            AdmissionRequirement::AtLeastOneAccepted
                | AdmissionRequirement::AtLeastOneAcceptedAndNoRejected
        ) {
            return Err(CheckedPublishError::UnsupportedVisibility {
                event_id: request.envelope().id().clone(),
                provider_id: self.inner.provider_id.clone(),
            });
        }
        let receipt = self
            .publish(request)
            .await
            .map_err(CheckedPublishError::Publish)?;
        match receipt.check_admission(requirement) {
            Ok(()) => Ok(receipt),
            Err(reason) => Err(CheckedPublishError::Admission {
                receipt: Box::new(receipt),
                reason,
            }),
        }
    }

    /// Returns the shared publication counters for this facade and its clones.
    ///
    /// # Returns
    /// A point-in-time snapshot of publication counters.
    #[inline]
    pub fn publish_metrics(&self) -> PublishMetricsSnapshot {
        self.inner.publish_metrics.snapshot()
    }

    /// Publishes one typed request through interceptors, retry, and provider
    /// SPI.
    ///
    /// # Type Parameters
    /// - `T`: Payload type carried by the request.
    ///
    /// # Parameters
    /// - `request`: Validated request to publish.
    ///
    /// # Returns
    /// The provider receipt, or the publication failure.
    ///
    /// # Errors
    /// Returns an error if shutdown has started, a required capability is
    /// unavailable, or middleware, codec, or provider operations fail.
    pub async fn publish<T: Send + Sync + 'static>(
        &self,
        request: PublishRequest<T>,
    ) -> Result<PublishReceipt, PublishFailure> {
        // This body begins on the first poll, so an unpolled future has no
        // effect on the counters.
        let event_id = request.envelope().id().clone();
        self.inner.publish_metrics.record_attempt();
        let mut completion = PublishCompletionGuard::new(&self.inner.publish_metrics);
        let Some(_publish) = self.inner.begin_publish() else {
            self.inner.publish_metrics.record_error();
            completion.complete();
            return Err(PublishFailure::new(
                event_id,
                PublishEffect::NotAccepted,
                PublishError::Closed,
            ));
        };
        let observers = self.observer_snapshot();
        let result = self
            .inner
            .publisher
            .publish_async(
                self.inner.spi.as_ref(),
                request,
                self.inner.facade_config.global_publisher_interceptors(),
                &observers,
                self.inner.timer.clone(),
            )
            .await
            .map_err(|failure| {
                self.inner.publish_metrics.record_error();
                publish_pipeline_error(event_id, failure)
            })
            .inspect(|receipt| {
                self.inner.publish_metrics.record_receipt(receipt);
            });
        completion.complete();
        result
    }

    /// Publishes each request in order and retains each independent result.
    ///
    /// # Type Parameters
    /// - `T`: Payload type shared by all requests.
    /// - `I`: Iterator yielding requests.
    ///
    /// # Parameters
    /// - `requests`: Requests to publish.
    ///
    /// # Returns
    /// One result per input request, in input order.
    pub async fn publish_all<T, I>(&self, requests: I) -> BatchPublishResult
    where
        T: Send + Sync + 'static,
        I: IntoIterator<Item = PublishRequest<T>>,
    {
        let mut results = Vec::new();
        for request in requests {
            results.push(self.publish(request).await);
        }
        BatchPublishResult::new(results)
    }
}

/// Maps a pipeline failure to public publish error variants.
///
/// # Parameters
/// - `event_id`: identity of the original publication used in the terminal
///   failure.
/// - `failure`: failure produced by publisher pipeline execution.
///
/// # Returns
/// The corresponding public publish error.
pub(in crate::facade) fn publish_pipeline_error(
    event_id: EventId,
    failure: PipelineFailure,
) -> PublishFailure {
    let effect = failure.publish_effect();
    let cause = match failure.into_error() {
        EventBusError::Configuration(error) => PublishError::Configuration(error),
        EventBusError::Capability(error) => PublishError::Capability(error),
        EventBusError::Codec(error) => PublishError::Codec(error),
        EventBusError::Publish(error) => error,
        other => PublishError::Configuration(ConfigurationError::InvalidField {
            field: "publish_pipeline",
            message: other.to_string().into(),
        }),
    };
    PublishFailure::new(event_id, effect, cause)
}
