// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous event bus publishing operations.

use crate::AsyncEventBus;
use crate::PublishError;
use crate::PublishFailure;
use crate::PublishMetricsSnapshot;
use crate::model::BatchPublishResult;
use crate::model::PublishEffect;
use crate::model::PublishReceipt;
use crate::model::PublishRequest;
use crate::pipeline::PipelineFailure;

impl AsyncEventBus {
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
    /// Returns an error if shutdown has started or a middleware, codec, or
    /// provider operation fails.
    pub async fn publish<T: Send + Sync + 'static>(
        &self,
        request: PublishRequest<T>,
    ) -> Result<PublishReceipt, PublishFailure> {
        // This body begins on the first poll, so a cancelled in-flight future
        // still contributes an attempt without recording a fabricated outcome.
        let event_id = request.envelope().id().clone();
        self.inner.publish_metrics.record_attempt();
        let Some(_publish) = self.inner.begin_publish() else {
            self.inner.publish_metrics.record_error();
            return Err(PublishFailure::new(
                event_id,
                PublishEffect::NotAccepted,
                PublishError::Closed,
            ));
        };
        let observers = self.observer_snapshot();
        self.inner
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
            })
    }

    /// Returns the shared publication counters for this facade and its clones.
    ///
    /// # Returns
    /// A point-in-time snapshot of publication counters.
    #[must_use]
    pub fn publish_metrics(&self) -> PublishMetricsSnapshot {
        self.inner.publish_metrics.snapshot()
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
/// - `failure`: failure produced by publisher pipeline execution.
///
/// # Returns
/// The corresponding public publish error.
pub(in crate::facade) fn publish_pipeline_error(
    event_id: crate::model::EventId,
    failure: PipelineFailure,
) -> PublishFailure {
    let effect = failure.publish_effect();
    let cause = match failure.into_error() {
        crate::error::EventBusError::Configuration(error) => PublishError::Configuration(error),
        crate::error::EventBusError::Capability(error) => PublishError::Capability(error),
        crate::error::EventBusError::Codec(error) => PublishError::Codec(error),
        crate::error::EventBusError::Publish(error) => error,
        other => PublishError::Configuration(crate::error::ConfigurationError::InvalidField {
            field: "publish_pipeline",
            message: other.to_string().into(),
        }),
    };
    PublishFailure::new(event_id, effect, cause)
}
