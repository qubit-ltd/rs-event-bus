// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event bus publishing operations.

use crate::ConfigurationError;
use crate::EventBus;
use crate::EventBusError;
use crate::PublishError;
use crate::PublishMetricsSnapshot;
use crate::facade::event_bus::Arc;
use crate::facade::event_bus::EventBusInner;
use crate::facade::internal::BusContextGuard;
use crate::model::BatchPublishResult;
use crate::model::PublishReceipt;
use crate::model::PublishRequest;

impl EventBus {
    /// Publishes one typed request through publisher interceptors, retry, and
    /// SPI.
    ///
    /// # Errors
    /// Returns the structured publication failure from request validation,
    /// capability/codec checks, SPI, retry, or a closed facade.
    pub fn publish<T: Send + Sync + 'static>(
        &self,
        request: PublishRequest<T>,
    ) -> Result<PublishReceipt, PublishError> {
        self.inner.publish_metrics.record_attempt();
        let Some(_operation) = self.inner.operations.enter() else {
            self.inner.publish_metrics.record_error();
            return Err(PublishError::Closed);
        };
        let bus_identity = Arc::as_ptr(&self.inner) as usize;
        let _call_context = BusContextGuard::enter(bus_identity);
        let observers = self.inner.observer_snapshot();
        self.inner
            .publisher
            .publish(
                self.inner.spi.as_ref(),
                request,
                self.inner.facade_config.global_publisher_interceptors(),
                &observers,
            )
            .map_err(|failure| {
                self.inner.publish_metrics.record_error();
                publish_pipeline_error(failure)
            })
            .inspect(|receipt| {
                self.inner.publish_metrics.record_receipt(receipt);
            })
    }

    /// Returns the shared publication counters for this facade and its clones.
    #[must_use]
    pub fn publish_metrics(&self) -> PublishMetricsSnapshot {
        self.inner.publish_metrics.snapshot()
    }

    /// Publishes requests independently in input order and retains each result.
    ///
    /// Later requests are still attempted after an earlier request fails.
    pub fn publish_all<T, I>(&self, requests: I) -> BatchPublishResult
    where
        T: Send + Sync + 'static,
        I: IntoIterator<Item = PublishRequest<T>>,
    {
        let items = requests.into_iter().map(|request| self.publish(request)).collect();
        BatchPublishResult::new(items)
    }
}

/// Converts a publisher pipeline failure into its operation-level error type.
pub(in crate::facade) fn publish_pipeline_error(failure: crate::pipeline::PipelineFailure) -> PublishError {
    match failure.into_error() {
        EventBusError::Configuration(error) => PublishError::Configuration(error),
        EventBusError::Capability(error) => PublishError::Capability(error),
        EventBusError::Codec(error) => PublishError::Codec(error),
        EventBusError::Publish(error) => error,
        other => PublishError::Configuration(ConfigurationError::InvalidField {
            field: "publish_pipeline",
            message: other.to_string().into(),
        }),
    }
}

/// Publishes an internally constructed record during graceful shutdown drain.
pub(in crate::facade) fn publish_internal<T: Send + Sync + 'static>(
    inner: &EventBusInner,
    request: PublishRequest<T>,
) -> Result<PublishReceipt, PublishError> {
    let observers = inner.observer_snapshot();
    inner
        .publisher
        .publish(inner.spi.as_ref(), request, &[], &observers)
        .map_err(publish_pipeline_error)
}
