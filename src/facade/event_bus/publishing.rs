// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event bus publishing operations.

use std::sync::Arc;

use crate::ConfigurationError;
use crate::EventBus;
use crate::EventBusError;
use crate::PublishError;
use crate::PublishFailure;
use crate::PublishMetricsSnapshot;
use crate::error::CheckedPublishError;
use crate::facade::event_bus::EventBusInner;
use crate::facade::internal::BusContextGuard;
use crate::model::AdmissionRequirement;
use crate::model::BatchPublishResult;
use crate::model::EventId;
use crate::model::PublishEffect;
use crate::model::PublishReceipt;
use crate::model::PublishRequest;
use crate::pipeline::PipelineFailure;

impl EventBus {
    /// Publishes once and requires the resulting receipt to satisfy `requirement`.
    /// Admission failure retains the receipt; retrying may duplicate accepted deliveries.
    ///
    /// ```
    /// # use qubit_event_bus::EventBus;
    /// # use qubit_event_bus::CheckedPublishError;
    /// # use qubit_event_bus::model::{AdmissionRequirement, PublishRequest};
    /// # fn checked(bus: &EventBus, request: PublishRequest<String>) -> Result<(), CheckedPublishError> {
    /// let _receipt = bus.publish_checked(request, AdmissionRequirement::AtLeastOneAccepted)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn publish_checked<T: Send + Sync + 'static>(
        &self,
        request: PublishRequest<T>,
        requirement: AdmissionRequirement,
    ) -> Result<PublishReceipt, CheckedPublishError> {
        let receipt = self
            .publish(request)
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
    pub fn publish_metrics(&self) -> PublishMetricsSnapshot {
        self.inner.publish_metrics.snapshot()
    }

    /// Publishes one typed request through publisher interceptors, retry, and
    /// SPI.
    ///
    /// # Type Parameters
    /// - `T`: payload type carried by the request.
    ///
    /// # Parameters
    /// - `request`: validated request to publish.
    ///
    /// # Returns
    /// The provider receipt when publication succeeds.
    ///
    /// # Errors
    /// Returns the structured publication failure from request validation,
    /// capability/codec checks, SPI, retry, or a closed facade.
    pub fn publish<T: Send + Sync + 'static>(
        &self,
        request: PublishRequest<T>,
    ) -> Result<PublishReceipt, PublishFailure> {
        let event_id = request.envelope().id().clone();
        self.inner.publish_metrics.record_attempt();
        let Some(_operation) = self.inner.operations.enter() else {
            self.inner.publish_metrics.record_error();
            return Err(PublishFailure::new(
                event_id,
                PublishEffect::NotAccepted,
                PublishError::Closed,
            ));
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
                publish_pipeline_error(event_id, failure)
            })
            .inspect(|receipt| {
                self.inner.publish_metrics.record_receipt(receipt);
            })
    }

    /// Publishes requests independently in input order and retains each result.
    ///
    /// Later requests are still attempted after an earlier request fails.
    ///
    /// # Type Parameters
    /// - `T`: payload type shared by all requests.
    /// - `I`: iterator yielding publish requests.
    ///
    /// # Parameters
    /// - `requests`: requests to publish in input order.
    ///
    /// # Returns
    /// One independent result per request, preserving input order.
    pub fn publish_all<T, I>(&self, requests: I) -> BatchPublishResult
    where
        T: Send + Sync + 'static,
        I: IntoIterator<Item = PublishRequest<T>>,
    {
        let items = requests
            .into_iter()
            .map(|request| self.publish(request))
            .collect();
        BatchPublishResult::new(items)
    }
}

/// Converts a publisher pipeline failure into its operation-level error type.
///
/// # Parameters
/// - `event_id`: Original event identifier, preserved in the publish failure.
/// - `failure`: Pipeline stage and underlying event bus error.
///
/// # Returns
/// The matching public publish error variant.
#[must_use]
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

/// Publishes an internally constructed record during graceful shutdown drain.
///
/// # Type Parameters
/// - `T`: payload type carried by the internal request.
///
/// # Parameters
/// - `inner`: provider and publication pipeline state.
/// - `request`: internal event to publish.
///
/// # Returns
/// The provider receipt for the internal event.
///
/// # Errors
/// Returns the publication pipeline or provider failure.
pub(in crate::facade) fn publish_internal<T: Send + Sync + 'static>(
    inner: &EventBusInner,
    request: PublishRequest<T>,
) -> Result<PublishReceipt, PublishFailure> {
    let event_id = request.envelope().id().clone();
    let observers = inner.observer_snapshot();
    inner
        .publisher
        .publish(inner.spi.as_ref(), request, &[], &observers)
        .map_err(|failure| publish_pipeline_error(event_id, failure))
}
