// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sync and runtime-neutral async publication processing.

#[path = "publisher/internal/mod.rs"]
mod internal;

use std::any::Any;
use std::cell::Cell;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use qubit_clock::Timer;

use self::internal::PreparedOutbound;
use crate::codec::CodecRegistry;
use crate::codec::resolve_codec;
use crate::error::CapabilityError;
use crate::error::EventBusError;
use crate::error::PublishError;
use crate::model::AdmissionStatus;
use crate::model::EventEnvelope;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;
use crate::model::PublishFailureContext;
use crate::model::PublishMetadata;
use crate::model::PublishReceipt;
use crate::model::PublishRequest;
use crate::pipeline::diagnostic::Diagnostic;
use crate::pipeline::diagnostic::DiagnosticObserver;
use crate::pipeline::diagnostic::PipelineFailure;
use crate::pipeline::diagnostic::PipelineFailureOrigin;
use crate::pipeline::diagnostic::emit_diagnostic;
use crate::pipeline::global_publisher_interceptor::GlobalPublisherInterceptor;
use crate::pipeline::retry;
use crate::spi::AsyncEventBusSpi;
use crate::spi::EncodedPayload;
use crate::spi::EventBusCapabilities;
use crate::spi::EventBusSpi;
use crate::spi::OrderingKey;
use crate::spi::PayloadModes;
use crate::spi::TopicAddress;
use crate::spi::TransportPayload;

/// Internal publisher pipeline shared by both typed facades.
#[must_use]
pub(crate) struct PublisherPipeline {
    /// Provider identity copied into publication receipts and SPI errors.
    provider_id: ProviderId,
    /// Codec implementations available for event payloads.
    codecs: Arc<CodecRegistry>,
    /// Transport capabilities used to validate publication metadata.
    capabilities: EventBusCapabilities,
    /// Maximum size accepted for encoded payloads.
    max_encoded_payload_bytes: NonZeroUsize,
}

impl PublisherPipeline {
    /// Creates a sync publisher pipeline for one provider instance.
    ///
    /// # Parameters
    ///
    /// - `provider_id`: Identity attached to receipts and provider failures.
    /// - `codecs`: Registry used to encode event payloads when required.
    /// - `capabilities`: Transport capabilities used to validate requests.
    /// - `max_encoded_payload_bytes`: Positive upper bound for encoded
    ///   payloads.
    ///
    /// # Returns
    ///
    /// A publication pipeline configured for the provider.
    pub(crate) fn new(
        provider_id: ProviderId,
        codecs: Arc<CodecRegistry>,
        capabilities: EventBusCapabilities,
        max_encoded_payload_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            provider_id,
            codecs,
            capabilities,
            max_encoded_payload_bytes,
        }
    }

    /// Publishes one typed event through the ordered sync pipeline.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Event payload type.
    ///
    /// # Parameters
    ///
    /// - `spi`: Synchronous provider implementation.
    /// - `request`: Event and per-publication options to process.
    /// - `global_interceptors`: Bus-wide metadata interceptors.
    /// - `observers`: Diagnostic observers notified about rejected admissions.
    ///
    /// # Returns
    ///
    /// The provider receipt, including interceptor drops, when publication
    /// completes.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid transport metadata, codec or interceptor
    /// failure, provider failure, or exhausted retry policy.
    pub(crate) fn publish<T: Send + Sync + 'static>(
        &self,
        spi: &dyn EventBusSpi,
        request: PublishRequest<T>,
        global_interceptors: &[GlobalPublisherInterceptor],
        observers: &[Arc<DiagnosticObserver>],
    ) -> Result<PublishReceipt, PipelineFailure> {
        let (mut envelope, options) = request.into_parts();
        let input_event_id = envelope.id().clone();
        let is_dead_letter =
            envelope.header(crate::model::DEAD_LETTER_HEADER) == Some(crate::model::DEAD_LETTER_HEADER_VALUE);
        for interceptor in options.interceptors() {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| interceptor(envelope))) {
                Ok(Ok(Some(next))) => {
                    if next.id() != &input_event_id {
                        return Err(failure(
                            PipelineFailureOrigin::Interceptor,
                            crate::error::ConfigurationError::InvalidField {
                                field: "event_id",
                                message: "typed publisher interceptor cannot change event identity".into(),
                            },
                        ));
                    }
                    envelope = next;
                }
                Ok(Ok(None)) => {
                    return Ok(PublishReceipt::new(
                        input_event_id.clone(),
                        None,
                        self.provider_id.clone(),
                        PublishAcknowledgement::DroppedByInterceptor,
                    ));
                }
                Ok(Err(error)) => return Err(failure(PipelineFailureOrigin::Interceptor, error)),
                Err(payload) => {
                    return Err(failure(
                        PipelineFailureOrigin::Interceptor,
                        PublishError::InterceptorPanicked {
                            scope: "typed",
                            message: panic_text(payload.as_ref()).into(),
                        },
                    ));
                }
            }
        }
        let mut metadata = PublishMetadata::from_headers(envelope.headers().clone());
        for interceptor in global_interceptors {
            match interceptor.apply(&mut metadata) {
                Ok(true) => {}
                Ok(false) => {
                    return Ok(PublishReceipt::new(
                        input_event_id.clone(),
                        None,
                        self.provider_id.clone(),
                        PublishAcknowledgement::DroppedByInterceptor,
                    ));
                }
                Err(error) => return Err(failure(PipelineFailureOrigin::Interceptor, error)),
            }
        }
        envelope.headers = metadata.into_headers();
        if is_dead_letter {
            envelope.headers.insert(
                crate::model::DEAD_LETTER_HEADER.into(),
                crate::model::DEAD_LETTER_HEADER_VALUE.into(),
            );
        }
        let capabilities = self.capabilities;
        validate_transport_metadata(envelope.delay(), envelope.ordering_key(), capabilities)?;
        let outbound = self.prepare_outbound(capabilities.payload_modes(), envelope)?;
        let seen_unknown = Cell::new(false);
        let seen_admission = Cell::new(false);
        let result = retry::publish_sync(
            spi,
            self.provider_id.as_str(),
            || outbound.build(),
            options.retry_policy(),
            options.retry_rule(),
            options.retry_cancellation_token(),
            options.duplicate_risk_policy(),
            &seen_unknown,
            &seen_admission,
        );
        let acknowledgement = match result {
            Ok(acknowledgement) => acknowledgement,
            Err(error) => {
                let origin = publish_failure_origin(&error);
                let effect = if seen_unknown.get() || seen_admission.get() {
                    crate::model::PublishEffect::MayHaveBeenAccepted
                } else {
                    error.publish_effect()
                };
                let error =
                    notify_publish_error_handlers(&outbound.failure_context, options.error_handlers(), error, effect);
                return Err(failure(origin, error).with_publish_effect(effect));
            }
        };
        self.emit_rejections(&acknowledgement, &outbound.event_id, outbound.topic.as_str(), observers);
        Ok(PublishReceipt::new(
            input_event_id,
            Some(outbound.event_id),
            self.provider_id.clone(),
            acknowledgement,
        )
        .with_duplicate_possible(seen_unknown.get()))
    }

    /// Publishes one typed event through the runtime-neutral async pipeline.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Event payload type.
    ///
    /// # Parameters
    ///
    /// - `spi`: Asynchronous provider implementation.
    /// - `request`: Event and per-publication options to process.
    /// - `global_interceptors`: Bus-wide metadata interceptors.
    /// - `observers`: Diagnostic observers notified about rejected admissions.
    /// - `timer`: Clock used for retry delays.
    ///
    /// # Returns
    ///
    /// The provider receipt, including interceptor drops, when publication
    /// completes.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid transport metadata, codec or interceptor
    /// failure, provider failure, or exhausted retry policy.
    pub(crate) async fn publish_async<T: Send + Sync + 'static>(
        &self,
        spi: &dyn AsyncEventBusSpi,
        request: PublishRequest<T>,
        global_interceptors: &[GlobalPublisherInterceptor],
        observers: &[Arc<DiagnosticObserver>],
        timer: Arc<dyn Timer>,
    ) -> Result<PublishReceipt, PipelineFailure> {
        let (mut envelope, options) = request.into_parts();
        let input_event_id = envelope.id().clone();
        let is_dead_letter =
            envelope.header(crate::model::DEAD_LETTER_HEADER) == Some(crate::model::DEAD_LETTER_HEADER_VALUE);
        for interceptor in options.interceptors() {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| interceptor(envelope))) {
                Ok(Ok(Some(next))) => {
                    if next.id() != &input_event_id {
                        return Err(failure(
                            PipelineFailureOrigin::Interceptor,
                            crate::error::ConfigurationError::InvalidField {
                                field: "event_id",
                                message: "typed publisher interceptor cannot change event identity".into(),
                            },
                        ));
                    }
                    envelope = next;
                }
                Ok(Ok(None)) => {
                    return Ok(PublishReceipt::new(
                        input_event_id.clone(),
                        None,
                        self.provider_id.clone(),
                        PublishAcknowledgement::DroppedByInterceptor,
                    ));
                }
                Ok(Err(error)) => return Err(failure(PipelineFailureOrigin::Interceptor, error)),
                Err(payload) => {
                    return Err(failure(
                        PipelineFailureOrigin::Interceptor,
                        PublishError::InterceptorPanicked {
                            scope: "typed",
                            message: panic_text(payload.as_ref()).into(),
                        },
                    ));
                }
            }
        }
        let mut metadata = PublishMetadata::from_headers(envelope.headers().clone());
        for interceptor in global_interceptors {
            match interceptor.apply(&mut metadata) {
                Ok(true) => {}
                Ok(false) => {
                    return Ok(PublishReceipt::new(
                        input_event_id.clone(),
                        None,
                        self.provider_id.clone(),
                        PublishAcknowledgement::DroppedByInterceptor,
                    ));
                }
                Err(error) => return Err(failure(PipelineFailureOrigin::Interceptor, error)),
            }
        }
        envelope.headers = metadata.into_headers();
        if is_dead_letter {
            envelope.headers.insert(
                crate::model::DEAD_LETTER_HEADER.into(),
                crate::model::DEAD_LETTER_HEADER_VALUE.into(),
            );
        }
        let capabilities = self.capabilities;
        validate_transport_metadata(envelope.delay(), envelope.ordering_key(), capabilities)?;
        let outbound = self.prepare_outbound_for(envelope, capabilities.payload_modes())?;
        let seen_unknown = Arc::new(AtomicBool::new(false));
        let seen_admission = Arc::new(AtomicBool::new(false));
        let result = retry::publish_async(
            spi,
            self.provider_id.as_str(),
            || outbound.build(),
            options.retry_policy(),
            options.retry_rule(),
            options.retry_cancellation_token(),
            timer,
            options.duplicate_risk_policy(),
            seen_unknown.clone(),
            seen_admission.clone(),
        )
        .await;
        let acknowledgement = match result {
            Ok(acknowledgement) => acknowledgement,
            Err(error) => {
                let origin = publish_failure_origin(&error);
                let effect = if seen_unknown.load(Ordering::Acquire) || seen_admission.load(Ordering::Acquire) {
                    crate::model::PublishEffect::MayHaveBeenAccepted
                } else {
                    error.publish_effect()
                };
                let error =
                    notify_publish_error_handlers(&outbound.failure_context, options.error_handlers(), error, effect);
                return Err(failure(origin, error).with_publish_effect(effect));
            }
        };
        self.emit_rejections(&acknowledgement, &outbound.event_id, outbound.topic.as_str(), observers);
        Ok(PublishReceipt::new(
            input_event_id,
            Some(outbound.event_id),
            self.provider_id.clone(),
            acknowledgement,
        )
        .with_duplicate_possible(seen_unknown.load(Ordering::Acquire)))
    }

    /// Prepares the transport payload using an explicitly selected payload
    /// mode.
    ///
    /// # Parameters
    ///
    /// - `modes`: Payload representations supported by the provider.
    /// - `envelope`: Event to convert into an outbound transport message.
    ///
    /// # Returns
    ///
    /// A prepared message retaining the original context for error callbacks.
    ///
    /// # Errors
    ///
    /// Returns a pipeline failure when metadata or payload preparation fails.
    fn prepare_outbound<T: Send + Sync + 'static>(
        &self,
        modes: PayloadModes,
        envelope: EventEnvelope<T>,
    ) -> Result<PreparedOutbound<T>, PipelineFailure> {
        self.prepare_outbound_for(envelope, modes)
    }

    /// Encodes or erases an event payload according to provider capabilities.
    ///
    /// # Parameters
    ///
    /// - `envelope`: Event and metadata to preserve in the outbound message.
    /// - `modes`: Payload representations supported by the provider.
    ///
    /// # Returns
    ///
    /// A transport-ready message and its publication failure context.
    ///
    /// # Errors
    ///
    /// Returns a pipeline failure when topic conversion, codec execution, or
    /// payload limits fail.
    fn prepare_outbound_for<T: Send + Sync + 'static>(
        &self,
        envelope: EventEnvelope<T>,
        modes: PayloadModes,
    ) -> Result<PreparedOutbound<T>, PipelineFailure> {
        let failure_context = PublishFailureContext::from_envelope(envelope);
        let topic = TopicAddress::new(failure_context.topic().name())
            .map_err(|error| failure(PipelineFailureOrigin::Capability, error))?;
        let codec = resolve_codec(failure_context.topic(), &self.codecs);
        let event_id = failure_context.event_id().clone();
        let timestamp = failure_context.timestamp();
        let headers = failure_context.headers().clone();
        let ordering_key = failure_context.ordering_key().and_then(OrderingKey::new);
        let delay = failure_context.delay();
        let transport_payload = match modes {
            PayloadModes::Native | PayloadModes::NativeAndEncoded => {
                TransportPayload::Native(failure_context.payload_arc() as Arc<dyn Any + Send + Sync>)
            }
            PayloadModes::Encoded => {
                let codec =
                    codec.ok_or_else(|| failure(PipelineFailureOrigin::Capability, CapabilityError::CodecRequired))?;
                let bytes = crate::codec::call_codec("encode", || codec.encode(failure_context.payload()))
                    .map_err(|error| failure(PipelineFailureOrigin::Codec, error))?;
                if bytes.len() > self.max_encoded_payload_bytes.get() {
                    return Err(failure(
                        PipelineFailureOrigin::Codec,
                        crate::error::CodecError::PayloadTooLarge {
                            actual: bytes.len(),
                            limit: self.max_encoded_payload_bytes.get(),
                            direction: crate::model::PayloadDirection::Publish,
                        },
                    ));
                }
                let content_type = crate::codec::call_codec("content_type", || Ok(codec.content_type().clone()))
                    .map_err(|error| failure(PipelineFailureOrigin::Codec, error))?;
                let schema_id = crate::codec::call_codec("schema_id", || Ok(codec.schema_id().cloned()))
                    .map_err(|error| failure(PipelineFailureOrigin::Codec, error))?;
                TransportPayload::Encoded(EncodedPayload::new(bytes, content_type, schema_id))
            }
        };
        Ok(PreparedOutbound {
            topic,
            event_id,
            timestamp,
            headers,
            ordering_key,
            delay,
            payload: transport_payload,
            failure_context,
        })
    }

    /// Reports rejected destination admissions to each diagnostic observer.
    ///
    /// # Parameters
    ///
    /// - `acknowledgement`: Provider result to inspect.
    /// - `event_id`: Identity of the published event.
    /// - `topic`: Destination topic name.
    /// - `observers`: Observers that receive rejection diagnostics.
    ///
    /// # Side Effects
    ///
    /// Invokes observers for every rejected destination admission.
    fn emit_rejections(
        &self,
        acknowledgement: &PublishAcknowledgement,
        event_id: &crate::model::EventId,
        topic: &str,
        observers: &[Arc<DiagnosticObserver>],
    ) {
        let PublishAcknowledgement::DestinationAdmissions(admissions) = acknowledgement else {
            return;
        };
        for admission in admissions {
            let AdmissionStatus::Rejected(reason) = admission.status() else {
                continue;
            };
            emit_diagnostic(
                observers,
                &Diagnostic::AdmissionRejected {
                    event_id: event_id.clone(),
                    topic: topic.into(),
                    subscriber_id: admission.subscriber_id().clone(),
                    reason: reason.clone(),
                },
            );
        }
    }
}

/// Classifies whether a publication error originated in retries or the
/// provider.
///
/// # Parameters
///
/// - `error`: Terminal publication error to classify.
///
/// # Returns
///
/// `Retry` for retry wrapper errors and `Provider` for all other publish
/// errors.
fn publish_failure_origin(error: &PublishError) -> PipelineFailureOrigin {
    if matches!(error, PublishError::Retry(_)) {
        PipelineFailureOrigin::Retry
    } else {
        PipelineFailureOrigin::Provider
    }
}

/// Notifies all publication error handlers and wraps any handler panic.
///
/// # Type Parameters
///
/// - `T`: Event payload type retained by the failure context.
///
/// # Parameters
///
/// - `context`: Immutable metadata for the failed publication.
/// - `handlers`: Callbacks to invoke for the terminal error.
/// - `terminal_error`: Failure passed to callbacks and returned when none
///   panic.
/// - `effect`: Aggregate admission evidence from every completed attempt.
///
/// # Returns
///
/// The terminal error, wrapped when one or more callbacks panic.
fn notify_publish_error_handlers<T: 'static>(
    context: &PublishFailureContext<T>,
    handlers: &[Arc<crate::model::PublishErrorHandler<T>>],
    terminal_error: PublishError,
    effect: crate::model::PublishEffect,
) -> PublishError {
    let terminal_error = crate::error::PublishFailure::new(context.event_id().clone(), effect, terminal_error);
    let mut panic_message = None;
    for handler in handlers {
        match std::panic::catch_unwind(AssertUnwindSafe(|| handler(context, &terminal_error))) {
            Err(payload) if panic_message.is_none() => {
                panic_message = Some(panic_text(payload.as_ref()).to_owned().into_boxed_str());
            }
            _ => {}
        }
    }
    match panic_message {
        Some(message) => PublishError::ErrorHandlerPanicked {
            message,
            source: Box::new(terminal_error),
        },
        None => terminal_error.into_cause(),
    }
}

/// Builds a pipeline failure with its origin and converted event bus error.
///
/// # Parameters
///
/// - `origin`: Pipeline stage where the error occurred.
/// - `error`: Error converted into the shared event bus error type.
///
/// # Returns
///
/// A failure suitable for returning from publication processing.
fn failure(origin: PipelineFailureOrigin, error: impl Into<EventBusError>) -> PipelineFailure {
    PipelineFailure::new(origin, error)
}

/// Extracts a readable message from a panic payload.
///
/// # Parameters
///
/// - `payload`: Panic payload produced by a caught callback.
///
/// # Returns
///
/// The contained string or a stable fallback for non-string payloads.
fn panic_text(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

/// Checks that requested delay and ordering metadata are supported by the
/// provider.
///
/// # Parameters
///
/// - `delay`: Optional delivery delay requested by the event.
/// - `ordering_key`: Optional per-event ordering key.
/// - `capabilities`: Provider capabilities used for validation.
///
/// # Returns
///
/// `Ok(())` when all requested metadata is supported.
///
/// # Errors
///
/// Returns a capability failure for an unsupported delay or ordering key.
fn validate_transport_metadata(
    delay: Option<std::time::Duration>,
    ordering_key: Option<&str>,
    capabilities: crate::spi::EventBusCapabilities,
) -> Result<(), PipelineFailure> {
    if delay.is_some() && capabilities.delayed_delivery() == crate::spi::DelayedDeliveryCapability::None {
        return Err(failure(
            PipelineFailureOrigin::Capability,
            CapabilityError::Unsupported {
                capability: "delayed_delivery",
            },
        ));
    }
    if ordering_key.is_some() && capabilities.ordering() == crate::spi::OrderingCapability::None {
        return Err(failure(
            PipelineFailureOrigin::Capability,
            CapabilityError::Unsupported { capability: "ordering" },
        ));
    }
    Ok(())
}
