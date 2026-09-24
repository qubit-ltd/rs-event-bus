// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sync and runtime-neutral async publication processing.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use crate::codec::CodecRegistry;
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
use crate::pipeline::interceptor::GlobalPublisherInterceptor;
use crate::pipeline::retry;
use crate::spi::AsyncEventBusSpi;
use crate::spi::EncodedPayload;
use crate::spi::EventBusSpi;
use crate::spi::OrderingKey;
use crate::spi::OutboundMessage;
use crate::spi::PayloadModes;
use crate::spi::TopicAddress;
use crate::spi::TransportPayload;

/// Internal publisher pipeline shared by both typed facades.
pub(crate) struct PublisherPipeline {
    provider_id: ProviderId,
    codecs: Arc<CodecRegistry>,
}

impl PublisherPipeline {
    /// Creates a sync publisher pipeline for one provider instance.
    pub(crate) fn new(provider_id: ProviderId, codecs: Arc<CodecRegistry>) -> Self {
        Self { provider_id, codecs }
    }

    /// Publishes one typed event through the ordered sync pipeline.
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
                Ok(Ok(Some(next))) => envelope = next,
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
        let capabilities = spi.capabilities();
        validate_transport_metadata(envelope.delay(), envelope.ordering_key(), capabilities)?;
        let outbound = self.prepare_outbound(capabilities.payload_modes(), envelope)?;
        let result = retry::publish_sync(
            spi,
            self.provider_id.as_str(),
            || outbound.build(),
            options.retry_policy(),
            options.retry_rule(),
            options.retry_cancellation_token(),
        );
        let acknowledgement = match result {
            Ok(acknowledgement) => acknowledgement,
            Err(error) => {
                let origin = publish_failure_origin(&error);
                let error = notify_publish_error_handlers(&outbound.failure_context, options.error_handlers(), error);
                return Err(failure(origin, error));
            }
        };
        self.emit_rejections(&acknowledgement, &outbound.event_id, outbound.topic.as_str(), observers);
        Ok(PublishReceipt::new(
            input_event_id,
            Some(outbound.event_id),
            self.provider_id.clone(),
            acknowledgement,
        ))
    }

    /// Publishes one typed event through the runtime-neutral async pipeline.
    pub(crate) async fn publish_async<T: Send + Sync + 'static>(
        &self,
        spi: &dyn AsyncEventBusSpi,
        request: PublishRequest<T>,
        global_interceptors: &[GlobalPublisherInterceptor],
        observers: &[Arc<DiagnosticObserver>],
        timer: Arc<dyn qubit_clock::Timer>,
    ) -> Result<PublishReceipt, PipelineFailure> {
        let (mut envelope, options) = request.into_parts();
        let input_event_id = envelope.id().clone();
        let is_dead_letter =
            envelope.header(crate::model::DEAD_LETTER_HEADER) == Some(crate::model::DEAD_LETTER_HEADER_VALUE);
        for interceptor in options.interceptors() {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| interceptor(envelope))) {
                Ok(Ok(Some(next))) => envelope = next,
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
        let capabilities = spi.capabilities();
        validate_transport_metadata(envelope.delay(), envelope.ordering_key(), capabilities)?;
        let outbound = self.prepare_outbound_for(envelope, capabilities.payload_modes())?;
        let result = retry::publish_async(
            spi,
            self.provider_id.as_str(),
            || outbound.build(),
            options.retry_policy(),
            options.retry_rule(),
            options.retry_cancellation_token(),
            timer,
        )
        .await;
        let acknowledgement = match result {
            Ok(acknowledgement) => acknowledgement,
            Err(error) => {
                let origin = publish_failure_origin(&error);
                let error = notify_publish_error_handlers(&outbound.failure_context, options.error_handlers(), error);
                return Err(failure(origin, error));
            }
        };
        self.emit_rejections(&acknowledgement, &outbound.event_id, outbound.topic.as_str(), observers);
        Ok(PublishReceipt::new(
            input_event_id,
            Some(outbound.event_id),
            self.provider_id.clone(),
            acknowledgement,
        ))
    }

    fn prepare_outbound<T: Send + Sync + 'static>(
        &self,
        modes: PayloadModes,
        envelope: EventEnvelope<T>,
    ) -> Result<PreparedOutbound<T>, PipelineFailure> {
        self.prepare_outbound_for(envelope, modes)
    }

    fn prepare_outbound_for<T: Send + Sync + 'static>(
        &self,
        envelope: EventEnvelope<T>,
        modes: PayloadModes,
    ) -> Result<PreparedOutbound<T>, PipelineFailure> {
        let failure_context = PublishFailureContext::from_envelope(envelope);
        let topic = TopicAddress::new(failure_context.topic().name())
            .map_err(|error| failure(PipelineFailureOrigin::Capability, error))?;
        let codec = failure_context
            .topic()
            .codec()
            .cloned()
            .or_else(|| self.codecs.get::<T>());
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
                let bytes = codec
                    .encode(failure_context.payload())
                    .map_err(|error| failure(PipelineFailureOrigin::Codec, error))?;
                TransportPayload::Encoded(EncodedPayload::new(
                    bytes,
                    codec.content_type().clone(),
                    codec.schema_id().cloned(),
                ))
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

struct PreparedOutbound<T: 'static> {
    topic: TopicAddress,
    event_id: crate::model::EventId,
    timestamp: std::time::SystemTime,
    headers: crate::model::Headers,
    ordering_key: Option<OrderingKey>,
    delay: Option<std::time::Duration>,
    payload: TransportPayload,
    failure_context: PublishFailureContext<T>,
}

impl<T: 'static> PreparedOutbound<T> {
    fn build(&self) -> OutboundMessage {
        let payload = match &self.payload {
            TransportPayload::Native(value) => TransportPayload::Native(value.clone()),
            TransportPayload::Encoded(value) => TransportPayload::Encoded(EncodedPayload::new(
                value.bytes().into(),
                value.content_type().clone(),
                value.schema_id().cloned(),
            )),
        };
        OutboundMessage::new(
            self.topic.clone(),
            self.event_id.clone(),
            self.timestamp,
            self.headers.clone(),
            self.ordering_key.clone(),
            self.delay,
            payload,
        )
    }
}

fn publish_failure_origin(error: &PublishError) -> PipelineFailureOrigin {
    if matches!(error, PublishError::Retry(_)) {
        PipelineFailureOrigin::Retry
    } else {
        PipelineFailureOrigin::Provider
    }
}

fn notify_publish_error_handlers<T: 'static>(
    context: &PublishFailureContext<T>,
    handlers: &[Arc<crate::model::PublishErrorHandler<T>>],
    terminal_error: PublishError,
) -> PublishError {
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
        None => terminal_error,
    }
}

fn failure(origin: PipelineFailureOrigin, error: impl Into<EventBusError>) -> PipelineFailure {
    PipelineFailure::new(origin, error)
}

fn panic_text(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

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
