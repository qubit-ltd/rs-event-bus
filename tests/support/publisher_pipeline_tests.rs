// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;

use qubit_clock::MonotonicClock;
use qubit_clock::StdMonotonicClock;
use qubit_id::Id;
use qubit_retry::RetryPolicy;

use crate::codec::EventCodec;
use crate::error::CodecError;
use crate::error::PublishError;
use crate::error::SpiError;
use crate::model::AdmissionStatus;
use crate::model::ContentType;
use crate::model::DestinationAdmission;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;
use crate::model::PublishOptions;
use crate::model::PublishRequest;
use crate::model::SchemaId;
use crate::model::SubscriberId;
use crate::model::Topic;
use crate::pipeline::Diagnostic;
use crate::pipeline::GlobalPublisherInterceptor;
use crate::pipeline::PipelineFailureOrigin;
use crate::pipeline::PublisherPipeline;
use crate::spi::AsyncEventBusSpi;
use crate::spi::DelayedDeliveryCapability;
use crate::spi::DurabilityCapability;
use crate::spi::EventBusCapabilities;
use crate::spi::EventBusSpi;
use crate::spi::EventSubscriptionSpi;
use crate::spi::OrderingCapability;
use crate::spi::OutboundMessage;
use crate::spi::PayloadModes;
use crate::spi::PublishGuarantee;
use crate::spi::PublishVisibility;
use crate::spi::ReplayCapability;
use crate::spi::SettlementCapabilities;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::SpiFuture;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TransportPayload;

#[derive(Default)]
struct State {
    calls: usize,
    fail_count: usize,
    reject_destination: bool,
    published_headers: Vec<Option<String>>,
    payload_was_encoded: Option<bool>,
}

struct FakeBus {
    state: Arc<Mutex<State>>,
    payload_modes: PayloadModes,
}

impl EventBusSpi for FakeBus {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            self.payload_modes,
            SettlementCapabilities::None,
            OrderingCapability::PerKey,
            DelayedDeliveryCapability::Native,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let mut state = self.state.lock().expect("test state poisoned");
        state.calls += 1;
        state.published_headers.push(message.headers().get("typed").cloned());
        state.payload_was_encoded = Some(matches!(message.payload(), TransportPayload::Encoded(_)));
        if state.calls <= state.fail_count {
            return Err(SpiError::Operation {
                provider_id: "fake".into(),
                operation: "publish",
                resource: Some(message.topic().as_str().into()),
                kind: "transient",
                retryable: Some(true),
                source: Box::new(std::io::Error::other("temporary failure")),
            });
        }
        if state.reject_destination {
            Ok(PublishAcknowledgement::DestinationAdmissions(vec![
                DestinationAdmission::new(
                    Id::new(1),
                    SubscriberId::new("subscriber").unwrap(),
                    AdmissionStatus::Rejected("queue full".into()),
                ),
            ]))
        } else {
            Ok(PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            })
        }
    }

    fn subscribe(&self, _request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        unreachable!("publisher tests do not subscribe")
    }

    fn shutdown(&self, _mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

impl AsyncEventBusSpi for FakeBus {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusSpi::capabilities(self)
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move { EventBusSpi::publish(self, message) })
    }

    fn subscribe<'a>(
        &'a self,
        _request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn crate::spi::AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async { unreachable!("publisher tests do not subscribe") })
    }

    fn shutdown<'a>(&'a self, _mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

fn bus(payload_modes: PayloadModes, fail_count: usize) -> (Arc<FakeBus>, Arc<Mutex<State>>) {
    let state = Arc::new(Mutex::new(State {
        fail_count,
        ..State::default()
    }));
    (
        Arc::new(FakeBus {
            state: state.clone(),
            payload_modes,
        }),
        state,
    )
}

fn make_pipeline(_bus: &Arc<FakeBus>) -> PublisherPipeline {
    PublisherPipeline::new(ProviderId::new("fake").unwrap(), Arc::default())
}

fn request(options: PublishOptions<String>) -> PublishRequest<String> {
    PublishRequest::builder()
        .topic(Topic::new("orders.created").unwrap())
        .payload("order-1".to_owned())
        .event_id(crate::model::EventId::new("test-event").unwrap())
        .options(options)
        .build()
        .unwrap()
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    struct ThreadWake(std::thread::Thread);
    impl std::task::Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(result) => return result,
            std::task::Poll::Pending => std::thread::park(),
        }
    }
}

struct StringCodec {
    content_type: ContentType,
}

impl EventCodec<String> for StringCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }
    fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

#[test]
fn typed_interceptor_runs_before_global_and_drop_short_circuits_spi() {
    let (spi, state) = bus(PayloadModes::Native, 0);
    let pipeline = make_pipeline(&spi);
    let options = PublishOptions::builder()
        .interceptor(|mut envelope: crate::model::EventEnvelope<String>| {
            envelope.set_header("typed", "yes")?;
            Ok(Some(envelope))
        })
        .build();
    let seen = Arc::new(Mutex::new(None));
    let capture = seen.clone();
    let global = GlobalPublisherInterceptor::new(move |metadata| {
        *capture.lock().unwrap() = metadata.header("typed").map(str::to_owned);
        Ok(true)
    });
    pipeline
        .publish(spi.as_ref(), request(options), &[global], &[])
        .unwrap();
    assert_eq!(seen.lock().unwrap().as_deref(), Some("yes"));
    assert_eq!(state.lock().unwrap().calls, 1);

    let (spi, state) = bus(PayloadModes::Native, 0);
    let pipeline = make_pipeline(&spi);
    let global = GlobalPublisherInterceptor::new(|_| Ok(false));
    let receipt = pipeline
        .publish(spi.as_ref(), request(PublishOptions::new()), &[global], &[])
        .unwrap();
    assert!(receipt.acknowledgement().is_dropped());
    assert_eq!(state.lock().unwrap().calls, 0);
}

#[test]
fn retry_exhaustion_preserves_retry_source_and_failure_origin() {
    let (spi, _) = bus(PayloadModes::Native, usize::MAX);
    let pipeline = make_pipeline(&spi);
    let options = PublishOptions::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .build();
    let failure = pipeline.publish(spi.as_ref(), request(options), &[], &[]).unwrap_err();
    assert_eq!(failure.origin(), PipelineFailureOrigin::Retry);
    assert!(matches!(
        failure.error(),
        crate::error::EventBusError::Publish(PublishError::Retry(_))
    ));
    assert!(std::error::Error::source(failure.error()).is_some());
}

#[test]
fn retry_replays_the_same_prepared_message_until_provider_accepts() {
    let (spi, state) = bus(PayloadModes::Native, 1);
    let pipeline = make_pipeline(&spi);
    let options = PublishOptions::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .build();
    pipeline.publish(spi.as_ref(), request(options), &[], &[]).unwrap();
    assert_eq!(state.lock().unwrap().calls, 2);
}

#[test]
fn interceptor_panic_is_converted_with_pipeline_origin() {
    let (spi, _) = bus(PayloadModes::Native, 0);
    let pipeline = make_pipeline(&spi);
    let options = PublishOptions::builder()
        .interceptor(
            |_| -> Result<Option<crate::model::EventEnvelope<String>>, PublishError> {
                panic!("typed interceptor failed")
            },
        )
        .build();
    let failure = pipeline.publish(spi.as_ref(), request(options), &[], &[]).unwrap_err();
    assert_eq!(failure.origin(), PipelineFailureOrigin::Interceptor);
    assert!(matches!(
        failure.error(),
        crate::error::EventBusError::Publish(PublishError::InterceptorPanicked { scope: "typed", .. })
    ));
}

#[test]
fn encoded_only_provider_uses_the_topic_codec_and_rejects_missing_codec() {
    let (spi, state) = bus(PayloadModes::Encoded, 0);
    let pipeline = make_pipeline(&spi);
    let topic = Topic::new_with_codec(
        "orders.encoded",
        StringCodec {
            content_type: ContentType::new("text/plain").unwrap(),
        },
    )
    .unwrap();
    let encoded_request = PublishRequest::builder()
        .topic(topic)
        .payload("serialized".to_owned())
        .event_id(crate::model::EventId::new("encoded-event").unwrap())
        .build()
        .unwrap();
    pipeline.publish(spi.as_ref(), encoded_request, &[], &[]).unwrap();
    assert_eq!(state.lock().unwrap().payload_was_encoded, Some(true));

    let request = request(PublishOptions::new());
    let failure = pipeline.publish(spi.as_ref(), request, &[], &[]).unwrap_err();
    assert_eq!(failure.origin(), PipelineFailureOrigin::Capability);
}

#[test]
fn native_payload_does_not_require_clone() {
    struct NonClonePayload;

    let (spi, state) = bus(PayloadModes::Native, 0);
    let pipeline = make_pipeline(&spi);
    let request = PublishRequest::builder()
        .topic(Topic::<NonClonePayload>::new("orders.native").unwrap())
        .payload(NonClonePayload)
        .event_id(crate::model::EventId::new("non-clone-event").unwrap())
        .build()
        .unwrap();
    pipeline.publish(spi.as_ref(), request, &[], &[]).unwrap();
    assert_eq!(state.lock().unwrap().payload_was_encoded, Some(false));
}

#[test]
fn native_and_encoded_provider_prefers_native_payload() {
    let (spi, state) = bus(PayloadModes::NativeAndEncoded, 0);
    let pipeline = make_pipeline(&spi);
    let topic = Topic::new_with_codec(
        "orders.hybrid",
        StringCodec {
            content_type: ContentType::new("text/plain").unwrap(),
        },
    )
    .unwrap();
    let request = PublishRequest::builder()
        .topic(topic)
        .payload("hybrid".to_owned())
        .event_id(crate::model::EventId::new("hybrid-event").unwrap())
        .build()
        .unwrap();
    pipeline.publish(spi.as_ref(), request, &[], &[]).unwrap();
    assert_eq!(state.lock().unwrap().payload_was_encoded, Some(false));
}

#[test]
fn destination_rejection_emits_one_diagnostic_and_observer_panic_is_isolated() {
    let (spi, state) = bus(PayloadModes::Native, 0);
    state.lock().unwrap().reject_destination = true;
    let pipeline = make_pipeline(&spi);
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let sink = diagnostics.clone();
    let observer: Arc<dyn Fn(&Diagnostic) + Send + Sync> =
        Arc::new(move |diagnostic: &Diagnostic| sink.lock().unwrap().push(diagnostic.clone()));
    let receipt = pipeline
        .publish(spi.as_ref(), request(PublishOptions::new()), &[], &[observer])
        .unwrap();
    assert!(!receipt.acknowledgement().is_dropped());
    assert_eq!(diagnostics.lock().unwrap().len(), 1);

    let (spi, state) = bus(PayloadModes::Native, 0);
    state.lock().unwrap().reject_destination = true;
    let pipeline = make_pipeline(&spi);
    let observer: Arc<dyn Fn(&Diagnostic) + Send + Sync> = Arc::new(|_| panic!("observer failure"));
    assert!(
        pipeline
            .publish(spi.as_ref(), request(PublishOptions::new()), &[], &[observer])
            .is_ok()
    );
}

#[test]
fn async_publish_is_runtime_neutral() {
    let (spi, state) = bus(PayloadModes::Native, 0);
    let pipeline = make_pipeline(&spi);
    let future = pipeline.publish_async(
        spi.as_ref(),
        request(PublishOptions::new()),
        &[],
        &[],
        StdMonotonicClock::new().new_timer(),
    );
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    use std::future::Future;
    assert!(matches!(
        future.as_mut().poll(&mut context),
        std::task::Poll::Ready(Ok(_))
    ));
    assert_eq!(state.lock().unwrap().calls, 1);
}

#[test]
fn async_publish_retries_after_retryable_failure_without_runtime() {
    let (spi, state) = bus(PayloadModes::Native, 1);
    let pipeline = make_pipeline(&spi);
    let options = PublishOptions::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .build();
    let receipt = block_on(pipeline.publish_async(
        spi.as_ref(),
        request(options),
        &[],
        &[],
        StdMonotonicClock::new().new_timer(),
    ))
    .unwrap();
    assert!(!receipt.acknowledgement().is_dropped());
    assert_eq!(state.lock().unwrap().calls, 2);
}

#[test]
fn publish_error_observers_receive_shared_non_clone_payload_and_metadata() {
    struct NonClonePayload {
        value: &'static str,
    }

    let (spi, _) = bus(PayloadModes::Native, usize::MAX);
    let pipeline = make_pipeline(&spi);
    let seen = Arc::new(Mutex::new(None));
    let capture = seen.clone();
    let timestamp = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(123);
    let options = PublishOptions::<NonClonePayload>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
        .error_handler(move |context, _error| {
            *capture.lock().unwrap() = Some((
                context.payload().value,
                context.event_id().as_str().to_owned(),
                context.topic().name().to_owned(),
                context.header("trace").map(str::to_owned),
                context.ordering_key().map(str::to_owned),
                context.timestamp(),
                context.delay(),
            ));
        })
        .build();
    let request = PublishRequest::builder()
        .topic(Topic::<NonClonePayload>::new("orders.native").unwrap())
        .payload(NonClonePayload { value: "payload" })
        .event_id(crate::model::EventId::new("failure-event").unwrap())
        .header("trace", "trace-1")
        .ordering_key("order-key")
        .timestamp(timestamp)
        .delay(std::time::Duration::from_secs(5))
        .options(options)
        .build()
        .unwrap();

    let failure = pipeline.publish(spi.as_ref(), request, &[], &[]).unwrap_err();
    assert_eq!(failure.origin(), PipelineFailureOrigin::Retry);
    let observed = seen.lock().unwrap().take().expect("handler should run");
    assert_eq!(observed.0, "payload");
    assert_eq!(observed.1, "failure-event");
    assert_eq!(observed.2, "orders.native");
    assert_eq!(observed.3.as_deref(), Some("trace-1"));
    assert_eq!(observed.4.as_deref(), Some("order-key"));
    assert_eq!(observed.5, timestamp);
    assert_eq!(observed.6, Some(std::time::Duration::from_secs(5)));
}

#[test]
fn publish_error_handler_panics_are_isolated_and_keep_terminal_source() {
    let (spi, _) = bus(PayloadModes::Native, usize::MAX);
    let pipeline = make_pipeline(&spi);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let first = calls.clone();
    let second = calls.clone();
    let options = PublishOptions::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
        .error_handler(move |_, _| {
            first.lock().unwrap().push("panicking");
            panic!("observer panic");
        })
        .error_handler(move |_, _| {
            second.lock().unwrap().push("later");
        })
        .build();

    let failure = pipeline.publish(spi.as_ref(), request(options), &[], &[]).unwrap_err();
    assert_eq!(*calls.lock().unwrap(), ["panicking", "later"]);
    assert_eq!(failure.origin(), PipelineFailureOrigin::Retry);
    let crate::error::EventBusError::Publish(PublishError::ErrorHandlerPanicked { source, .. }) = failure.error()
    else {
        panic!("expected structured publish observer panic, got {:?}", failure.error());
    };
    assert!(matches!(
        source.downcast_ref::<PublishError>(),
        Some(PublishError::Retry(_))
    ));
    assert!(std::error::Error::source(source.as_ref()).is_some());
}
