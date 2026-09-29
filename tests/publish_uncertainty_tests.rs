// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for conservative publication evidence across retries.
mod support;
use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::error::PublishAttemptError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::DuplicateRiskPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryPolicy;

struct Scripted {
    calls: Mutex<Vec<OutboundMessage>>,
    succeeds: bool,
    generic: bool,
    panics: bool,
    pending: bool,
    encoded: bool,
    reject_then_pending: bool,
    immediate_ack: bool,
}
impl Scripted {
    fn new(succeeds: bool) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            succeeds,
            generic: false,
            panics: false,
            pending: false,
            encoded: false,
            reject_then_pending: false,
            immediate_ack: false,
        }
    }
    fn attempt(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let mut calls = self.calls.lock().unwrap();
        let first = calls.is_empty();
        calls.push(message);
        drop(calls);
        if first && self.panics {
            panic!("provider panic");
        }
        if first && self.generic {
            return Err(SpiError::Operation {
                provider_id: "scripted".into(),
                operation: "publish",
                resource: None,
                kind: "lost_response",
                retryable: Some(true),
                source: Box::new(std::io::Error::other("generic source")),
            });
        }
        if self.immediate_ack || (!first && self.succeeds) {
            return Ok(PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            });
        }
        Err(SpiError::Publish {
            provider_id: "scripted".into(),
            resource: None,
            kind: "injected",
            retryable: Some(true),
            effect: if first && !self.reject_then_pending {
                PublishEffect::MayHaveBeenAccepted
            } else {
                PublishEffect::NotAccepted
            },
            source: Box::new(std::io::Error::other("original provider source")),
        })
    }
}
impl EventBusSpi for Scripted {
    fn capabilities(&self) -> EventBusCapabilities {
        if self.encoded {
            EventBusCapabilities::new(
                PayloadModes::Encoded,
                SettlementCapabilities::AcceptRetryReject,
                OrderingCapability::PerSubscription,
                DelayedDeliveryCapability::None,
                DurabilityCapability::Ephemeral,
                SubscriptionModes::EPHEMERAL,
                false,
                ReplayCapability::None,
                PublishGuarantee::Accepted,
                PublishVisibility::Opaque,
            )
        } else {
            support::fake_spi::full_capabilities()
        }
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.attempt(message)
    }
    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        unreachable!()
    }
    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}
impl AsyncEventBusSpi for Scripted {
    fn capabilities(&self) -> EventBusCapabilities {
        if self.encoded {
            EventBusCapabilities::new(
                PayloadModes::Encoded,
                SettlementCapabilities::AcceptRetryReject,
                OrderingCapability::PerSubscription,
                DelayedDeliveryCapability::None,
                DurabilityCapability::Ephemeral,
                SubscriptionModes::EPHEMERAL,
                false,
                ReplayCapability::None,
                PublishGuarantee::Accepted,
                PublishVisibility::Opaque,
            )
        } else {
            support::fake_spi::full_capabilities()
        }
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            let prior_attempt = !self.calls.lock().unwrap().is_empty();
            if self.pending || (self.reject_then_pending && prior_attempt) {
                self.calls.lock().unwrap().push(message);
                std::future::pending().await
            } else {
                self.attempt(message)
            }
        })
    }
    fn subscribe<'a>(
        &'a self,
        _: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async { unreachable!() })
    }
    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}
fn request(policy: DuplicateRiskPolicy) -> PublishRequest<String> {
    PublishRequest::builder()
        .topic(Topic::new("uncertain.test").unwrap())
        .payload("stable bytes".to_owned())
        .duplicate_risk_policy(policy)
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .retry_rule(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .build()
        .unwrap()
}
#[test]
fn sync_forbid_is_a_hard_gate() {
    let spi = Arc::new(Scripted::new(true));
    let bus = EventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let failure = bus
        .publish(request(DuplicateRiskPolicy::Forbid))
        .expect_err("uncertainty stops retries");
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(spi.calls.lock().unwrap().len(), 1);
}
#[test]
fn sync_allow_keeps_prior_unknown_on_failure() {
    let spi = Arc::new(Scripted::new(false));
    let bus = EventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let failure = bus.publish(request(DuplicateRiskPolicy::AllowDuplicates)).unwrap_err();
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(spi.calls.lock().unwrap().len(), 2);
}
#[test]
fn sync_allow_success_reports_duplicates_and_stable_metadata() {
    let spi = Arc::new(Scripted::new(true));
    let bus = EventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let receipt = bus.publish(request(DuplicateRiskPolicy::AllowDuplicates)).unwrap();
    assert!(receipt.duplicate_possible());
    let calls = spi.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].id(), calls[1].id());
    assert_eq!(calls[0].timestamp(), calls[1].timestamp());
}
#[test]
fn async_forbid_is_a_hard_gate() {
    let spi = Arc::new(Scripted::new(true));
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let failure = support::manual_async::block_on(bus.publish(request(DuplicateRiskPolicy::Forbid)))
        .expect_err("uncertainty stops retries");
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(spi.calls.lock().unwrap().len(), 1);
}
#[test]
fn async_allow_keeps_prior_unknown_on_failure() {
    let spi = Arc::new(Scripted::new(false));
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let failure =
        support::manual_async::block_on(bus.publish(request(DuplicateRiskPolicy::AllowDuplicates))).unwrap_err();
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
}
#[test]
fn async_allow_success_reports_duplicates() {
    let spi = Arc::new(Scripted::new(true));
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").unwrap(), spi).unwrap();
    assert!(
        support::manual_async::block_on(bus.publish(request(DuplicateRiskPolicy::AllowDuplicates)))
            .unwrap()
            .duplicate_possible()
    );
}

#[test]
fn generic_publish_error_is_conservatively_unknown() {
    let mut scripted = Scripted::new(true);
    scripted.generic = true;
    let spi = Arc::new(scripted);
    let bus = EventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    assert_eq!(
        bus.publish(request(DuplicateRiskPolicy::Forbid)).unwrap_err().effect(),
        PublishEffect::MayHaveBeenAccepted
    );
    assert_eq!(spi.calls.lock().unwrap().len(), 1);
}
#[test]
fn provider_panic_is_conservatively_unknown() {
    let mut scripted = Scripted::new(true);
    scripted.panics = true;
    let spi = Arc::new(scripted);
    let bus = EventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    assert_eq!(
        bus.publish(request(DuplicateRiskPolicy::Forbid)).unwrap_err().effect(),
        PublishEffect::MayHaveBeenAccepted
    );
    assert_eq!(spi.calls.lock().unwrap().len(), 1);
}
#[test]
fn error_handler_panic_preserves_original_failure_identity_and_effect() {
    use qubit_event_bus::error::PublishError;
    use qubit_event_bus::error::PublishFailure;
    let spi = Arc::new(Scripted::new(false));
    let bus = EventBus::from_spi(ProviderId::new("scripted").unwrap(), spi).unwrap();
    let observed = Arc::new(Mutex::new(None));
    let record = observed.clone();
    let request = PublishRequest::builder()
        .topic(Topic::new("uncertain.handler").unwrap())
        .payload("payload".to_owned())
        .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .error_handler(move |context, failure| {
            *record.lock().unwrap() = Some((context.event_id().clone(), failure.effect()));
            panic!("handler panic");
        })
        .build()
        .unwrap();
    let id = request.envelope().id().clone();
    let failure = bus.publish(request).unwrap_err();
    assert_eq!(failure.event_id(), &id);
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(
        *observed.lock().unwrap(),
        Some((id.clone(), PublishEffect::MayHaveBeenAccepted))
    );
    let PublishError::ErrorHandlerPanicked { source, .. } = failure.cause() else {
        panic!("handler panic wrapper")
    };
    let original = source.downcast_ref::<PublishFailure>().expect("original failure");
    assert_eq!(original.event_id(), &id);
    assert_eq!(original.effect(), PublishEffect::MayHaveBeenAccepted);
    assert!(matches!(original.cause(), PublishError::Retry(_)));
}
#[test]
fn allow_duplicates_does_not_force_retry_without_a_policy() {
    let spi = Arc::new(Scripted::new(true));
    let bus = EventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let request = PublishRequest::builder()
        .topic(Topic::new("no.policy").unwrap())
        .payload("payload".to_owned())
        .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
        .build()
        .unwrap();
    assert_eq!(
        bus.publish(request).unwrap_err().effect(),
        PublishEffect::MayHaveBeenAccepted
    );
    assert_eq!(spi.calls.lock().unwrap().len(), 1);
}
#[test]
fn async_unpolled_publish_makes_no_provider_attempt() {
    let spi = Arc::new(Scripted::new(true));
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let future = bus.publish(request(DuplicateRiskPolicy::AllowDuplicates));
    drop(future);
    assert!(spi.calls.lock().unwrap().is_empty());
}

/// Sends source records through the real facade while losing all DLQ responses.
struct DeadLetterSpi {
    sync: support::fake_spi::FakeEventBusSpi,
    asynchronous: support::fake_spi::FakeAsyncEventBusSpi,
    attempts: std::sync::atomic::AtomicUsize,
}
impl DeadLetterSpi {
    fn new() -> Self {
        Self {
            sync: support::fake_spi::FakeEventBusSpi::with_capabilities(durable_capabilities()),
            asynchronous: support::fake_spi::FakeAsyncEventBusSpi::with_capabilities(durable_capabilities()),
            attempts: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    fn lost_response(&self) -> SpiError {
        self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        SpiError::Publish {
            provider_id: "dead-letter".into(),
            resource: None,
            kind: "response_lost",
            retryable: Some(true),
            effect: PublishEffect::MayHaveBeenAccepted,
            source: Box::new(std::io::Error::other("DLQ response lost")),
        }
    }
}
impl EventBusSpi for DeadLetterSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.sync.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        if message.topic().as_str() == "uncertain.dead" {
            Err(self.lost_response())
        } else {
            self.sync.publish(message)
        }
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        self.sync.subscribe(request)
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.sync.shutdown(mode)
    }
}
impl AsyncEventBusSpi for DeadLetterSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.asynchronous.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        if message.topic().as_str() == "uncertain.dead" {
            Box::pin(async { Err(self.lost_response()) })
        } else {
            self.asynchronous.publish(message)
        }
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.asynchronous.subscribe(request)
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.asynchronous.shutdown(mode)
    }
}
fn durable_capabilities() -> EventBusCapabilities {
    use qubit_event_bus::spi::DelayedDeliveryCapability;
    use qubit_event_bus::spi::DurabilityCapability;
    use qubit_event_bus::spi::OrderingCapability;
    use qubit_event_bus::spi::PayloadModes;
    use qubit_event_bus::spi::PublishGuarantee;
    use qubit_event_bus::spi::PublishVisibility;
    use qubit_event_bus::spi::ReplayCapability;
    use qubit_event_bus::spi::SettlementCapabilities;
    use qubit_event_bus::spi::SubscriptionModes;
    EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::AcceptRetryReject,
        OrderingCapability::PerSubscription,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Durable,
        SubscriptionModes::BOTH,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    )
}
fn dead_letter_options() -> qubit_event_bus::model::SubscribeOptions<u32> {
    use qubit_event_bus::model::DeadLetterPolicy;
    use qubit_event_bus::model::FailureDirective;
    use qubit_event_bus::model::SubscribeOptions;
    SubscribeOptions::builder()
        .durability(qubit_event_bus::model::SubscriptionDurability::Durable)
        .retry_policy(RetryPolicy::builder().max_attempts(3).build().unwrap())
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::with_topic_name("uncertain.dead").unwrap())
        .build()
}
#[test]
fn sync_uncertain_dlq_stops_without_settling_source_or_blind_retry() {
    use qubit_event_bus::model::Delivery;
    use qubit_event_bus::model::SubscribeRequest;
    use qubit_event_bus::pipeline::Diagnostic;
    let spi = Arc::new(DeadLetterSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("dead-letter").unwrap(), spi.clone()).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::DeliveryFailed { .. }) {
            tx.send(()).unwrap();
        }
    });
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("dlq-source", Topic::new("test.topic").unwrap())
                .unwrap()
                .with_options(dead_letter_options()),
            |_: Delivery<u32>| {
                Err(qubit_event_bus::DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("handler failure")),
                })
            },
        )
        .unwrap();
    bus.publish(PublishRequest::new(Topic::new("test.topic").unwrap(), 42_u32).unwrap())
        .unwrap();
    rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
    assert_eq!(spi.attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(!spi.sync.operation_log().contains(&"settle"));
    subscription.cancel().unwrap();
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}
#[test]
fn async_uncertain_dlq_stops_without_settling_source_or_blind_retry() {
    use qubit_event_bus::model::SubscribeRequest;
    use qubit_event_bus::spi::SettlementToken;
    let spi = Arc::new(DeadLetterSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("dead-letter").unwrap(), spi.clone()).unwrap();
    support::manual_async::block_on(async {
        let mut subscription = bus
            .subscribe(
                SubscribeRequest::new("dlq-source", Topic::new("test.topic").unwrap())
                    .unwrap()
                    .with_options(dead_letter_options()),
            )
            .await
            .unwrap();
        spi.asynchronous
            .enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
                subscription.id(),
                "source-token",
            ))));
        let failure = subscription
            .run(|_| async {
                Err(qubit_event_bus::DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("handler failure")),
                })
            })
            .await
            .unwrap_err();
        assert!(matches!(
            failure,
            qubit_event_bus::error::ReceiveError::DeadLetterForwardFailed { .. }
        ));
        assert_eq!(spi.attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(spi.asynchronous.settlement_dispositions().is_empty());
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        assert_eq!(spi.asynchronous.receiver_drop_recoveries(), 1);
    });
}

#[test]
fn cancelled_pending_async_publish_does_not_invent_a_terminal_result() {
    let mut scripted = Scripted::new(true);
    scripted.pending = true;
    let spi = Arc::new(scripted);
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let mut future = Box::pin(bus.publish(request(DuplicateRiskPolicy::AllowDuplicates)));
    assert!(support::manual_async::poll_once(future.as_mut()).is_pending());
    drop(future);
    assert_eq!(spi.calls.lock().unwrap().len(), 1);
    assert_eq!(bus.publish_metrics().attempts, 1);
    assert_eq!(bus.publish_metrics().errors, 0);
    assert_eq!(bus.publish_metrics().opaque_accepted, 0);
}
struct TextCodec(qubit_event_bus::model::ContentType);
impl qubit_event_bus::codec::EventCodec<String> for TextCodec {
    fn content_type(&self) -> &qubit_event_bus::model::ContentType {
        &self.0
    }
    fn schema_id(&self) -> Option<&qubit_event_bus::model::SchemaId> {
        None
    }
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, qubit_event_bus::error::CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }
    fn decode(
        &self,
        payload: &qubit_event_bus::spi::EncodedPayload,
    ) -> Result<String, qubit_event_bus::error::CodecError> {
        Ok(String::from_utf8_lossy(payload.bytes()).into_owned())
    }
}
#[test]
fn allow_duplicates_preserves_encoded_bytes_identity_and_timestamp() {
    let mut scripted = Scripted::new(true);
    scripted.encoded = true;
    let spi = Arc::new(scripted);
    let bus = EventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let topic = Topic::new_with_codec(
        "encoded.uncertain",
        TextCodec(qubit_event_bus::model::ContentType::new("text/plain").unwrap()),
    )
    .unwrap();
    let request = PublishRequest::builder()
        .topic(topic)
        .payload("same encoded bytes".to_owned())
        .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .build()
        .unwrap();
    assert!(bus.publish(request).unwrap().duplicate_possible());
    let calls = spi.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].id(), calls[1].id());
    assert_eq!(calls[0].timestamp(), calls[1].timestamp());
    let qubit_event_bus::spi::TransportPayload::Encoded(first) = calls[0].payload() else {
        panic!("encoded payload")
    };
    let qubit_event_bus::spi::TransportPayload::Encoded(second) = calls[1].payload() else {
        panic!("encoded payload")
    };
    assert_eq!(first.bytes(), second.bytes());
    assert_eq!(first.bytes(), b"same encoded bytes");
}

/// Builds a pending publication with a real cancellation token and observable
/// callback.
fn pending_request(
    token: qubit_retry::RetryCancellationToken,
    observed: Arc<Mutex<Option<PublishEffect>>>,
) -> PublishRequest<String> {
    PublishRequest::builder()
        .topic(Topic::new("pending.uncertain").unwrap())
        .payload("provider may already have admitted this".to_owned())
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .retry_cancellation_token(token)
        .error_handler(move |_, failure| *observed.lock().unwrap() = Some(failure.effect()))
        .build()
        .unwrap()
}
#[test]
fn pending_provider_token_cancellation_returns_unknown_with_original_identity() {
    let mut scripted = Scripted::new(true);
    scripted.pending = true;
    let spi = Arc::new(scripted);
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let token = qubit_retry::RetryCancellationToken::new();
    let observed = Arc::new(Mutex::new(None));
    let request = pending_request(token.clone(), observed.clone());
    let id = request.envelope().id().clone();
    let mut future = Box::pin(bus.publish(request));
    assert!(support::manual_async::poll_once(future.as_mut()).is_pending());
    assert_eq!(spi.calls.lock().unwrap().len(), 1);
    token.cancel();
    let failure = support::manual_async::block_on(future).unwrap_err();
    assert_eq!(failure.event_id(), &id);
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(*observed.lock().unwrap(), Some(PublishEffect::MayHaveBeenAccepted));
    assert_eq!(spi.calls.lock().unwrap().len(), 1);
}
#[test]
fn token_cancelled_before_spi_retains_not_accepted_without_provider_attempt() {
    let mut scripted = Scripted::new(true);
    scripted.pending = true;
    let spi = Arc::new(scripted);
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let token = qubit_retry::RetryCancellationToken::new();
    token.cancel();
    let observed = Arc::new(Mutex::new(None));
    let request = pending_request(token, observed.clone());
    let id = request.envelope().id().clone();
    let failure = support::manual_async::block_on(bus.publish(request)).unwrap_err();
    assert_eq!(failure.event_id(), &id);
    assert_eq!(failure.effect(), PublishEffect::NotAccepted);
    assert_eq!(*observed.lock().unwrap(), Some(PublishEffect::NotAccepted));
    assert!(spi.calls.lock().unwrap().is_empty());
}
fn assert_soft_budget_keeps_pending_until_token_cancellation(total_budget: bool) {
    use std::time::Duration;

    use qubit_clock::ManualMonotonicClock;
    use qubit_clock::MonotonicClock;
    let mut scripted = Scripted::new(true);
    scripted.pending = true;
    let spi = Arc::new(scripted);
    let clock = ManualMonotonicClock::new_shared();
    let bus = AsyncEventBus::with_timer(ProviderId::new("scripted").unwrap(), spi.clone(), clock.new_timer()).unwrap();
    let policy = RetryPolicy::builder().max_attempts(2);
    let policy = if total_budget {
        policy.total_time_budget(Duration::from_secs(1))
    } else {
        policy.operation_time_budget(Duration::from_secs(1))
    };
    let observed = Arc::new(Mutex::new(None));
    let callback = observed.clone();
    let token = qubit_retry::RetryCancellationToken::new();
    let request = PublishRequest::builder()
        .topic(Topic::new("budget.uncertain").unwrap())
        .payload("pending admission".to_owned())
        .retry_policy(policy.build().unwrap())
        .retry_cancellation_token(token.clone())
        .retry_rule(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .error_handler(move |_, failure| *callback.lock().unwrap() = Some(failure.effect()))
        .build()
        .unwrap();
    let id = request.envelope().id().clone();
    let mut future = Box::pin(bus.publish(request));
    assert!(support::manual_async::poll_once(future.as_mut()).is_pending());
    clock.advance(Duration::from_secs(1)).unwrap();
    // RetryPolicy budgets are soft accounting limits. Hard interruption is
    // an independent AsyncRetry runtime control, absent from this facade.
    assert!(support::manual_async::poll_once(future.as_mut()).is_pending());
    assert_eq!(spi.calls.lock().unwrap().len(), 1);
    token.cancel();
    let failure = support::manual_async::block_on(future).unwrap_err();
    assert_eq!(failure.event_id(), &id);
    assert_eq!(
        failure.effect(),
        PublishEffect::MayHaveBeenAccepted,
        "total budget: {total_budget}"
    );
    assert_eq!(*observed.lock().unwrap(), Some(PublishEffect::MayHaveBeenAccepted));
    assert_eq!(spi.calls.lock().unwrap().len(), 1);
}
#[test]
fn pending_provider_soft_operation_budget_preserves_cancellation_uncertainty() {
    assert_soft_budget_keeps_pending_until_token_cancellation(false);
}
#[test]
fn pending_provider_soft_total_budget_preserves_cancellation_uncertainty() {
    assert_soft_budget_keeps_pending_until_token_cancellation(true);
}

#[test]
fn token_cancelled_during_second_pending_attempt_keeps_unknown_after_rejection() {
    let mut scripted = Scripted::new(false);
    scripted.reject_then_pending = true;
    let spi = Arc::new(scripted);
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let token = qubit_retry::RetryCancellationToken::new();
    let observed = Arc::new(Mutex::new(None));
    let request = pending_request(token.clone(), observed.clone());
    let id = request.envelope().id().clone();
    let mut future = Box::pin(bus.publish(request));
    assert!(support::manual_async::poll_once(future.as_mut()).is_pending());
    assert_eq!(spi.calls.lock().unwrap().len(), 2);
    token.cancel();
    let failure = support::manual_async::block_on(future).unwrap_err();
    assert_eq!(failure.event_id(), &id);
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(*observed.lock().unwrap(), Some(PublishEffect::MayHaveBeenAccepted));
}
#[test]
fn successful_first_async_attempt_does_not_report_possible_duplicates() {
    let mut scripted = Scripted::new(true);
    scripted.immediate_ack = true;
    let spi = Arc::new(scripted);
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").unwrap(), spi.clone()).unwrap();
    let receipt = support::manual_async::block_on(bus.publish(request(DuplicateRiskPolicy::Forbid))).unwrap();
    assert!(!receipt.duplicate_possible());
    assert_eq!(spi.calls.lock().unwrap().len(), 1);
}

/// A deterministic timer whose clock regresses only after provider
/// acknowledgement.
#[derive(Clone)]
struct AckRegressionTimer {
    manual: Arc<qubit_clock::ManualMonotonicClock>,
    early: qubit_clock::MonotonicInstant,
    acknowledged: Arc<std::sync::atomic::AtomicBool>,
}
impl AckRegressionTimer {
    fn new() -> Self {
        use qubit_clock::MonotonicClock;
        let manual = qubit_clock::ManualMonotonicClock::new_shared();
        let early = manual.now();
        manual.advance(std::time::Duration::from_secs(2)).unwrap();
        Self {
            manual,
            early,
            acknowledged: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}
impl qubit_clock::MonotonicClock for AckRegressionTimer {
    fn domain(&self) -> qubit_clock::ClockDomain {
        self.early.domain()
    }
    fn now(&self) -> qubit_clock::MonotonicInstant {
        if self.acknowledged.load(std::sync::atomic::Ordering::Acquire) {
            self.early
        } else {
            qubit_clock::MonotonicClock::now(self.manual.as_ref())
        }
    }
    fn new_timer(&self) -> Arc<dyn qubit_clock::Timer> {
        Arc::new(self.clone())
    }
}
impl qubit_clock::Timer for AckRegressionTimer {
    fn clock(&self) -> &dyn qubit_clock::MonotonicClock {
        self
    }
    fn at(&self, deadline: qubit_clock::MonotonicInstant) -> Result<qubit_clock::TimerFuture, qubit_clock::TimeError> {
        qubit_clock::MonotonicClock::new_timer(self.manual.as_ref()).at(deadline)
    }
}
struct AckFailureSpi {
    clock: AckRegressionTimer,
    rejects_first: bool,
    no_destinations: bool,
    calls: std::sync::atomic::AtomicUsize,
}
impl AsyncEventBusSpi for AckFailureSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        support::fake_spi::full_capabilities()
    }
    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            let first = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
            if first && self.rejects_first {
                return Err(SpiError::Publish {
                    provider_id: "ack-clock".into(),
                    resource: None,
                    kind: "rejected",
                    retryable: Some(true),
                    effect: PublishEffect::NotAccepted,
                    source: Box::new(std::io::Error::other("known rejection")),
                });
            }
            self.clock
                .acknowledged
                .store(true, std::sync::atomic::Ordering::Release);
            if self.no_destinations {
                Ok(PublishAcknowledgement::DestinationAdmissions(Vec::new()))
            } else {
                Ok(PublishAcknowledgement::Accepted {
                    provider_message_id: Some("accepted".to_owned()),
                    metadata: Default::default(),
                })
            }
        })
    }
    fn subscribe<'a>(
        &'a self,
        _: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async { unreachable!() })
    }
    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}
fn assert_ack_followed_by_clock_failure_effect(rejects_first: bool, no_destinations: bool) {
    let clock = AckRegressionTimer::new();
    let spi = Arc::new(AckFailureSpi {
        clock: clock.clone(),
        rejects_first,
        no_destinations,
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let bus = AsyncEventBus::with_timer(ProviderId::new("ack-clock").unwrap(), spi.clone(), Arc::new(clock)).unwrap();
    let observed = Arc::new(Mutex::new(None));
    let callback = observed.clone();
    let request = PublishRequest::builder()
        .topic(Topic::new("acked.clock.failure").unwrap())
        .payload("accepted before clock failure".to_owned())
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .error_handler(move |context, failure| {
            *callback.lock().unwrap() = Some((context.event_id().clone(), failure.effect()))
        })
        .build()
        .unwrap();
    let id = request.envelope().id().clone();
    let failure = support::manual_async::block_on(bus.publish(request)).expect_err("clock failure after ACK");
    assert!(matches!(
        failure.cause(),
        qubit_event_bus::error::PublishError::Retry(_)
    ));
    assert_eq!(failure.event_id(), &id);
    let expected_effect = if no_destinations {
        PublishEffect::NotAccepted
    } else {
        PublishEffect::MayHaveBeenAccepted
    };
    assert_eq!(failure.effect(), expected_effect);
    assert_eq!(*observed.lock().unwrap(), Some((id, expected_effect)));
    assert_eq!(
        spi.calls.load(std::sync::atomic::Ordering::SeqCst),
        if rejects_first { 2 } else { 1 }
    );
}
#[test]
fn first_ack_followed_by_clock_failure_is_not_misclassified_as_rejection() {
    assert_ack_followed_by_clock_failure_effect(false, false);
}
#[test]
fn rejection_then_ack_followed_by_clock_failure_keeps_admission_evidence() {
    assert_ack_followed_by_clock_failure_effect(true, false);
}

#[test]
fn no_destination_ack_then_clock_failure_retains_known_non_admission() {
    assert_ack_followed_by_clock_failure_effect(false, true);
}
