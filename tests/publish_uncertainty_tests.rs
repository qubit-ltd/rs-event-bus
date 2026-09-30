// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for conservative publication evidence across retries.
mod support;

use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_clock::ClockDomain;
use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::error::PublishAttemptError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::error::ReceiveError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::DuplicateRiskPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EncodedPayload;
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
use qubit_event_bus::spi::TransportPayload;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryPolicy;

/// Configures synchronous and asynchronous provider outcomes for retry tests.
///
/// The flags inject ambiguous failures, panics, pending futures, encoded
/// payload support, and terminal acknowledgements while `calls` records every
/// attempt for identity and retry-count assertions.
struct Scripted {
    calls: Mutex<Vec<OutboundMessage>>,
    succeeds: bool,
    generic: bool,
    panics: bool,
    pending: bool,
    encoded: bool,
    reject_then_pending: bool,
    immediate_ack: bool,
    terminal_ack: Option<PublishAcknowledgement>,
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
            terminal_ack: None,
        }
    }
    /// Records one message and returns the configured first or later outcome.
    ///
    /// The first attempt may panic or return a generic error; later attempts
    /// can acknowledge, remain pending, or return the configured terminal
    /// acknowledgement.
    fn attempt(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let mut calls = self.calls.lock().expect("attempt: calls mutex must not be poisoned");
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
                source: Box::new(IoError::other("generic source")),
            });
        }
        if !first && let Some(acknowledgement) = &self.terminal_ack {
            return Ok(acknowledgement.clone());
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
            source: Box::new(IoError::other("original provider source")),
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
            let prior_attempt = !self
                .calls
                .lock()
                .expect("capabilities: calls mutex must not be poisoned")
                .is_empty();
            if self.pending || (self.reject_then_pending && prior_attempt) {
                self.calls
                    .lock()
                    .expect("capabilities: calls mutex must not be poisoned")
                    .push(message);
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
/// Builds the shared stable-payload request used to compare duplicate-risk
/// policies across synchronous and asynchronous facades.
fn request(policy: DuplicateRiskPolicy) -> PublishRequest<String> {
    PublishRequest::builder()
        .topic(Topic::new("uncertain.test").expect("request: test topic name must be valid"))
        .payload("stable bytes".to_owned())
        .duplicate_risk_policy(policy)
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .build()
                .expect("request: retry policy must satisfy its attempt limit"),
        )
        .retry_rule(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .build()
        .expect("request: publish request configuration must pass validation")
}
#[test]
fn test_sync_forbid_is_a_hard_gate() {
    let spi = Arc::new(Scripted::new(true));
    let bus = EventBus::from_spi(
        ProviderId::new("scripted").expect("test_sync_forbid_is_a_hard_gate: test provider identity must be valid"),
        spi.clone(),
    )
    .expect("test_sync_forbid_is_a_hard_gate: facade must accept the scripted provider capabilities");
    let failure = bus
        .publish(request(DuplicateRiskPolicy::Forbid))
        .expect_err("uncertainty stops retries");
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(
        spi.calls
            .lock()
            .expect("test_sync_forbid_is_a_hard_gate: spi.calls mutex must not be poisoned")
            .len(),
        1
    );
}
#[test]
fn test_sync_allow_keeps_prior_unknown_on_failure() {
    let spi = Arc::new(Scripted::new(false));
    let bus = EventBus::from_spi(
        ProviderId::new("scripted")
            .expect("test_sync_allow_keeps_prior_unknown_on_failure: test provider identity must be valid"),
        spi.clone(),
    )
    .expect("test_sync_allow_keeps_prior_unknown_on_failure: facade must accept the scripted provider capabilities");
    let failure = bus.publish(request(DuplicateRiskPolicy::AllowDuplicates)).unwrap_err();
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(
        spi.calls
            .lock()
            .expect("test_sync_allow_keeps_prior_unknown_on_failure: spi.calls mutex must not be poisoned")
            .len(),
        2
    );
}
#[test]
fn test_sync_allow_success_reports_duplicates_and_stable_metadata() {
    let spi = Arc::new(Scripted::new(true));
    let bus = EventBus::from_spi(ProviderId::new("scripted").expect("test_sync_allow_success_reports_duplicates_and_stable_metadata: test provider identity must be valid"), spi.clone()).expect("test_sync_allow_success_reports_duplicates_and_stable_metadata: facade must accept the scripted provider capabilities");
    let receipt = bus.publish(request(DuplicateRiskPolicy::AllowDuplicates)).expect("test_sync_allow_success_reports_duplicates_and_stable_metadata: publication should produce the expected receipt");
    assert!(receipt.duplicate_possible());
    let calls = spi
        .calls
        .lock()
        .expect("test_sync_allow_success_reports_duplicates_and_stable_metadata: spi.calls mutex must not be poisoned");
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].id(), calls[1].id());
    assert_eq!(calls[0].timestamp(), calls[1].timestamp());
}
#[test]
fn test_async_forbid_is_a_hard_gate() {
    let spi = Arc::new(Scripted::new(true));
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("scripted").expect("test_async_forbid_is_a_hard_gate: test provider identity must be valid"),
        spi.clone(),
    )
    .expect("test_async_forbid_is_a_hard_gate: facade must accept the scripted provider capabilities");
    let failure = support::manual_async::block_on(bus.publish(request(DuplicateRiskPolicy::Forbid)))
        .expect_err("uncertainty stops retries");
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(
        spi.calls
            .lock()
            .expect("test_async_forbid_is_a_hard_gate: spi.calls mutex must not be poisoned")
            .len(),
        1
    );
}
#[test]
fn test_async_allow_keeps_prior_unknown_on_failure() {
    let spi = Arc::new(Scripted::new(false));
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("scripted")
            .expect("test_async_allow_keeps_prior_unknown_on_failure: test provider identity must be valid"),
        spi.clone(),
    )
    .expect("test_async_allow_keeps_prior_unknown_on_failure: facade must accept the scripted provider capabilities");
    let failure =
        support::manual_async::block_on(bus.publish(request(DuplicateRiskPolicy::AllowDuplicates))).unwrap_err();
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
}
#[test]
fn test_async_allow_success_reports_duplicates() {
    let spi = Arc::new(Scripted::new(true));
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("scripted")
            .expect("test_async_allow_success_reports_duplicates: test provider identity must be valid"),
        spi,
    )
    .expect("test_async_allow_success_reports_duplicates: facade must accept the scripted provider capabilities");
    assert!(
        support::manual_async::block_on(bus.publish(request(DuplicateRiskPolicy::AllowDuplicates)))
            .expect("test_async_allow_success_reports_duplicates: publication should produce the expected receipt")
            .duplicate_possible()
    );
}

#[test]
fn test_generic_publish_error_is_conservatively_unknown() {
    let mut scripted = Scripted::new(true);
    scripted.generic = true;
    let spi = Arc::new(scripted);
    let bus = EventBus::from_spi(
        ProviderId::new("scripted")
            .expect("test_generic_publish_error_is_conservatively_unknown: test provider identity must be valid"),
        spi.clone(),
    )
    .expect(
        "test_generic_publish_error_is_conservatively_unknown: facade must accept the scripted provider capabilities",
    );
    assert_eq!(
        bus.publish(request(DuplicateRiskPolicy::Forbid)).unwrap_err().effect(),
        PublishEffect::MayHaveBeenAccepted
    );
    assert_eq!(
        spi.calls
            .lock()
            .expect("test_generic_publish_error_is_conservatively_unknown: spi.calls mutex must not be poisoned")
            .len(),
        1
    );
}
#[test]
fn test_provider_panic_is_conservatively_unknown() {
    let mut scripted = Scripted::new(true);
    scripted.panics = true;
    let spi = Arc::new(scripted);
    let bus = EventBus::from_spi(
        ProviderId::new("scripted")
            .expect("test_provider_panic_is_conservatively_unknown: test provider identity must be valid"),
        spi.clone(),
    )
    .expect("test_provider_panic_is_conservatively_unknown: facade must accept the scripted provider capabilities");
    assert_eq!(
        bus.publish(request(DuplicateRiskPolicy::Forbid)).unwrap_err().effect(),
        PublishEffect::MayHaveBeenAccepted
    );
    assert_eq!(
        spi.calls
            .lock()
            .expect("test_provider_panic_is_conservatively_unknown: spi.calls mutex must not be poisoned")
            .len(),
        1
    );
}
#[test]
fn test_error_handler_panic_preserves_original_failure_identity_and_effect() {
    use qubit_event_bus::error::PublishError;
    use qubit_event_bus::error::PublishFailure;
    let spi = Arc::new(Scripted::new(false));
    let bus = EventBus::from_spi(ProviderId::new("scripted").expect("test_error_handler_panic_preserves_original_failure_identity_and_effect: test provider identity must be valid"), spi).expect("test_error_handler_panic_preserves_original_failure_identity_and_effect: facade must accept the scripted provider capabilities");
    let observed = Arc::new(Mutex::new(None));
    let record = observed.clone();
    let request = PublishRequest::builder()
        .topic(Topic::new("uncertain.handler").expect("test_error_handler_panic_preserves_original_failure_identity_and_effect: test topic name must be valid"))
        .payload("payload".to_owned())
        .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().expect("test_error_handler_panic_preserves_original_failure_identity_and_effect: retry policy must satisfy its attempt limit"))
        .error_handler(move |context, failure| {
            *record.lock().expect("test_error_handler_panic_preserves_original_failure_identity_and_effect: record mutex must not be poisoned") = Some((context.event_id().clone(), failure.effect()));
            panic!("handler panic");
        })
        .build()
        .expect("test_error_handler_panic_preserves_original_failure_identity_and_effect: configured test request must pass builder validation");
    let id = request.envelope().id().clone();
    let failure = bus.publish(request).unwrap_err();
    assert_eq!(failure.event_id(), &id);
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(
        *observed.lock().expect("test_error_handler_panic_preserves_original_failure_identity_and_effect: observed mutex must not be poisoned"),
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
fn test_allow_duplicates_does_not_force_retry_without_a_policy() {
    let spi = Arc::new(Scripted::new(true));
    let bus = EventBus::from_spi(ProviderId::new("scripted").expect("test_allow_duplicates_does_not_force_retry_without_a_policy: test provider identity must be valid"), spi.clone()).expect("test_allow_duplicates_does_not_force_retry_without_a_policy: facade must accept the scripted provider capabilities");
    let request = PublishRequest::builder()
        .topic(Topic::new("no.policy").expect("test_allow_duplicates_does_not_force_retry_without_a_policy: test topic name must be valid"))
        .payload("payload".to_owned())
        .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
        .build()
        .expect("test_allow_duplicates_does_not_force_retry_without_a_policy: configured test request must pass builder validation");
    assert_eq!(
        bus.publish(request).unwrap_err().effect(),
        PublishEffect::MayHaveBeenAccepted
    );
    assert_eq!(
        spi.calls
            .lock()
            .expect("test_allow_duplicates_does_not_force_retry_without_a_policy: spi.calls mutex must not be poisoned")
            .len(),
        1
    );
}
#[test]
fn test_async_unpolled_publish_makes_no_provider_attempt() {
    let spi = Arc::new(Scripted::new(true));
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("scripted")
            .expect("test_async_unpolled_publish_makes_no_provider_attempt: test provider identity must be valid"),
        spi.clone(),
    )
    .expect(
        "test_async_unpolled_publish_makes_no_provider_attempt: facade must accept the scripted provider capabilities",
    );
    let future = bus.publish(request(DuplicateRiskPolicy::AllowDuplicates));
    drop(future);
    assert!(
        spi.calls
            .lock()
            .expect("test_async_unpolled_publish_makes_no_provider_attempt: spi.calls mutex must not be poisoned")
            .is_empty()
    );
}

/// Sends source records through the real facade while losing all DLQ responses.
struct DeadLetterSpi {
    sync: support::fake_spi::FakeEventBusSpi,
    asynchronous: support::fake_spi::FakeAsyncEventBusSpi,
    attempts: AtomicUsize,
}
impl DeadLetterSpi {
    fn new() -> Self {
        Self {
            sync: support::fake_spi::FakeEventBusSpi::with_capabilities(durable_capabilities()),
            asynchronous: support::fake_spi::FakeAsyncEventBusSpi::with_capabilities(durable_capabilities()),
            attempts: AtomicUsize::new(0),
        }
    }
    /// Records a DLQ attempt whose response is lost after possible admission.
    fn lost_response(&self) -> SpiError {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        SpiError::Publish {
            provider_id: "dead-letter".into(),
            resource: None,
            kind: "response_lost",
            retryable: Some(true),
            effect: PublishEffect::MayHaveBeenAccepted,
            source: Box::new(IoError::other("DLQ response lost")),
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
/// Returns the durable, replay-capable provider profile required by DLQ tests.
fn durable_capabilities() -> EventBusCapabilities {
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
/// Builds a durable subscription policy that routes terminal failures to the
/// scripted DLQ topic.
fn dead_letter_options() -> qubit_event_bus::model::SubscribeOptions<u32> {
    use qubit_event_bus::model::DeadLetterPolicy;
    use qubit_event_bus::model::FailureDirective;
    use qubit_event_bus::model::SubscribeOptions;
    SubscribeOptions::builder()
        .durability(qubit_event_bus::model::SubscriptionDurability::Durable)
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .build()
                .expect("dead_letter_options: retry policy must satisfy its attempt limit"),
        )
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(
            DeadLetterPolicy::with_topic_name("uncertain.dead")
                .expect("dead_letter_options: dead-letter destination must be valid"),
        )
        .build()
}
#[test]
fn test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry() {
    use qubit_event_bus::model::Delivery;
    use qubit_event_bus::model::SubscribeRequest;
    use qubit_event_bus::pipeline::Diagnostic;
    let spi = Arc::new(DeadLetterSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("dead-letter").expect("test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: test provider identity must be valid"), spi.clone()).expect("test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: facade must accept the scripted provider capabilities");
    let (tx, rx) = std::sync::mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::DeliveryFailed { .. }) {
            tx.send(()).expect("test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: test synchronization receiver must remain connected");
        }
    });
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("dlq-source", Topic::new("test.topic").expect("test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: test topic name must be valid"))
                .expect("test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: provider subscription must be created successfully")
                .with_options(dead_letter_options()),
            |_: Delivery<u32>| {
                Err(DeliveryError::Handler {
                    source: Box::new(IoError::other("handler failure")),
                })
            },
        )
        .expect("test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: dead-letter source subscription must register successfully");
    let _ = bus.publish(PublishRequest::new(Topic::new("test.topic").expect("test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: test topic name must be valid"), 42_u32).expect("test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: publish request must accept the valid topic"))
        .expect("test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: publication should produce the expected receipt");
    rx.recv_timeout(std::time::Duration::from_secs(2)).expect("test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: expected delivery signal must arrive before timeout");
    assert_eq!(spi.attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(!spi.sync.operation_log().contains(&"settle"));
    subscription.cancel().expect("test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: source subscription cancellation must succeed");
    let _ = bus.shutdown(ShutdownMode::Immediate).expect(
        "test_sync_uncertain_dlq_stops_without_settling_source_or_blind_retry: event bus shutdown must complete",
    );
}
#[test]
fn test_async_uncertain_dlq_stops_without_settling_source_or_blind_retry() {
    use qubit_event_bus::model::SubscribeRequest;
    use qubit_event_bus::spi::SettlementToken;
    let spi = Arc::new(DeadLetterSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("dead-letter").expect("test_async_uncertain_dlq_stops_without_settling_source_or_blind_retry: test provider identity must be valid"), spi.clone()).expect("test_async_uncertain_dlq_stops_without_settling_source_or_blind_retry: facade must accept the scripted provider capabilities");
    support::manual_async::block_on(async {
        let mut subscription = bus
            .subscribe(
                SubscribeRequest::new("dlq-source", Topic::new("test.topic").expect("test_async_uncertain_dlq_stops_without_settling_source_or_blind_retry: test topic name must be valid"))
                    .expect("test_async_uncertain_dlq_stops_without_settling_source_or_blind_retry: provider subscription must be created successfully")
                    .with_options(dead_letter_options()),
            )
            .await
            .expect("test_async_uncertain_dlq_stops_without_settling_source_or_blind_retry: dead-letter source subscription must register successfully");
        spi.asynchronous
            .enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
                subscription.id(),
                "source-token",
            ))));
        let failure = subscription
            .run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(IoError::other("handler failure")),
                })
            })
            .await
            .unwrap_err();
        assert!(matches!(failure, ReceiveError::DeadLetterForwardFailed { .. }));
        assert_eq!(spi.attempts.load(Ordering::SeqCst), 1);
        assert!(spi.asynchronous.settlement_dispositions().is_empty());
        let _ = bus.shutdown(ShutdownMode::Immediate).await.expect(
            "test_async_uncertain_dlq_stops_without_settling_source_or_blind_retry: event bus shutdown must complete",
        );
        assert_eq!(spi.asynchronous.receiver_drop_recoveries(), 1);
    });
}

#[test]
fn test_cancelled_pending_async_publish_does_not_invent_a_terminal_result() {
    let mut scripted = Scripted::new(true);
    scripted.pending = true;
    let spi = Arc::new(scripted);
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").expect("test_cancelled_pending_async_publish_does_not_invent_a_terminal_result: test provider identity must be valid"), spi.clone()).expect("test_cancelled_pending_async_publish_does_not_invent_a_terminal_result: facade must accept the scripted provider capabilities");
    let mut future = Box::pin(bus.publish(request(DuplicateRiskPolicy::AllowDuplicates)));
    assert!(support::manual_async::poll_once(future.as_mut()).is_pending());
    drop(future);
    assert_eq!(spi.calls.lock().expect("test_cancelled_pending_async_publish_does_not_invent_a_terminal_result: spi.calls mutex must not be poisoned").len(), 1);
    assert_eq!(bus.publish_metrics().attempts, 1);
    assert_eq!(bus.publish_metrics().errors, 0);
    assert_eq!(bus.publish_metrics().opaque_accepted, 0);
}
/// Minimal deterministic string codec for comparing encoded retry payloads.
struct TextCodec(qubit_event_bus::model::ContentType);
impl qubit_event_bus::codec::EventCodec<String> for TextCodec {
    fn content_type(&self) -> &qubit_event_bus::model::ContentType {
        &self.0
    }
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }
    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        Ok(String::from_utf8_lossy(payload.bytes()).into_owned())
    }
}
#[test]
fn test_allow_duplicates_preserves_encoded_bytes_identity_and_timestamp() {
    let mut scripted = Scripted::new(true);
    scripted.encoded = true;
    let spi = Arc::new(scripted);
    let bus = EventBus::from_spi(ProviderId::new("scripted").expect("test_allow_duplicates_preserves_encoded_bytes_identity_and_timestamp: test provider identity must be valid"), spi.clone()).expect("test_allow_duplicates_preserves_encoded_bytes_identity_and_timestamp: facade must accept the scripted provider capabilities");
    let topic = Topic::new("encoded.uncertain")
        .expect("test_allow_duplicates_preserves_encoded_bytes_identity_and_timestamp: codec-enabled topic configuration must be valid")
        .with_codec(TextCodec(qubit_event_bus::model::ContentType::new("text/plain").expect("test_allow_duplicates_preserves_encoded_bytes_identity_and_timestamp: codec content type must be valid")));
    let request = PublishRequest::builder()
        .topic(topic)
        .payload("same encoded bytes".to_owned())
        .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().expect("test_allow_duplicates_preserves_encoded_bytes_identity_and_timestamp: retry policy must satisfy its attempt limit"))
        .build()
        .expect("test_allow_duplicates_preserves_encoded_bytes_identity_and_timestamp: publish request configuration must pass validation");
    assert!(bus.publish(request).expect("test_allow_duplicates_preserves_encoded_bytes_identity_and_timestamp: publication should produce the expected receipt").duplicate_possible());
    let calls = spi.calls.lock().expect(
        "test_allow_duplicates_preserves_encoded_bytes_identity_and_timestamp: spi.calls mutex must not be poisoned",
    );
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].id(), calls[1].id());
    assert_eq!(calls[0].timestamp(), calls[1].timestamp());
    let TransportPayload::Encoded(first) = calls[0].payload() else {
        panic!("encoded payload")
    };
    let TransportPayload::Encoded(second) = calls[1].payload() else {
        panic!("encoded payload")
    };
    assert_eq!(first.bytes(), second.bytes());
    assert_eq!(first.bytes(), b"same encoded bytes");
}

/// Builds a request whose provider future remains pending until its token is
/// cancelled, recording the terminal admission classification in `observed`.
fn pending_request(
    token: RetryCancellationToken,
    observed: Arc<Mutex<Option<PublishEffect>>>,
) -> PublishRequest<String> {
    PublishRequest::builder()
        .topic(Topic::new("pending.uncertain").expect("pending_request: test topic name must be valid"))
        .payload("provider may already have admitted this".to_owned())
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .build()
                .expect("pending_request: retry policy must satisfy its attempt limit"),
        )
        .retry_cancellation_token(token)
        .error_handler(move |_, failure| {
            *observed
                .lock()
                .expect("pending_request: observed mutex must not be poisoned") = Some(failure.effect())
        })
        .build()
        .expect("pending_request: configured request must build successfully")
}
#[test]
fn test_pending_provider_token_cancellation_returns_unknown_with_original_identity() {
    let mut scripted = Scripted::new(true);
    scripted.pending = true;
    let spi = Arc::new(scripted);
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").expect("test_pending_provider_token_cancellation_returns_unknown_with_original_identity: test provider identity must be valid"), spi.clone()).expect("test_pending_provider_token_cancellation_returns_unknown_with_original_identity: facade must accept the scripted provider capabilities");
    let token = qubit_retry::RetryCancellationToken::new();
    let observed = Arc::new(Mutex::new(None));
    let request = pending_request(token.clone(), observed.clone());
    let id = request.envelope().id().clone();
    let mut future = Box::pin(bus.publish(request));
    assert!(support::manual_async::poll_once(future.as_mut()).is_pending());
    assert_eq!(spi.calls.lock().expect("test_pending_provider_token_cancellation_returns_unknown_with_original_identity: spi.calls mutex must not be poisoned").len(), 1);
    token.cancel();
    let failure = support::manual_async::block_on(future).unwrap_err();
    assert_eq!(failure.event_id(), &id);
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(*observed.lock().expect("test_pending_provider_token_cancellation_returns_unknown_with_original_identity: observed mutex must not be poisoned"), Some(PublishEffect::MayHaveBeenAccepted));
    assert_eq!(spi.calls.lock().expect("test_pending_provider_token_cancellation_returns_unknown_with_original_identity: spi.calls mutex must not be poisoned").len(), 1);
}
#[test]
fn test_token_cancelled_before_spi_retains_not_accepted_without_provider_attempt() {
    let mut scripted = Scripted::new(true);
    scripted.pending = true;
    let spi = Arc::new(scripted);
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").expect("test_token_cancelled_before_spi_retains_not_accepted_without_provider_attempt: test provider identity must be valid"), spi.clone()).expect("test_token_cancelled_before_spi_retains_not_accepted_without_provider_attempt: facade must accept the scripted provider capabilities");
    let token = qubit_retry::RetryCancellationToken::new();
    token.cancel();
    let observed = Arc::new(Mutex::new(None));
    let request = pending_request(token, observed.clone());
    let id = request.envelope().id().clone();
    let failure = support::manual_async::block_on(bus.publish(request)).unwrap_err();
    assert_eq!(failure.event_id(), &id);
    assert_eq!(failure.effect(), PublishEffect::NotAccepted);
    assert_eq!(*observed.lock().expect("test_token_cancelled_before_spi_retains_not_accepted_without_provider_attempt: observed mutex must not be poisoned"), Some(PublishEffect::NotAccepted));
    assert!(spi.calls.lock().expect("test_token_cancelled_before_spi_retains_not_accepted_without_provider_attempt: spi.calls mutex must not be poisoned").is_empty());
}
/// Verifies retry time budgets are soft accounting limits while a provider
/// attempt is pending, then cancels it and checks admission uncertainty.
fn assert_soft_budget_keeps_pending_until_token_cancellation(total_budget: bool) {
    let mut scripted = Scripted::new(true);
    scripted.pending = true;
    let spi = Arc::new(scripted);
    let clock = ManualMonotonicClock::new_shared();
    let bus = AsyncEventBus::with_timer(ProviderId::new("scripted").expect("assert_soft_budget_keeps_pending_until_token_cancellation: test provider identity must be valid"), spi.clone(), clock.new_timer()).expect("assert_soft_budget_keeps_pending_until_token_cancellation: facade must accept the scripted provider capabilities");
    let policy = RetryPolicy::builder().max_attempts(2);
    let policy = if total_budget {
        policy.total_time_budget(Duration::from_secs(1))
    } else {
        policy.operation_time_budget(Duration::from_secs(1))
    };
    let observed = Arc::new(Mutex::new(None));
    let callback = observed.clone();
    let token = RetryCancellationToken::new();
    let request = PublishRequest::builder()
        .topic(Topic::new("budget.uncertain").expect("assert_soft_budget_keeps_pending_until_token_cancellation: test topic name must be valid"))
        .payload("pending admission".to_owned())
        .retry_policy(policy.build().expect("assert_soft_budget_keeps_pending_until_token_cancellation: retry time budget must pass policy validation"))
        .retry_cancellation_token(token.clone())
        .retry_rule(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .error_handler(move |_, failure| *callback.lock().expect("assert_soft_budget_keeps_pending_until_token_cancellation: callback mutex must not be poisoned") = Some(failure.effect()))
        .build()
        .expect("assert_soft_budget_keeps_pending_until_token_cancellation: configured test request must pass builder validation");
    let id = request.envelope().id().clone();
    let mut future = Box::pin(bus.publish(request));
    assert!(support::manual_async::poll_once(future.as_mut()).is_pending());
    clock.advance(Duration::from_secs(1)).expect(
        "assert_soft_budget_keeps_pending_until_token_cancellation: manual clock advance must stay in its domain",
    );
    // RetryPolicy budgets are soft accounting limits. Hard interruption is
    // an independent AsyncRetry runtime control, absent from this facade.
    assert!(support::manual_async::poll_once(future.as_mut()).is_pending());
    assert_eq!(
        spi.calls
            .lock()
            .expect("assert_soft_budget_keeps_pending_until_token_cancellation: spi.calls mutex must not be poisoned")
            .len(),
        1
    );
    token.cancel();
    let failure = support::manual_async::block_on(future).unwrap_err();
    assert_eq!(failure.event_id(), &id);
    assert_eq!(
        failure.effect(),
        PublishEffect::MayHaveBeenAccepted,
        "total budget: {total_budget}"
    );
    assert_eq!(
        *observed
            .lock()
            .expect("assert_soft_budget_keeps_pending_until_token_cancellation: observed mutex must not be poisoned"),
        Some(PublishEffect::MayHaveBeenAccepted)
    );
    assert_eq!(
        spi.calls
            .lock()
            .expect("assert_soft_budget_keeps_pending_until_token_cancellation: spi.calls mutex must not be poisoned")
            .len(),
        1
    );
}
#[test]
fn test_pending_provider_soft_operation_budget_preserves_cancellation_uncertainty() {
    assert_soft_budget_keeps_pending_until_token_cancellation(false);
}
#[test]
fn test_pending_provider_soft_total_budget_preserves_cancellation_uncertainty() {
    assert_soft_budget_keeps_pending_until_token_cancellation(true);
}

#[test]
fn test_token_cancelled_during_second_pending_attempt_keeps_unknown_after_rejection() {
    let mut scripted = Scripted::new(false);
    scripted.reject_then_pending = true;
    let spi = Arc::new(scripted);
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").expect("test_token_cancelled_during_second_pending_attempt_keeps_unknown_after_rejection: test provider identity must be valid"), spi.clone()).expect("test_token_cancelled_during_second_pending_attempt_keeps_unknown_after_rejection: facade must accept the scripted provider capabilities");
    let token = qubit_retry::RetryCancellationToken::new();
    let observed = Arc::new(Mutex::new(None));
    let request = pending_request(token.clone(), observed.clone());
    let id = request.envelope().id().clone();
    let mut future = Box::pin(bus.publish(request));
    assert!(support::manual_async::poll_once(future.as_mut()).is_pending());
    assert_eq!(spi.calls.lock().expect("test_token_cancelled_during_second_pending_attempt_keeps_unknown_after_rejection: spi.calls mutex must not be poisoned").len(), 2);
    token.cancel();
    let failure = support::manual_async::block_on(future).unwrap_err();
    assert_eq!(failure.event_id(), &id);
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert_eq!(*observed.lock().expect("test_token_cancelled_during_second_pending_attempt_keeps_unknown_after_rejection: observed mutex must not be poisoned"), Some(PublishEffect::MayHaveBeenAccepted));
}
#[test]
fn test_successful_first_async_attempt_does_not_report_possible_duplicates() {
    let mut scripted = Scripted::new(true);
    scripted.immediate_ack = true;
    let spi = Arc::new(scripted);
    let bus = AsyncEventBus::from_spi(ProviderId::new("scripted").expect("test_successful_first_async_attempt_does_not_report_possible_duplicates: test provider identity must be valid"), spi.clone()).expect("test_successful_first_async_attempt_does_not_report_possible_duplicates: facade must accept the scripted provider capabilities");
    let receipt = support::manual_async::block_on(bus.publish(request(DuplicateRiskPolicy::Forbid))).expect("test_successful_first_async_attempt_does_not_report_possible_duplicates: publication should produce the expected receipt");
    assert!(!receipt.duplicate_possible());
    assert_eq!(spi.calls.lock().expect("test_successful_first_async_attempt_does_not_report_possible_duplicates: spi.calls mutex must not be poisoned").len(), 1);
}

/// A deterministic timer whose clock regresses only after provider
/// acknowledgement.
#[derive(Clone)]
struct AckRegressionTimer {
    manual: Arc<ManualMonotonicClock>,
    early: MonotonicInstant,
    acknowledged: Arc<AtomicBool>,
}
impl AckRegressionTimer {
    /// Starts the clock two seconds ahead so acknowledgement can reveal an
    /// earlier instant.
    fn new() -> Self {
        let manual = ManualMonotonicClock::new_shared();
        let early = manual.now();
        manual
            .advance(std::time::Duration::from_secs(2))
            .expect("new: manual clock advance must stay in its domain");
        Self {
            manual,
            early,
            acknowledged: Arc::new(AtomicBool::new(false)),
        }
    }
}
impl MonotonicClock for AckRegressionTimer {
    fn domain(&self) -> ClockDomain {
        self.early.domain()
    }
    fn now(&self) -> MonotonicInstant {
        if self.acknowledged.load(Ordering::Acquire) {
            self.early
        } else {
            MonotonicClock::now(self.manual.as_ref())
        }
    }
    fn new_timer(&self) -> Arc<dyn Timer> {
        Arc::new(self.clone())
    }
}
impl Timer for AckRegressionTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        self
    }
    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        MonotonicClock::new_timer(self.manual.as_ref()).at(deadline)
    }
}
/// Async provider that acknowledges and then exposes a regressing clock, with
/// options to reject the first attempt or acknowledge no destinations.
struct AckFailureSpi {
    clock: AckRegressionTimer,
    rejects_first: bool,
    no_destinations: bool,
    calls: AtomicUsize,
}
impl AsyncEventBusSpi for AckFailureSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        support::fake_spi::full_capabilities()
    }
    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            if first && self.rejects_first {
                return Err(SpiError::Publish {
                    provider_id: "ack-clock".into(),
                    resource: None,
                    kind: "rejected",
                    retryable: Some(true),
                    effect: PublishEffect::NotAccepted,
                    source: Box::new(IoError::other("known rejection")),
                });
            }
            self.clock.acknowledged.store(true, Ordering::Release);
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
/// Checks that an acknowledgement followed by a clock failure preserves the
/// correct admission effect across optional first-attempt rejection.
fn assert_ack_followed_by_clock_failure_effect(rejects_first: bool, no_destinations: bool) {
    let clock = AckRegressionTimer::new();
    let spi = Arc::new(AckFailureSpi {
        clock: clock.clone(),
        rejects_first,
        no_destinations,
        calls: AtomicUsize::new(0),
    });
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("ack-clock")
            .expect("assert_ack_followed_by_clock_failure_effect: test provider identity must be valid"),
        spi.clone(),
        Arc::new(clock),
    )
    .expect("assert_ack_followed_by_clock_failure_effect: facade must accept the scripted provider capabilities");
    let observed = Arc::new(Mutex::new(None));
    let callback = observed.clone();
    let request = PublishRequest::builder()
        .topic(
            Topic::new("acked.clock.failure")
                .expect("assert_ack_followed_by_clock_failure_effect: test topic name must be valid"),
        )
        .payload("accepted before clock failure".to_owned())
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .build()
                .expect("assert_ack_followed_by_clock_failure_effect: retry policy must satisfy its attempt limit"),
        )
        .error_handler(move |context, failure| {
            *callback
                .lock()
                .expect("assert_ack_followed_by_clock_failure_effect: callback mutex must not be poisoned") =
                Some((context.event_id().clone(), failure.effect()))
        })
        .build()
        .expect("assert_ack_followed_by_clock_failure_effect: configured test request must pass builder validation");
    let id = request.envelope().id().clone();
    let failure = support::manual_async::block_on(bus.publish(request)).expect_err("clock failure after ACK");
    assert!(matches!(failure.cause(), PublishError::Retry(_)));
    assert_eq!(failure.event_id(), &id);
    let expected_effect = if no_destinations {
        PublishEffect::NotAccepted
    } else {
        PublishEffect::MayHaveBeenAccepted
    };
    assert_eq!(failure.effect(), expected_effect);
    assert_eq!(
        *observed
            .lock()
            .expect("assert_ack_followed_by_clock_failure_effect: observed mutex must not be poisoned"),
        Some((id, expected_effect))
    );
    assert_eq!(
        spi.calls.load(std::sync::atomic::Ordering::SeqCst),
        if rejects_first { 2 } else { 1 }
    );
}
#[test]
fn test_first_ack_followed_by_clock_failure_is_not_misclassified_as_rejection() {
    assert_ack_followed_by_clock_failure_effect(false, false);
}
#[test]
fn test_rejection_then_ack_followed_by_clock_failure_keeps_admission_evidence() {
    assert_ack_followed_by_clock_failure_effect(true, false);
}

#[test]
fn test_no_destination_ack_then_clock_failure_retains_known_non_admission() {
    assert_ack_followed_by_clock_failure_effect(false, true);
}

/// Counts real codec encoding calls across the complete publication.
struct CountingTextCodec {
    content_type: qubit_event_bus::model::ContentType,
    encodes: Arc<std::sync::atomic::AtomicUsize>,
}
impl qubit_event_bus::codec::EventCodec<String> for CountingTextCodec {
    fn content_type(&self) -> &qubit_event_bus::model::ContentType {
        &self.content_type
    }
    fn schema_id(&self) -> Option<&qubit_event_bus::model::SchemaId> {
        None
    }
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, qubit_event_bus::error::CodecError> {
        self.encodes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Arc::from(value.as_bytes()))
    }
    fn decode(
        &self,
        payload: &qubit_event_bus::spi::EncodedPayload,
    ) -> Result<String, qubit_event_bus::error::CodecError> {
        Ok(String::from_utf8_lossy(payload.bytes()).into_owned())
    }
}

/// Publishes through either facade after an unknown attempt and verifies that
/// terminal admission details cannot erase earlier uncertainty.
fn assert_unknown_then_admissions(asynchronous: bool, statuses: Vec<qubit_event_bus::model::AdmissionStatus>) {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use qubit_event_bus::model::DestinationAdmission;
    use qubit_event_bus::model::SubscriberId;
    let acknowledgement = PublishAcknowledgement::DestinationAdmissions(
        statuses
            .into_iter()
            .enumerate()
            .map(|(index, status)| {
                DestinationAdmission::new(
                    qubit_id::Id::new(index as u64 + 1),
                    SubscriberId::new(format!("destination-{index}")).expect("subscriber identity"),
                    status,
                )
            })
            .collect(),
    );
    let mut scripted = Scripted::new(true);
    scripted.encoded = true;
    scripted.terminal_ack = Some(acknowledgement.clone());
    let spi = Arc::new(scripted);
    let encodes = Arc::new(AtomicUsize::new(0));
    let typed_calls = Arc::new(AtomicUsize::new(0));
    let global_calls = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let typed = typed_calls.clone();
    let global = global_calls.clone();
    let error_count = errors.clone();
    let config = qubit_event_bus::EventBusFacadeConfig::new().publisher_interceptor(move |_| {
        global.fetch_add(1, Ordering::SeqCst);
        Ok(true)
    });
    let topic = Topic::new("unknown.admissions")
        .expect("encoded topic")
        .with_codec(CountingTextCodec {
            content_type: qubit_event_bus::model::ContentType::new("text/plain").expect("content type"),
            encodes: encodes.clone(),
        });
    let request = PublishRequest::builder()
        .topic(topic)
        .payload("stable encoded admission".to_owned())
        .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().expect("retry policy"))
        .retry_rule(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .interceptor(move |envelope| {
            typed.fetch_add(1, Ordering::SeqCst);
            Ok(Some(envelope))
        })
        .error_handler(move |_, _| {
            error_count.fetch_add(1, Ordering::SeqCst);
        })
        .build()
        .expect("publish request");
    let id = request.envelope().id().clone();
    let receipt = if asynchronous {
        let bus = AsyncEventBus::with_config(ProviderId::new("scripted").expect("provider"), spi.clone(), config)
            .expect("async facade");
        support::manual_async::block_on(bus.publish(request)).expect("terminal acknowledgement")
    } else {
        let bus = EventBus::with_config(ProviderId::new("scripted").expect("provider"), spi.clone(), config)
            .expect("sync facade");
        bus.publish(request).expect("terminal acknowledgement")
    };
    assert!(receipt.duplicate_possible());
    assert_eq!(receipt.input_event_id(), &id);
    assert_eq!(receipt.dispatched_event_id(), Some(&id));
    assert_eq!(receipt.acknowledgement(), &acknowledgement);
    assert_eq!(encodes.load(Ordering::SeqCst), 1);
    assert_eq!(typed_calls.load(Ordering::SeqCst), 1);
    assert_eq!(global_calls.load(Ordering::SeqCst), 1);
    assert_eq!(errors.load(Ordering::SeqCst), 0);
    let calls = spi.calls.lock().expect("provider attempts");
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].id(), &id);
    assert_eq!(calls[1].id(), &id);
    assert_eq!(calls[0].timestamp(), calls[1].timestamp());
    assert_eq!(calls[0].headers(), calls[1].headers());
    let qubit_event_bus::spi::TransportPayload::Encoded(first) = calls[0].payload() else {
        panic!("encoded first attempt")
    };
    let qubit_event_bus::spi::TransportPayload::Encoded(second) = calls[1].payload() else {
        panic!("encoded second attempt")
    };
    assert_eq!(first.bytes(), second.bytes());
}

#[test]
fn test_sync_unknown_then_no_destinations_keeps_duplicates() {
    assert_unknown_then_admissions(false, Vec::new());
}
#[test]
fn test_async_unknown_then_no_destinations_keeps_duplicates() {
    assert_unknown_then_admissions(true, Vec::new());
}
#[test]
fn test_sync_unknown_then_all_rejected_keeps_duplicates() {
    assert_unknown_then_admissions(
        false,
        vec![qubit_event_bus::model::AdmissionStatus::Rejected("full".into())],
    );
}
#[test]
fn test_async_unknown_then_all_rejected_keeps_duplicates() {
    assert_unknown_then_admissions(
        true,
        vec![qubit_event_bus::model::AdmissionStatus::Rejected("full".into())],
    );
}
#[test]
fn test_sync_unknown_then_partial_keeps_duplicates() {
    assert_unknown_then_admissions(
        false,
        vec![
            qubit_event_bus::model::AdmissionStatus::Accepted,
            qubit_event_bus::model::AdmissionStatus::Rejected("full".into()),
        ],
    );
}
#[test]
fn test_async_unknown_then_partial_keeps_duplicates() {
    assert_unknown_then_admissions(
        true,
        vec![
            qubit_event_bus::model::AdmissionStatus::Accepted,
            qubit_event_bus::model::AdmissionStatus::Rejected("full".into()),
        ],
    );
}

/// Traverses the public error source chain to verify that outcome aggregation
/// returns the original provider source for either facade.
fn assert_terminal_source_chain(asynchronous: bool) {
    use std::error::Error;
    let spi = Arc::new(Scripted::new(false));
    let request = request(DuplicateRiskPolicy::AllowDuplicates);
    let id = request.envelope().id().clone();
    let failure = if asynchronous {
        let bus =
            AsyncEventBus::from_spi(ProviderId::new("scripted").expect("provider"), spi.clone()).expect("async facade");
        support::manual_async::block_on(bus.publish(request)).expect_err("retry exhausted")
    } else {
        let bus = EventBus::from_spi(ProviderId::new("scripted").expect("provider"), spi.clone()).expect("sync facade");
        bus.publish(request).expect_err("retry exhausted")
    };
    assert_eq!(failure.event_id(), &id);
    assert_eq!(failure.effect(), PublishEffect::MayHaveBeenAccepted);
    assert!(matches!(
        failure.cause(),
        qubit_event_bus::error::PublishError::Retry(_)
    ));
    let mut source: &(dyn Error + 'static) = &failure;
    let mut original = None;
    while let Some(next) = source.source() {
        if let Some(provider_source) = next.downcast_ref::<std::io::Error>() {
            original = Some(provider_source.to_string());
        }
        source = next;
    }
    assert_eq!(original.as_deref(), Some("original provider source"));
    assert_eq!(spi.calls.lock().expect("provider attempts").len(), 2);
}

#[test]
fn test_sync_terminal_failure_preserves_original_provider_source() {
    assert_terminal_source_chain(false);
}
#[test]
fn test_async_terminal_failure_preserves_original_provider_source() {
    assert_terminal_source_chain(true);
}

#[test]
fn test_unpolled_async_publish_defers_interceptors_and_codec() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    let mut scripted = Scripted::new(true);
    scripted.encoded = true;
    let spi = Arc::new(scripted);
    let encodes = Arc::new(AtomicUsize::new(0));
    let typed_calls = Arc::new(AtomicUsize::new(0));
    let global_calls = Arc::new(AtomicUsize::new(0));
    let typed = typed_calls.clone();
    let global = global_calls.clone();
    let bus = AsyncEventBus::with_config(
        ProviderId::new("scripted").expect("provider"),
        spi.clone(),
        qubit_event_bus::EventBusFacadeConfig::new().publisher_interceptor(move |_| {
            global.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }),
    )
    .expect("async facade");
    let topic = Topic::new("lazy.prepare")
        .expect("encoded topic")
        .with_codec(CountingTextCodec {
            content_type: qubit_event_bus::model::ContentType::new("text/plain").expect("content type"),
            encodes: encodes.clone(),
        });
    let request = PublishRequest::builder()
        .topic(topic)
        .payload("unpolled".to_owned())
        .interceptor(move |envelope| {
            typed.fetch_add(1, Ordering::SeqCst);
            Ok(Some(envelope))
        })
        .build()
        .expect("request");
    drop(bus.publish(request));
    assert_eq!(typed_calls.load(Ordering::SeqCst), 0);
    assert_eq!(global_calls.load(Ordering::SeqCst), 0);
    assert_eq!(encodes.load(Ordering::SeqCst), 0);
    assert!(spi.calls.lock().expect("provider attempts").is_empty());
}
