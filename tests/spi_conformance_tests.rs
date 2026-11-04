// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Backend-neutral SPI conformance checks shared by provider implementations.

mod support;

#[cfg(feature = "conformance")]
use std::panic::catch_unwind;
use std::sync::Arc;
#[cfg(feature = "conformance")]
use std::sync::Mutex;
use std::sync::mpsc::channel;
#[cfg(feature = "conformance")]
use std::task::Context;
use std::task::Poll;
#[cfg(feature = "conformance")]
use std::task::Waker;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::SpiError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
#[cfg(feature = "conformance")]
use qubit_event_bus::local::AsyncLocalEventBusProvider;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::ContentType;
#[cfg(feature = "conformance")]
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::SubscribeRequest;
#[cfg(feature = "conformance")]
use qubit_event_bus::model::SubscriberId;
#[cfg(feature = "conformance")]
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EncodedPayload;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::PublishGuarantee;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReceiveOutcome;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::AsyncConformanceCheck;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::AsyncConformanceHooks;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::ConformanceCase;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::ConformanceCaseContext;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::ConformanceHooks;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::ConformanceProfile;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::ConformanceSkipReason;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::run_async;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::run_async_with_profile;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::run_sync;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::run_sync_with_profile;
use qubit_id::Id;
use qubit_spi::ServiceProvider;
#[cfg(feature = "conformance")]
use qubit_spi::AsyncServiceProvider;

#[cfg(feature = "conformance")]
use crate::support::fake_spi::ConformanceTamper;
use crate::support::fake_spi::FakeAsyncEventBusSpi;
use crate::support::fake_spi::FakeEventBusSpi;
use crate::support::fake_spi::full_capabilities;
use crate::support::fake_spi::inbound_message;
use crate::support::fake_spi::native_no_settlement_capabilities;
use crate::support::fake_spi::outbound_message;
use crate::support::fake_spi::subscription_request;
use crate::support::flume_spi::create as create_flume_spi;
use crate::support::manual_async::block_on;
use crate::support::manual_async::poll_once;
use crate::support::provider_shapes::ChannelShapedEventBusSpi;
use crate::support::provider_shapes::encoded_settlement_capabilities;

#[cfg(feature = "conformance")]
#[derive(Clone, Copy)]
enum ReceiveStateFault {
    EmptyClosed,
    AfterCloseTimedOut,
    PendingMessageAfterClose,
    CloseFails,
}

#[cfg(feature = "conformance")]
struct FaultySyncProvider {
    inner: FakeEventBusSpi,
    fault: ReceiveStateFault,
    published: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(feature = "conformance")]
impl EventBusSpi for FaultySyncProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.published.store(true, std::sync::atomic::Ordering::Release);
        self.inner.publish(message)
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(FaultySyncReceiver {
            inner: self.inner.subscribe(request)?,
            fault: self.fault,
            published: self.published.clone(),
            closed: false,
            received: false,
        }))
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}

#[cfg(feature = "conformance")]
struct FaultySyncReceiver {
    inner: Box<dyn EventSubscriptionSpi>,
    fault: ReceiveStateFault,
    published: Arc<std::sync::atomic::AtomicBool>,
    closed: bool,
    received: bool,
}

#[cfg(feature = "conformance")]
impl EventSubscriptionSpi for FaultySyncReceiver {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        if matches!(self.fault, ReceiveStateFault::PendingMessageAfterClose) && self.closed && !self.received {
            self.received = true;
            return Ok(ReceiveOutcome::Message(inbound_message(None)));
        }
        if matches!(self.fault, ReceiveStateFault::EmptyClosed)
            && timeout.is_zero()
            && !self.published.load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(ReceiveOutcome::Closed);
        }
        if matches!(self.fault, ReceiveStateFault::AfterCloseTimedOut) && self.closed {
            return Ok(ReceiveOutcome::TimedOut);
        }
        let result = self.inner.receive(timeout);
        if matches!(&result, Ok(ReceiveOutcome::Message(_))) {
            self.received = true;
        }
        result
    }
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        self.inner.settle(token, disposition)
    }
    fn close(&mut self) -> Result<(), SpiError> {
        if matches!(self.fault, ReceiveStateFault::CloseFails) {
            return Err(SpiError::Operation {
                provider_id: "faulty-conformance".into(),
                operation: "close",
                resource: None,
                kind: "injected_close_failure",
                retryable: Some(false),
                source: Box::new(std::io::Error::other("injected close failure")),
            });
        }
        self.closed = true;
        self.inner.close()
    }
}

#[cfg(feature = "conformance")]
struct FaultyAsyncProvider {
    inner: FakeAsyncEventBusSpi,
    fault: ReceiveStateFault,
    published: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(feature = "conformance")]
impl AsyncEventBusSpi for FaultyAsyncProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.published.store(true, std::sync::atomic::Ordering::Release);
        self.inner.publish(message)
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            let inner = self.inner.subscribe(request).await?;
            Ok(Box::new(FaultyAsyncReceiver {
                inner,
                fault: self.fault,
                published: self.published.clone(),
                closed: false,
                received: false,
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

#[cfg(feature = "conformance")]
struct FaultyAsyncReceiver {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    fault: ReceiveStateFault,
    published: Arc<std::sync::atomic::AtomicBool>,
    closed: bool,
    received: bool,
}

#[cfg(feature = "conformance")]
impl AsyncEventSubscriptionSpi for FaultyAsyncReceiver {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        if matches!(self.fault, ReceiveStateFault::PendingMessageAfterClose) && self.closed && !self.received {
            self.received = true;
            return Box::pin(async { Ok(ReceiveOutcome::Message(inbound_message(None))) });
        }
        if matches!(self.fault, ReceiveStateFault::EmptyClosed)
            && timeout.is_zero()
            && !self.published.load(std::sync::atomic::Ordering::Acquire)
        {
            return Box::pin(async { Ok(ReceiveOutcome::Closed) });
        }
        if matches!(self.fault, ReceiveStateFault::AfterCloseTimedOut) && self.closed {
            return Box::pin(async { Ok(ReceiveOutcome::TimedOut) });
        }
        self.received = true;
        self.inner.receive(timeout)
    }
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        self.inner.settle(token, disposition)
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        if matches!(self.fault, ReceiveStateFault::CloseFails) {
            return Box::pin(async {
                Err(SpiError::Operation {
                    provider_id: "faulty-conformance".into(),
                    operation: "close",
                    resource: None,
                    kind: "injected_close_failure",
                    retryable: Some(false),
                    source: Box::new(std::io::Error::other("injected close failure")),
                })
            });
        }
        self.closed = true;
        self.inner.close()
    }
}

/// Accepts an overall publication acceptance or one matching accepted
/// admission.
fn accepted_publication(acknowledgement: PublishAcknowledgement, subscription_id: Id) -> Result<(), String> {
    match acknowledgement {
        PublishAcknowledgement::Accepted { .. } => Ok(()),
        PublishAcknowledgement::DestinationAdmissions(admissions) => match admissions.as_slice() {
            [admission]
                if admission.subscription_id() == subscription_id
                    && matches!(admission.status(), AdmissionStatus::Accepted) =>
            {
                Ok(())
            }
            _ => Err(format!(
                "expected an accepted admission for subscription {subscription_id}, got {admissions:?}"
            )),
        },
        PublishAcknowledgement::DroppedByInterceptor => {
            Err("conformance publication was dropped by an interceptor".into())
        }
        _ => Err("provider returned an unsupported publication acknowledgement".into()),
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_conformance_checks_receive_state_for_sync_and_async_providers() {
    for fault in [
        ReceiveStateFault::EmptyClosed,
        ReceiveStateFault::AfterCloseTimedOut,
        ReceiveStateFault::PendingMessageAfterClose,
    ] {
        for profile in [ConformanceProfile::Structural, ConformanceProfile::Strict] {
            let sync = run_sync_with_profile(
                || {
                    Arc::new(FaultySyncProvider {
                        inner: FakeEventBusSpi::new(),
                        fault,
                        published: Arc::default(),
                    })
                },
                &ConformanceHooks::default(),
                profile,
            );
            let asynchronous = block_on(run_async_with_profile(
                || async {
                    Arc::new(FaultyAsyncProvider {
                        inner: FakeAsyncEventBusSpi::new(),
                        fault,
                        published: Arc::default(),
                    }) as Arc<dyn AsyncEventBusSpi>
                },
                &AsyncConformanceHooks::default(),
                profile,
            ));
            let case_id = match fault {
                ReceiveStateFault::EmptyClosed => "receive-empty-zero-timeout",
                ReceiveStateFault::AfterCloseTimedOut => "receive-after-close",
                ReceiveStateFault::PendingMessageAfterClose => "receive-after-close-with-pending",
                ReceiveStateFault::CloseFails => unreachable!(),
            };
            for report in [&sync, &asynchronous] {
                let failed_index = report
                    .cases()
                    .iter()
                    .position(|case| {
                        matches!(
                            case,
                            ConformanceCase::Failed { case_id: actual, detail }
                                if actual == case_id && (detail.contains("sync ") || detail.contains("async "))
                        )
                    })
                    .unwrap_or_else(|| panic!("fault {case_id} must be reported with mode context: {report:?}"));
                if matches!(fault, ReceiveStateFault::PendingMessageAfterClose) {
                    assert!(
                        report.case_context(failed_index).is_some(),
                        "pending-message failure must retain its combination: {report:?}"
                    );
                }
            }
        }
    }

    for profile in [ConformanceProfile::Structural, ConformanceProfile::Strict] {
        let sync = run_sync_with_profile(
            || {
                Arc::new(FaultySyncProvider {
                    inner: FakeEventBusSpi::new(),
                    fault: ReceiveStateFault::CloseFails,
                    published: Arc::default(),
                })
            },
            &ConformanceHooks::default(),
            profile,
        );
        let asynchronous = block_on(run_async_with_profile(
            || async {
                Arc::new(FaultyAsyncProvider {
                    inner: FakeAsyncEventBusSpi::new(),
                    fault: ReceiveStateFault::CloseFails,
                    published: Arc::default(),
                }) as Arc<dyn AsyncEventBusSpi>
            },
            &AsyncConformanceHooks::default(),
            profile,
        ));
        for report in [&sync, &asynchronous] {
            if profile == ConformanceProfile::Structural {
                assert!(
                    report.cases().iter().any(|case| matches!(
                        case,
                        ConformanceCase::Skipped {
                            case_id,
                            reason: ConformanceSkipReason::MissingFixture { detail },
                        } if case_id == "receive-after-close" && detail == "receiver close failed"
                    )),
                    "failed close must skip post-close receive: {report:?}"
                );
            } else {
                assert!(
                    report.cases().iter().any(|case| matches!(
                        case,
                        ConformanceCase::Failed { case_id, detail }
                            if case_id == "receive-after-close" && detail.contains("strict profile requires")
                    )),
                    "strict profile must promote the missing close fixture: {report:?}"
                );
            }
        }
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_sync_and_async_runners_check_foreign_settlement_tokens() {
    for profile in [ConformanceProfile::Structural, ConformanceProfile::Strict] {
        let sync = run_sync_with_profile(
            || LocalEventBusProvider.create_configured(&EventBusConfig::default()).expect("local provider"),
            &ConformanceHooks::default(),
            profile,
        );
        let asynchronous = block_on(run_async_with_profile(
            || async { AsyncLocalEventBusProvider.create_configured(&EventBusConfig::default()).await.expect("async local provider") },
            &AsyncConformanceHooks::default(),
            profile,
        ));
        for report in [&sync, &asynchronous] {
            for expected in ["settlement-foreign-token", "settlement-owner-after-foreign-token"] {
                let index = report.cases().iter().position(|case| matches!(case,
                    ConformanceCase::Passed { case_id } if case_id == expected
                )).unwrap_or_else(|| panic!("missing successful {expected}: {report:?}"));
                let context = report.case_context(index).expect("probe case retains payload/mode context");
                assert!(matches!(context.durability(), SubscriptionDurability::Ephemeral | SubscriptionDurability::Durable));
            }
        }
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_subscription_mode_runner_exercises_both_declared_modes_for_sync_and_async() {
    let capabilities = EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::None,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Durable,
        SubscriptionModes::BOTH,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    );
    let sync_seen = Arc::new(Mutex::new(Vec::new()));
    let async_seen = Arc::new(Mutex::new(Vec::new()));
    let sync = run_sync(
        || {
            Arc::new(SubscriptionModeSyncAdapter {
                inner: FakeEventBusSpi::with_capabilities(capabilities),
                seen: sync_seen.clone(),
                reject: None,
                acknowledgement: None,
            })
        },
        &ConformanceHooks::default(),
    );
    let asynchronous = block_on(run_async(
        || async {
            Arc::new(SubscriptionModeAsyncAdapter {
                inner: FakeAsyncEventBusSpi::with_capabilities(capabilities),
                seen: async_seen.clone(),
                reject: None,
                acknowledgement: None,
            }) as Arc<dyn AsyncEventBusSpi>
        },
        &AsyncConformanceHooks::default(),
    ));
    for report in [&sync, &asynchronous] {
        assert_eq!(
            report
                .cases()
                .iter()
                .filter(|case| matches!(case,
                    ConformanceCase::Passed { case_id } if case_id == "subscribe"
                ))
                .count(),
            2,
            "BOTH must cause one subscribe probe for each declared mode"
        );
    }
    use qubit_event_bus::model::SubscriptionDurability;
    assert_eq!(
        *sync_seen.lock().unwrap(),
        vec![
            SubscriptionDurability::Ephemeral,
            SubscriptionDurability::Ephemeral,
            SubscriptionDurability::Durable,
            SubscriptionDurability::Durable,
        ]
    );
    assert_eq!(
        *async_seen.lock().unwrap(),
        vec![
            SubscriptionDurability::Ephemeral,
            SubscriptionDurability::Ephemeral,
            SubscriptionDurability::Durable,
            SubscriptionDurability::Durable,
        ]
    );
}

#[cfg(feature = "conformance")]
#[test]
fn test_conformance_report_context_tracks_each_payload_and_durability_combination() {
    let capabilities = EventBusCapabilities::new(
        PayloadModes::NativeAndEncoded,
        SettlementCapabilities::None,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Durable,
        SubscriptionModes::BOTH,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    );
    let sync = run_sync(
        || Arc::new(FakeEventBusSpi::with_capabilities(capabilities)),
        &ConformanceHooks::default(),
    );
    let asynchronous = block_on(run_async(
        || async { Arc::new(FakeAsyncEventBusSpi::with_capabilities(capabilities)) as Arc<dyn AsyncEventBusSpi> },
        &AsyncConformanceHooks::default(),
    ));

    for (report, api) in [(&sync, "sync"), (&asynchronous, "async")] {
        let subscriptions = report
            .cases()
            .iter()
            .enumerate()
            .filter(|(_, case)| matches!(case, ConformanceCase::Passed { case_id } if case_id == "subscribe"))
            .map(|(index, _)| report.case_context(index).expect("combination has context"))
            .collect::<Vec<&ConformanceCaseContext>>();
        assert_eq!(subscriptions.len(), 4);
        for payload_mode in [PayloadModes::Native, PayloadModes::Encoded] {
            for durability in [SubscriptionDurability::Ephemeral, SubscriptionDurability::Durable] {
                assert!(subscriptions.iter().any(|context| {
                    context.api() == api && context.payload_mode() == payload_mode && context.durability() == durability
                }));
            }
        }
        assert!(report.cases().iter().enumerate().any(|(index, case)| {
            matches!(case, ConformanceCase::Skipped { case_id, .. } if case_id == "settlement-idempotence")
                && report.case_context(index).is_some()
        }));
        assert!(report.case_context(report.cases().len()).is_none());
        let global_case = report
            .cases()
            .iter()
            .position(|case| matches!(case, ConformanceCase::Skipped { case_id, .. } if case_id == "provider-settlement-idempotence"))
            .expect("global settlement hook result exists");
        assert!(report.case_context(global_case).is_none());
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_subscription_mode_runner_records_declared_mode_rejections() {
    let capabilities = durable_test_capabilities();
    use qubit_event_bus::model::SubscriptionDurability;
    let sync = run_sync(
        || {
            Arc::new(SubscriptionModeSyncAdapter {
                inner: FakeEventBusSpi::with_capabilities(capabilities),
                seen: Arc::default(),
                reject: Some(SubscriptionDurability::Ephemeral),
                acknowledgement: None,
            })
        },
        &ConformanceHooks::default(),
    );
    let asynchronous = block_on(run_async(
        || async {
            Arc::new(SubscriptionModeAsyncAdapter {
                inner: FakeAsyncEventBusSpi::with_capabilities(capabilities),
                seen: Arc::default(),
                reject: Some(SubscriptionDurability::Ephemeral),
                acknowledgement: None,
            }) as Arc<dyn AsyncEventBusSpi>
        },
        &AsyncConformanceHooks::default(),
    ));
    for report in [&sync, &asynchronous] {
        assert!(report.cases().iter().any(|case| matches!(case,
            ConformanceCase::Failed { case_id, detail }
                if case_id == "subscribe" && detail.contains("ephemeral mode")
        )));
        assert!(
            report.cases().iter().any(|case| matches!(case,
                ConformanceCase::Passed { case_id } if case_id == "subscribe"
            )),
            "durable subscription should still be attempted after ephemeral rejection"
        );
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_subscription_mode_probe_is_independent_of_retention_capability() {
    use qubit_event_bus::model::SubscriptionDurability;
    let capabilities = EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::None,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        SubscriptionModes::DURABLE,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let report = run_sync(
        || {
            Arc::new(SubscriptionModeSyncAdapter {
                inner: FakeEventBusSpi::with_capabilities(capabilities),
                seen: seen.clone(),
                reject: None,
                acknowledgement: None,
            })
        },
        &ConformanceHooks::default(),
    );
    assert!(report.all_passed(), "{report:?}");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![SubscriptionDurability::Durable, SubscriptionDurability::Durable]
    );
    let async_seen = Arc::new(Mutex::new(Vec::new()));
    let async_report = block_on(run_async(
        || async {
            Arc::new(SubscriptionModeAsyncAdapter {
                inner: FakeAsyncEventBusSpi::with_capabilities(capabilities),
                seen: async_seen.clone(),
                reject: None,
                acknowledgement: None,
            }) as Arc<dyn AsyncEventBusSpi>
        },
        &AsyncConformanceHooks::default(),
    ));
    assert!(async_report.all_passed(), "{async_report:?}");
    assert_eq!(
        *async_seen.lock().unwrap(),
        vec![SubscriptionDurability::Durable, SubscriptionDurability::Durable]
    );
}

#[cfg(feature = "conformance")]
struct SubscriptionModeSyncAdapter {
    inner: FakeEventBusSpi,
    seen: Arc<Mutex<Vec<qubit_event_bus::model::SubscriptionDurability>>>,
    reject: Option<qubit_event_bus::model::SubscriptionDurability>,
    acknowledgement: Option<PublishAcknowledgement>,
}

#[cfg(feature = "conformance")]
impl EventBusSpi for SubscriptionModeSyncAdapter {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let acknowledgement = self.inner.publish(message)?;
        Ok(self.acknowledgement.clone().unwrap_or(acknowledgement))
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        let mode = request.durability();
        self.seen.lock().unwrap().push(mode);
        if self.reject == Some(mode) {
            return Err(SpiError::Operation {
                provider_id: "subscription-mode-test".into(),
                operation: "subscribe",
                resource: None,
                kind: "unsupported",
                retryable: Some(false),
                source: Box::new(std::io::Error::other("test adapter rejected declared mode")),
            });
        }
        self.inner.subscribe(request)
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}

#[cfg(feature = "conformance")]
struct SubscriptionModeAsyncAdapter {
    inner: FakeAsyncEventBusSpi,
    seen: Arc<Mutex<Vec<qubit_event_bus::model::SubscriptionDurability>>>,
    reject: Option<qubit_event_bus::model::SubscriptionDurability>,
    acknowledgement: Option<PublishAcknowledgement>,
}

#[cfg(feature = "conformance")]
impl AsyncEventBusSpi for SubscriptionModeAsyncAdapter {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        let acknowledgement = self.acknowledgement.clone();
        let publish = self.inner.publish(message);
        Box::pin(async move {
            let actual = publish.await?;
            Ok(acknowledgement.unwrap_or(actual))
        })
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn qubit_event_bus::spi::AsyncEventSubscriptionSpi>, SpiError>> {
        let mode = request.durability();
        self.seen.lock().unwrap().push(mode);
        if self.reject == Some(mode) {
            return Box::pin(async {
                Err(SpiError::Operation {
                    provider_id: "subscription-mode-test".into(),
                    operation: "subscribe",
                    resource: None,
                    kind: "unsupported",
                    retryable: Some(false),
                    source: Box::new(std::io::Error::other("test adapter rejected declared mode")),
                })
            });
        }
        self.inner.subscribe(request)
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_publication_acknowledgement_visibility_mismatch_keeps_receive_and_cleanup_path() {
    let capabilities = EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::None,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        SubscriptionModes::EPHEMERAL,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::DestinationAdmissions,
    );
    let sync = run_sync(
        || Arc::new(FakeEventBusSpi::with_capabilities(capabilities)),
        &ConformanceHooks::default(),
    );
    let asynchronous = block_on(run_async(
        || async { Arc::new(FakeAsyncEventBusSpi::with_capabilities(capabilities)) as Arc<dyn AsyncEventBusSpi> },
        &AsyncConformanceHooks::default(),
    ));
    for report in [&sync, &asynchronous] {
        assert!(report.cases().iter().any(|case| matches!(case,
            ConformanceCase::Failed { case_id, detail }
                if case_id == "declared-native-publish" && detail.contains("opaque acceptance")
        )));
        assert!(
            report.cases().iter().any(|case| matches!(case,
                ConformanceCase::Passed { case_id } if case_id == "receive-payload"
            )),
            "an invalid acknowledgement must not bypass receive"
        );
        for case_id in ["close", "shutdown"] {
            assert!(
                report.cases().iter().any(|case| matches!(case,
                    ConformanceCase::Passed { case_id: actual } if actual == case_id
                )),
                "{case_id} must still run after acknowledgement failure"
            );
        }
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_publication_acknowledgement_matrix_for_sync_and_async_runners() {
    let opaque = || PublishAcknowledgement::Accepted {
        provider_message_id: None,
        metadata: Default::default(),
    };
    let admission = |subscription_id, status| {
        DestinationAdmission::new(Id::new(subscription_id), SubscriberId::new("probe").unwrap(), status)
    };
    let cases = vec![
        (
            "opaque acknowledgement with visible declaration",
            opaque(),
            PublishVisibility::DestinationAdmissions,
            false,
        ),
        (
            "interceptor drop",
            PublishAcknowledgement::DroppedByInterceptor,
            PublishVisibility::Opaque,
            false,
        ),
        (
            "empty admissions",
            PublishAcknowledgement::DestinationAdmissions(vec![]),
            PublishVisibility::DestinationAdmissions,
            false,
        ),
        (
            "filtered only",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(1, AdmissionStatus::Filtered)]),
            PublishVisibility::DestinationAdmissions,
            false,
        ),
        (
            "rejected only",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(1, AdmissionStatus::Rejected("full".into()))]),
            PublishVisibility::DestinationAdmissions,
            false,
        ),
        (
            "other subscription accepted",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(2, AdmissionStatus::Accepted)]),
            PublishVisibility::DestinationAdmissions,
            false,
        ),
        ("opaque accepted", opaque(), PublishVisibility::Opaque, true),
        (
            "target subscription accepted",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(1, AdmissionStatus::Accepted)]),
            PublishVisibility::DestinationAdmissions,
            true,
        ),
        (
            "opaque declaration with detailed target acceptance",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(1, AdmissionStatus::Accepted)]),
            PublishVisibility::Opaque,
            true,
        ),
    ];
    for (label, acknowledgement, visibility, expected_pass) in cases {
        let capabilities = EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            visibility,
        );
        let sync = run_sync(
            || {
                Arc::new(SubscriptionModeSyncAdapter {
                    inner: FakeEventBusSpi::with_capabilities(capabilities),
                    seen: Arc::default(),
                    reject: None,
                    acknowledgement: Some(acknowledgement.clone()),
                })
            },
            &ConformanceHooks::default(),
        );
        let asynchronous = block_on(run_async(
            || async {
                Arc::new(SubscriptionModeAsyncAdapter {
                    inner: FakeAsyncEventBusSpi::with_capabilities(capabilities),
                    seen: Arc::default(),
                    reject: None,
                    acknowledgement: Some(acknowledgement.clone()),
                }) as Arc<dyn AsyncEventBusSpi>
            },
            &AsyncConformanceHooks::default(),
        ));
        for report in [&sync, &asynchronous] {
            assert_eq!(
                report.cases().iter().find_map(|case| match case {
                    ConformanceCase::Passed { case_id } if case_id == "declared-native-publish" => Some(true),
                    ConformanceCase::Failed { case_id, .. } if case_id == "declared-native-publish" => Some(false),
                    _ => None,
                }),
                Some(expected_pass),
                "unexpected acknowledgement outcome for {label}: {report:?}"
            );
            assert!(
                report.cases().iter().any(|case| matches!(case,
                    ConformanceCase::Passed { case_id } if case_id == "receive-payload"
                )),
                "{label}: successful publish must retain the receive path"
            );
            for case_id in ["close", "shutdown"] {
                assert!(
                    report.cases().iter().any(|case| matches!(case,
                        ConformanceCase::Passed { case_id: actual } if actual == case_id
                    )),
                    "{label}: {case_id} cleanup must run"
                );
            }
        }
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_pending_close_acknowledgement_matrix_for_sync_and_async_runners() {
    let opaque = || PublishAcknowledgement::Accepted {
        provider_message_id: None,
        metadata: Default::default(),
    };
    let admission = |subscription_id, status| {
        DestinationAdmission::new(Id::new(subscription_id), SubscriberId::new("probe").unwrap(), status)
    };
    let cases = vec![
        (
            "empty admissions",
            PublishAcknowledgement::DestinationAdmissions(vec![]),
            PublishVisibility::DestinationAdmissions,
            false,
        ),
        (
            "filtered",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(1, AdmissionStatus::Filtered)]),
            PublishVisibility::DestinationAdmissions,
            false,
        ),
        (
            "rejected",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(1, AdmissionStatus::Rejected("full".into()))]),
            PublishVisibility::DestinationAdmissions,
            false,
        ),
        (
            "wrong target",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(2, AdmissionStatus::Accepted)]),
            PublishVisibility::DestinationAdmissions,
            false,
        ),
        (
            "interceptor drop",
            PublishAcknowledgement::DroppedByInterceptor,
            PublishVisibility::Opaque,
            false,
        ),
        ("opaque accepted", opaque(), PublishVisibility::Opaque, true),
        (
            "target accepted",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(1, AdmissionStatus::Accepted)]),
            PublishVisibility::DestinationAdmissions,
            true,
        ),
    ];
    for (label, acknowledgement, visibility, expected_pass) in cases {
        let capabilities = EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            visibility,
        );
        let sync = run_sync(
            || {
                Arc::new(SubscriptionModeSyncAdapter {
                    inner: FakeEventBusSpi::with_capabilities(capabilities),
                    seen: Arc::default(),
                    reject: None,
                    acknowledgement: Some(acknowledgement.clone()),
                })
            },
            &ConformanceHooks::default(),
        );
        let asynchronous = block_on(run_async(
            || async {
                Arc::new(SubscriptionModeAsyncAdapter {
                    inner: FakeAsyncEventBusSpi::with_capabilities(capabilities),
                    seen: Arc::default(),
                    reject: None,
                    acknowledgement: Some(acknowledgement.clone()),
                }) as Arc<dyn AsyncEventBusSpi>
            },
            &AsyncConformanceHooks::default(),
        ));
        for (api, report) in [("sync", &sync), ("async", &asynchronous)] {
            let setup_index = report
                .cases()
                .iter()
                .enumerate()
                .find_map(|(index, case)| {
                    (matches!(case,
                        ConformanceCase::Passed { case_id } | ConformanceCase::Failed { case_id, .. }
                            if case_id == "close-with-pending-setup")
                        && report.case_context(index).is_some_and(|context| context.api() == api))
                    .then_some(index)
                })
                .unwrap_or_else(|| panic!("{api} {label}: pending setup case was not reported: {report:?}"));
            assert_eq!(
                matches!(report.cases()[setup_index], ConformanceCase::Passed { .. }),
                expected_pass,
                "{api} {label}: unexpected pending setup result: {report:?}"
            );
            let case_for_api = |target: &str| {
                report.cases().iter().enumerate().any(|(index, case)| {
                    matches!(case,
                        ConformanceCase::Passed { case_id } | ConformanceCase::Failed { case_id, .. }
                            if case_id == target)
                        && report.case_context(index).is_some_and(|context| context.api() == api)
                })
            };
            let passed_for_api = |target: &str| {
                report.cases().iter().enumerate().any(|(index, case)| {
                    matches!(case, ConformanceCase::Passed { case_id } if case_id == target)
                        && report.case_context(index).is_some_and(|context| context.api() == api)
                })
            };
            assert!(
                case_for_api("close-with-pending-setup"),
                "{api} {label}: setup assertion must target its report"
            );
            if expected_pass {
                assert!(
                    passed_for_api("receive-after-close-with-pending"),
                    "{api} {label}: close receive must proceed"
                );
            } else {
                assert!(
                    passed_for_api("close-with-pending-setup-cleanup"),
                    "{api} {label}: failed setup must still close the receiver and shut down the SPI"
                );
                assert!(
                    !passed_for_api("receive-after-close-with-pending"),
                    "{api} {label}: failed setup must not report the dependent receive as passed"
                );
            }
        }
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_pending_close_shutdown_fault_matrix_for_sync_and_async_runners() {
    use crate::support::pending_close_shutdown_fixture::AsyncFixture;
    use crate::support::pending_close_shutdown_fixture::InjectedShutdown;
    use crate::support::pending_close_shutdown_fixture::SyncFixture;

    for (label, mode, expected_pass, detail) in [
        ("complete", InjectedShutdown::Complete, true, None),
        ("timed out", InjectedShutdown::TimedOut, false, Some("TimedOut")),
        (
            "error",
            InjectedShutdown::Error,
            false,
            Some("pending-close-shutdown-injected"),
        ),
    ] {
        let sync = run_sync(|| Arc::new(SyncFixture::new(mode)), &ConformanceHooks::default());
        let asynchronous = block_on(run_async(
            || async { Arc::new(AsyncFixture::new(mode)) as Arc<dyn AsyncEventBusSpi> },
            &AsyncConformanceHooks::default(),
        ));

        for (api, report) in [("sync", &sync), ("async", &asynchronous)] {
            let result = report
                .cases()
                .iter()
                .enumerate()
                .find(|(index, case)| {
                    matches!(case, ConformanceCase::Passed { case_id } | ConformanceCase::Failed { case_id, .. }
                    if case_id == "close-with-pending-shutdown")
                        && report.case_context(*index).is_some_and(|context| context.api() == api)
                })
                .unwrap_or_else(|| panic!("{api} {label}: pending-close shutdown missing: {report:?}"));
            assert_eq!(
                matches!(result.1, ConformanceCase::Passed { .. }),
                expected_pass,
                "{api} {label}: {report:?}"
            );
            let context = report.case_context(result.0).unwrap();
            assert_eq!(context.api(), api);
            assert_eq!(context.payload_mode(), PayloadModes::Native);
            assert_eq!(context.durability(), SubscriptionDurability::Ephemeral);
            if let Some(detail) = detail {
                let ConformanceCase::Failed { detail: actual, .. } = result.1 else {
                    unreachable!()
                };
                assert!(actual.contains(detail), "{api} {label}: {actual}");
                assert!(
                    !report.all_passed(),
                    "{api} {label}: failed cleanup must fail aggregate report"
                );
            } else {
                assert!(report.all_passed(), "{api} {label}: {report:?}");
            }
            assert!(
                report.cases().iter().enumerate().any(|(index, case)| {
                    matches!(case, ConformanceCase::Passed { case_id } if case_id == "shutdown")
                        && report.case_context(index).is_some_and(|context| context.api() == api)
                }),
                "{api} {label}: ordinary shutdown case should stay passed"
            );
        }
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_durable_only_conformance_does_not_invoke_ephemeral_cleanup_hooks() {
    let capabilities = EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::None,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Durable,
        SubscriptionModes::DURABLE,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    );
    let sync_hooks = ConformanceHooks {
        ephemeral_cleanup: Some(Arc::new(|| panic!("durable-only provider has no ephemeral cleanup"))),
        durable_recovery: Some(Arc::new(|| Ok(()))),
        ..ConformanceHooks::default()
    };
    let async_hooks = AsyncConformanceHooks {
        ephemeral_cleanup: Some(Arc::new(|| {
            Box::pin(async { panic!("durable-only provider has no ephemeral cleanup") })
        })),
        durable_recovery: Some(Arc::new(|| Box::pin(async { Err("recovery fixture failed".into()) }))),
        ..AsyncConformanceHooks::default()
    };
    let sync_report = run_sync(
        || Arc::new(FakeEventBusSpi::with_capabilities(capabilities)),
        &sync_hooks,
    );
    let async_report = block_on(run_async(
        || async { Arc::new(FakeAsyncEventBusSpi::with_capabilities(capabilities)) as Arc<dyn AsyncEventBusSpi> },
        &async_hooks,
    ));
    assert!(sync_report.all_passed(), "{sync_report:?}");
    assert!(!async_report.all_passed());
    for report in [&sync_report, &async_report] {
        assert!(report.cases().iter().any(|case| matches!(case,
            ConformanceCase::Skipped {
                case_id,
                reason: ConformanceSkipReason::UnsupportedCapability {
                    capability: "ephemeral-subscription",
                },
            }
                if case_id == "ephemeral-cleanup"
        )));
    }
    assert!(async_report.cases().iter().any(|case| matches!(case,
        ConformanceCase::Failed { case_id, detail }
            if case_id == "durable-unsettled-recovery" && detail == "recovery fixture failed"
    )));
}

#[cfg(feature = "conformance")]
#[test]
fn test_conformance_publish_failures_skip_receive_and_still_close_providers() {
    let sync_report = run_sync(
        || {
            let spi = Arc::new(FakeEventBusSpi::new());
            spi.fail_next_publish();
            spi
        },
        &ConformanceHooks::default(),
    );
    let async_report = block_on(run_async(
        || async {
            let spi = Arc::new(FakeAsyncEventBusSpi::new());
            spi.fail_next_publish();
            spi as Arc<dyn AsyncEventBusSpi>
        },
        &AsyncConformanceHooks::default(),
    ));

    for report in [sync_report, async_report] {
        assert!(!report.all_passed());
        assert!(report.cases().iter().any(|case| matches!(case,
            ConformanceCase::Failed { case_id, .. } if case_id == "declared-native-publish"
        )));
        assert!(report.cases().iter().any(|case| matches!(case,
            ConformanceCase::Skipped { case_id, reason: ConformanceSkipReason::MissingFixture { detail } }
                if case_id == "receive-payload" && detail == "publish failed"
        )));
        for expected in ["close", "close-idempotence", "shutdown", "shutdown-idempotence"] {
            assert!(
                report.cases().iter().any(|case| matches!(case,
                    ConformanceCase::Passed { case_id } if case_id == expected
                )),
                "cleanup case {expected} must run after publish failure"
            );
        }
    }

    let strict_report = run_sync_with_profile(
        || {
            let spi = Arc::new(FakeEventBusSpi::new());
            spi.fail_next_publish();
            spi
        },
        &ConformanceHooks::default(),
        ConformanceProfile::Strict,
    );
    let strict_failure_index = strict_report
        .cases()
        .iter()
        .position(|case| {
            matches!(case,
                ConformanceCase::Failed { case_id, detail }
                    if case_id == "receive-payload" && detail.contains("strict profile requires")
            )
        })
        .expect("strict profile promotes the missing receive fixture");
    assert!(strict_report.case_context(strict_failure_index).is_some());
}

#[cfg(feature = "conformance")]
#[test]
fn test_conformance_without_settlement_skips_hooks_even_when_supplied() {
    let sync_hooks = ConformanceHooks {
        settlement: Some(Arc::new(|| panic!("unsupported settlement hook must not run"))),
        ..ConformanceHooks::default()
    };
    let async_hooks = AsyncConformanceHooks {
        settlement: Some(Arc::new(|| {
            Box::pin(async { panic!("unsupported settlement hook must not run") })
        })),
        settlement_cancellation: Some(Arc::new(|| {
            Box::pin(async { panic!("unsupported settlement cancellation must not run") })
        })),
        ..AsyncConformanceHooks::default()
    };
    let sync_report = run_sync(
        || Arc::new(FakeEventBusSpi::with_capabilities(native_no_settlement_capabilities())),
        &sync_hooks,
    );
    let async_report = block_on(run_async(
        || async {
            Arc::new(FakeAsyncEventBusSpi::with_capabilities(
                native_no_settlement_capabilities(),
            )) as Arc<dyn AsyncEventBusSpi>
        },
        &async_hooks,
    ));
    for report in [sync_report, async_report] {
        assert!(report.all_passed(), "{report:?}");
        assert!(report.cases().iter().any(|case| matches!(case,
            ConformanceCase::Skipped {
                case_id,
                reason: ConformanceSkipReason::UnsupportedCapability {
                    capability: "settlement",
                },
            }
                if case_id == "provider-settlement-idempotence"
        )));
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_public_conformance_runner_preserves_failed_and_skipped_case_results() {
    let hooks = ConformanceHooks {
        settlement: Some(Arc::new(|| Err("repeated settlement changed result".into()))),
        receive_cancellation: None,
        durable_recovery: None,
        close_cancellation: Some(Arc::new(|| Err("close cancellation lost progress".into()))),
        ..ConformanceHooks::default()
    };
    let report = run_sync(
        || Arc::new(FakeEventBusSpi::with_capabilities(full_capabilities())),
        &hooks,
    );
    assert!(!report.all_passed());
    assert!(report.cases().iter().any(|case| matches!(case,
        ConformanceCase::Skipped { case_id, reason: ConformanceSkipReason::NotApplicable { .. } }
        if case_id == "close-cancellation"
    )));
    assert!(matches!(
        report.cases().first(),
        Some(ConformanceCase::Passed { case_id }) if case_id == "capability-payload-mode"
    ));
    assert!(
        report
            .cases()
            .iter()
            .any(|case| matches!(case, ConformanceCase::Passed { case_id } if case_id == "declared-native-publish"))
    );
    assert!(report.cases().iter().any(
        |case| matches!(case, ConformanceCase::Failed { case_id, .. } if case_id == "provider-settlement-idempotence")
    ));
    assert!(
        report
            .cases()
            .iter()
            .any(|case| matches!(case, ConformanceCase::Skipped { case_id, .. } if case_id == "receive-cancellation"))
    );
}

#[cfg(feature = "conformance")]
#[test]
fn test_public_conformance_runner_probes_encoded_only_providers() {
    let report = run_sync(
        || Arc::new(FakeEventBusSpi::with_capabilities(encoded_settlement_capabilities())),
        &ConformanceHooks::default(),
    );
    assert!(report.all_passed());
    assert!(
        report
            .cases()
            .iter()
            .any(|case| matches!(case, ConformanceCase::Passed { case_id } if case_id == "declared-encoded-publish"))
    );
}

#[cfg(feature = "conformance")]
#[test]
fn test_conformance_detects_payload_and_envelope_corruption_in_sync_and_async_providers() {
    let native_cases = [
        (ConformanceTamper::NativeValue, "native payload value"),
        (ConformanceTamper::Topic, "topic"),
        (ConformanceTamper::EventId, "event ID"),
        (ConformanceTamper::Timestamp, "timestamp"),
        (ConformanceTamper::Headers, "headers"),
        (ConformanceTamper::OrderingKey, "ordering key"),
    ];
    for (tamper, field) in native_cases {
        let sync = run_sync(
            || {
                let spi = Arc::new(FakeEventBusSpi::new());
                spi.tamper_conformance_message(tamper);
                spi
            },
            &ConformanceHooks::default(),
        );
        let asynchronous = block_on(run_async(
            || async {
                let spi = Arc::new(FakeAsyncEventBusSpi::new());
                spi.tamper_conformance_message(tamper);
                spi as Arc<dyn AsyncEventBusSpi>
            },
            &AsyncConformanceHooks::default(),
        ));
        for report in [&sync, &asynchronous] {
            assert!(
                report.cases().iter().any(|case| matches!(case,
                    ConformanceCase::Failed { case_id, detail }
                        if case_id == "receive-payload"
                            && (detail == &format!("ephemeral mode: received message differs at {field}")
                                || detail == &format!("durable mode: received message differs at {field}"))
                )),
                "tampering {field} must fail: {report:?}"
            );
        }
    }
    let encoded_cases = [
        (ConformanceTamper::EncodedBytes, "encoded bytes"),
        (ConformanceTamper::ContentType, "encoded content type"),
        (ConformanceTamper::Schema, "encoded schema"),
    ];
    for (tamper, field) in encoded_cases {
        let sync = run_sync(
            || {
                let spi = Arc::new(FakeEventBusSpi::with_capabilities(encoded_settlement_capabilities()));
                spi.tamper_conformance_message(tamper);
                spi
            },
            &ConformanceHooks::default(),
        );
        let asynchronous = block_on(run_async(
            || async {
                let spi = Arc::new(FakeAsyncEventBusSpi::with_capabilities(
                    encoded_settlement_capabilities(),
                ));
                spi.tamper_conformance_message(tamper);
                spi as Arc<dyn AsyncEventBusSpi>
            },
            &AsyncConformanceHooks::default(),
        ));
        for report in [&sync, &asynchronous] {
            assert!(
                report.cases().iter().any(|case| matches!(case,
                    ConformanceCase::Failed { case_id, detail }
                        if case_id == "receive-payload"
                            && (detail == &format!("ephemeral mode: received message differs at {field}")
                                || detail == &format!("durable mode: received message differs at {field}"))
                )),
                "tampering {field} must fail: {report:?}"
            );
        }
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_strict_conformance_promotes_missing_required_fixtures_to_failures() {
    let report = run_sync_with_profile(
        || Arc::new(FakeEventBusSpi::with_capabilities(full_capabilities())),
        &ConformanceHooks::default(),
        ConformanceProfile::Strict,
    );

    assert!(report.cases().iter().any(|case| matches!(
        case,
        ConformanceCase::Failed { case_id, detail }
            if case_id == "provider-settlement-idempotence" && detail.contains("strict profile requires")
    )));
    assert!(catch_unwind(|| report.assert_all_passed()).is_err());
}

#[cfg(feature = "conformance")]
#[test]
fn test_public_conformance_runner_executes_each_provider_supplied_hook() {
    let successful_check: Arc<dyn Fn() -> Result<(), String> + Send + Sync> = Arc::new(|| Ok(()));
    let hooks = ConformanceHooks {
        settlement: Some(successful_check.clone()),
        receive_cancellation: Some(successful_check.clone()),
        durable_recovery: Some(successful_check.clone()),
        ephemeral_cleanup: Some(successful_check.clone()),
        settlement_cancellation: Some(successful_check.clone()),
        close_cancellation: Some(successful_check.clone()),
        shutdown_cancellation: Some(successful_check),
    };
    let report = run_sync(
        || Arc::new(FakeEventBusSpi::with_capabilities(full_capabilities())),
        &hooks,
    );

    assert!(report.all_passed(), "{report:?}");
    assert!(report.cases().iter().any(|case| matches!(
        case,
        ConformanceCase::Passed { case_id } if case_id == "ephemeral-cleanup"
    )));
    report.assert_all_passed();
}

#[cfg(feature = "conformance")]
#[test]
fn test_public_async_conformance_runner_executes_provider_hooks() {
    let successful_check: AsyncConformanceCheck = Arc::new(|| Box::pin(async { Ok(()) }));
    let hooks = AsyncConformanceHooks {
        settlement: Some(successful_check.clone()),
        receive_cancellation: Some(successful_check.clone()),
        subscribe_cancellation: Some(subscribe_cancellation_check(Arc::new(PartialSubscribeProvider::new(
            false,
        )))),
        durable_recovery: None,
        ephemeral_cleanup: Some(successful_check.clone()),
        settlement_cancellation: Some(successful_check.clone()),
        close_cancellation: Some(successful_check.clone()),
        shutdown_cancellation: Some(successful_check),
    };
    let report = block_on(run_async(
        || async {
            Arc::new(FakeAsyncEventBusSpi::with_capabilities(full_capabilities())) as Arc<dyn AsyncEventBusSpi>
        },
        &hooks,
    ));

    assert!(report.all_passed(), "{report:?}");
    assert!(report.cases().iter().any(|case| matches!(
        case,
        ConformanceCase::Passed { case_id } if case_id == "close-cancellation"
    )));
    report.assert_all_passed();
}

#[test]
fn test_sync_conformance_accepts_provider_supplied_trait_object_factory_and_gates_cases() {
    let full = FakeEventBusSpi::with_capabilities(full_capabilities());
    let full_cases = run_sync_conformance(
        &full,
        |request| full.subscribe(request),
        |_| {
            full.inject_gap();
            true
        },
    );
    assert!(full_cases.contains(&"native-publish-receive"));
    assert!(full_cases.contains(&"settlement"));
    assert_eq!(full.shutdown_transition_count(), 1);
    assert!(full.operation_log().contains(&"receive"));

    let native_no_settlement = FakeEventBusSpi::with_capabilities(native_no_settlement_capabilities());
    let limited_cases = run_sync_conformance(
        &native_no_settlement,
        |request| native_no_settlement.subscribe(request),
        |_| {
            native_no_settlement.inject_gap();
            true
        },
    );
    assert!(limited_cases.contains(&"native-publish-receive"));
    assert!(!limited_cases.contains(&"settlement"));
    assert!(limited_cases.contains(&"skip-settlement"));

    let channel = ChannelShapedEventBusSpi::new();
    let channel_cases = run_sync_conformance(
        &channel,
        |request| channel.subscribe(request),
        |_| {
            channel.inject_gap();
            true
        },
    );
    assert!(channel_cases.contains(&"native-publish-receive"));
    assert!(channel_cases.contains(&"skip-settlement"));

    let broker = FakeEventBusSpi::with_capabilities(encoded_settlement_capabilities());
    let broker_cases = run_sync_conformance(
        &broker,
        |request| broker.subscribe(request),
        |_| {
            broker.inject_gap();
            true
        },
    );
    assert!(broker_cases.contains(&"encoded-publish-receive"));
    assert!(broker_cases.contains(&"settlement"));
    assert!(broker.operation_log().contains(&"settle"));
}

#[test]
fn test_local_sync_provider_passes_supported_spi_conformance_cases() {
    let local_config = EventBusConfig::default().with_provider_options(LocalEventBusConfig::new().provider_options());
    let local = LocalEventBusProvider
        .create_configured(&local_config)
        .expect("local provider constructs through the provider contract");
    let local_cases = run_sync_conformance(local.as_ref(), |request| local.subscribe(request), |_| false);
    assert!(local_cases.contains(&"native-publish-receive"));
    assert!(local_cases.contains(&"settlement"));
    assert!(local_cases.contains(&"skip-gap-provider-does-not-support-gap-injection"));
}

#[cfg(feature = "conformance")]
#[test]
fn test_sync_local_passes_public_spi_conformance_publish_cases() {
    let config = EventBusConfig::default().with_provider_options(LocalEventBusConfig::new().provider_options());
    let report = run_sync(
        || {
            LocalEventBusProvider
                .create_configured(&config)
                .expect("local provider config is valid")
        },
        &ConformanceHooks::default(),
    );
    report.assert_all_passed();
}

#[cfg(feature = "conformance")]
#[test]
fn test_strict_local_ephemeral_cleanup_discards_unsettled_delivery() {
    let cleanup: Arc<dyn Fn() -> Result<(), String> + Send + Sync> = Arc::new(|| {
        let config = EventBusConfig::default().with_provider_options(LocalEventBusConfig::new().provider_options());
        let spi = LocalEventBusProvider
            .create_configured(&config)
            .map_err(|error| error.to_string())?;
        let request = subscription_request();
        let subscription_id = request.subscription_id();
        let mut receiver = spi.subscribe(request).map_err(|error| error.to_string())?;
        let acknowledgement = spi.publish(outbound_message()).map_err(|error| error.to_string())?;
        accepted_publication(acknowledgement, subscription_id)?;
        let unsettled = receiver
            .receive(Duration::from_secs(1))
            .map_err(|error| error.to_string())?;
        if !matches!(unsettled, ReceiveOutcome::Message(_)) {
            return Err("published delivery was not received".into());
        }
        receiver.close().map_err(|error| error.to_string())?;
        let mut replacement = spi
            .subscribe(subscription_request())
            .map_err(|error| error.to_string())?;
        if !matches!(replacement.receive(Duration::ZERO), Ok(ReceiveOutcome::TimedOut)) {
            return Err("closed ephemeral delivery was restored to a new receiver".into());
        }
        replacement.close().map_err(|error| error.to_string())?;
        let _ = spi
            .shutdown(ShutdownMode::Immediate)
            .map_err(|error| error.to_string())?;
        Ok(())
    });
    let hooks = ConformanceHooks {
        ephemeral_cleanup: Some(cleanup),
        ..ConformanceHooks::default()
    };
    let report = run_sync_with_profile(
        || {
            LocalEventBusProvider
                .create_configured(
                    &EventBusConfig::default().with_provider_options(LocalEventBusConfig::new().provider_options()),
                )
                .expect("local provider configuration is valid")
        },
        &hooks,
        ConformanceProfile::Strict,
    );
    assert!(
        report.cases().iter().any(|case| matches!(case,
            ConformanceCase::Passed { case_id } if case_id == "ephemeral-cleanup"
        )),
        "{report:?}"
    );
}

#[cfg(feature = "conformance")]
#[test]
fn test_bounded_channel_fixture_passes_public_spi_conformance_without_settlement() {
    let report = run_sync(create_flume_spi, &ConformanceHooks::default());
    report.assert_all_passed();
    assert!(report.cases().iter().any(
        |case| matches!(case, ConformanceCase::Skipped { case_id, .. } if case_id == "provider-settlement-idempotence")
    ));
}

#[cfg(feature = "conformance")]
#[test]
fn test_strict_conformance_fails_when_required_provider_hooks_are_missing() {
    let report = run_sync_with_profile(
        create_flume_spi,
        &ConformanceHooks::default(),
        ConformanceProfile::Strict,
    );
    assert!(!report.all_passed());
    assert!(report.cases().iter().any(|case| {
        matches!(case, ConformanceCase::Failed { case_id, detail }
            if case_id == "ephemeral-cleanup" && detail.contains("missing fixture"))
    }));
    assert!(report.cases().iter().any(|case| matches!(case,
        ConformanceCase::Skipped {
            case_id,
            reason: ConformanceSkipReason::UnsupportedCapability {
                capability: "durable-subscription",
            },
        }
        if case_id == "durable-unsettled-recovery"
    )));
    assert!(report.cases().iter().any(|case| matches!(case,
        ConformanceCase::Skipped { case_id, reason: ConformanceSkipReason::NotApplicable { .. } }
        if case_id == "receive-cancellation"
    )));
    assert!(report.cases().iter().any(|case| {
        matches!(case, ConformanceCase::Skipped {
            case_id,
            reason: ConformanceSkipReason::UnsupportedCapability { capability: "settlement" },
        } if case_id == "settlement-idempotence")
    }));
}

#[cfg(feature = "conformance")]
#[test]
fn test_strict_conformance_requires_each_advertised_durability_cleanup() {
    let report = run_sync_with_profile(
        || Arc::new(FakeEventBusSpi::with_capabilities(durable_test_capabilities())),
        &ConformanceHooks::default(),
        ConformanceProfile::Strict,
    );
    for case_id in ["ephemeral-cleanup", "durable-unsettled-recovery"] {
        assert!(report.cases().iter().any(|case| matches!(case,
            ConformanceCase::Failed { case_id: actual, detail }
            if actual == case_id && detail.contains("missing fixture")
        )));
    }
    assert!(report.cases().iter().any(|case| matches!(case,
        ConformanceCase::Skipped { case_id, reason: ConformanceSkipReason::NotApplicable { .. } }
        if case_id == "receive-cancellation"
    )));
}

#[cfg(feature = "conformance")]
#[test]
fn test_strict_conformance_reports_a_provider_durable_recovery_check() {
    let hooks = ConformanceHooks {
        settlement: Some(Arc::new(|| Ok(()))),
        receive_cancellation: Some(Arc::new(|| Ok(()))),
        durable_recovery: Some(Arc::new(|| Ok(()))),
        ephemeral_cleanup: Some(Arc::new(|| Ok(()))),
        settlement_cancellation: Some(Arc::new(|| Ok(()))),
        close_cancellation: Some(Arc::new(|| Ok(()))),
        shutdown_cancellation: Some(Arc::new(|| Ok(()))),
    };
    let report = run_sync_with_profile(
        || Arc::new(FakeEventBusSpi::with_capabilities(durable_test_capabilities())),
        &hooks,
        ConformanceProfile::Strict,
    );
    assert!(report.all_passed(), "{report:?}");
    assert!(
        report
            .cases()
            .iter()
            .any(|case| matches!(case, ConformanceCase::Passed { case_id } if case_id == "durable-unsettled-recovery"))
    );
}

#[cfg(feature = "conformance")]
#[test]
fn test_strict_async_conformance_awaits_a_provider_durable_recovery_check() {
    let check: AsyncConformanceCheck = Arc::new(|| Box::pin(async { Ok(()) }));
    let hooks = AsyncConformanceHooks {
        settlement: Some(check.clone()),
        receive_cancellation: Some(check.clone()),
        subscribe_cancellation: Some(subscribe_cancellation_check(Arc::new(PartialSubscribeProvider::new(
            false,
        )))),
        durable_recovery: Some(check),
        ephemeral_cleanup: Some(Arc::new(|| Box::pin(async { Ok(()) }))),
        settlement_cancellation: Some(Arc::new(|| Box::pin(async { Ok(()) }))),
        close_cancellation: Some(Arc::new(|| Box::pin(async { Ok(()) }))),
        shutdown_cancellation: Some(Arc::new(|| Box::pin(async { Ok(()) }))),
    };
    let report = block_on(run_async_with_profile(
        || async {
            Arc::new(FakeAsyncEventBusSpi::with_capabilities(durable_test_capabilities())) as Arc<dyn AsyncEventBusSpi>
        },
        &hooks,
        ConformanceProfile::Strict,
    ));
    assert!(report.all_passed(), "{report:?}");
    assert!(
        report
            .cases()
            .iter()
            .any(|case| matches!(case, ConformanceCase::Passed { case_id } if case_id == "durable-unsettled-recovery"))
    );
}

#[cfg(feature = "conformance")]
#[test]
fn test_async_subscribe_cancellation_case_is_profiled_once_and_ungated() {
    let missing_structural = block_on(run_async(
        || async {
            Arc::new(FakeAsyncEventBusSpi::with_capabilities(
                native_no_settlement_capabilities(),
            )) as Arc<dyn AsyncEventBusSpi>
        },
        &AsyncConformanceHooks::default(),
    ));
    assert!(missing_structural.cases().iter().any(|case| matches!(case,
        ConformanceCase::Skipped { case_id, reason: ConformanceSkipReason::MissingFixture { .. } }
            if case_id == "subscribe-cancellation"
    )));
    let missing_strict = block_on(run_async_with_profile(
        || async {
            Arc::new(FakeAsyncEventBusSpi::with_capabilities(
                native_no_settlement_capabilities(),
            )) as Arc<dyn AsyncEventBusSpi>
        },
        &AsyncConformanceHooks::default(),
        ConformanceProfile::Strict,
    ));
    assert!(missing_strict.cases().iter().any(|case| matches!(case,
        ConformanceCase::Failed { case_id, detail }
            if case_id == "subscribe-cancellation" && detail.contains("strict profile requires")
    )));

    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = count.clone();
    let hooks = AsyncConformanceHooks {
        subscribe_cancellation: Some(Arc::new(move || {
            observed.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Box::pin(async { Err("subscribe cancellation fixture failed".into()) })
        })),
        ..AsyncConformanceHooks::default()
    };
    let report = block_on(run_async(
        || async {
            Arc::new(FakeAsyncEventBusSpi::with_capabilities(
                native_no_settlement_capabilities(),
            )) as Arc<dyn AsyncEventBusSpi>
        },
        &hooks,
    ));
    assert_eq!(count.load(std::sync::atomic::Ordering::Acquire), 1);
    assert!(report.cases().iter().any(|case| matches!(case,
        ConformanceCase::Failed { case_id, detail }
            if case_id == "subscribe-cancellation" && detail == "subscribe cancellation fixture failed"
    )));

    let provider = Arc::new(PartialSubscribeProvider::new(false));
    let fixture = subscribe_cancellation_check(provider);
    assert_eq!(
        block_on(fixture()).expect("partial subscribe cancellation must release its guard"),
        ()
    );
    let broken = Arc::new(PartialSubscribeProvider::new(true));
    let fixture = subscribe_cancellation_check(broken);
    assert!(
        block_on(fixture())
            .expect_err("leaked guard must be detected")
            .contains("permission")
    );

    let successful_hooks = AsyncConformanceHooks {
        subscribe_cancellation: Some(subscribe_cancellation_check(Arc::new(PartialSubscribeProvider::new(
            false,
        )))),
        ..AsyncConformanceHooks::default()
    };
    let successful_report = block_on(run_async(
        || async {
            Arc::new(FakeAsyncEventBusSpi::with_capabilities(
                native_no_settlement_capabilities(),
            )) as Arc<dyn AsyncEventBusSpi>
        },
        &successful_hooks,
    ));
    let subscribe_cases = successful_report
        .cases()
        .iter()
        .filter(|case| {
            matches!(case,
                ConformanceCase::Passed { case_id } | ConformanceCase::Failed { case_id, .. }
                    if case_id == "subscribe-cancellation"
            )
        })
        .count();
    assert_eq!(subscribe_cases, 1, "subscribe cancellation hook must run exactly once");
    assert!(successful_report.cases().iter().any(|case| matches!(case,
        ConformanceCase::Passed { case_id } if case_id == "subscribe-cancellation"
    )));
}

#[cfg(feature = "conformance")]
fn subscribe_cancellation_check(provider: Arc<PartialSubscribeProvider>) -> AsyncConformanceCheck {
    Arc::new(move || {
        let provider = provider.clone();
        Box::pin(async move {
            {
                let mut creation = provider.subscribe(subscription_request());
                let mut context = Context::from_waker(Waker::noop());
                assert!(creation.as_mut().poll(&mut context).is_pending());
            }
            let mut subscription = provider
                .subscribe(subscription_request())
                .await
                .map_err(|error| format!("provider did not release subscribe permission: {error}"))?;
            subscription.close().await.map_err(|error| error.to_string())
        })
    })
}

#[cfg(feature = "conformance")]
struct PartialSubscribeProvider {
    inner: FakeAsyncEventBusSpi,
    permission: Arc<std::sync::atomic::AtomicBool>,
    attempts: std::sync::atomic::AtomicUsize,
    leak_guard: bool,
}

#[cfg(feature = "conformance")]
impl PartialSubscribeProvider {
    fn new(leak_guard: bool) -> Self {
        Self {
            inner: FakeAsyncEventBusSpi::new(),
            permission: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            attempts: std::sync::atomic::AtomicUsize::new(0),
            leak_guard,
        }
    }
}

#[cfg(feature = "conformance")]
struct SubscribePermissionGuard {
    permission: Arc<std::sync::atomic::AtomicBool>,
    leak: bool,
}

#[cfg(feature = "conformance")]
impl Drop for SubscribePermissionGuard {
    fn drop(&mut self) {
        if !self.leak {
            self.permission.store(false, std::sync::atomic::Ordering::Release);
        }
    }
}

#[cfg(feature = "conformance")]
impl AsyncEventBusSpi for PartialSubscribeProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            if self
                .permission
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                )
                .is_err()
            {
                return Err(SpiError::Operation {
                    provider_id: "partial-subscribe-fixture".into(),
                    operation: "subscribe",
                    resource: None,
                    kind: "permission_unavailable",
                    retryable: Some(true),
                    source: Box::new(std::io::Error::other("subscribe permission is still held")),
                });
            }
            let _guard = SubscribePermissionGuard {
                permission: self.permission.clone(),
                leak: self.leak_guard,
            };
            if self.attempts.fetch_add(1, std::sync::atomic::Ordering::AcqRel) == 0 {
                std::future::poll_fn(|_| std::task::Poll::<()>::Pending).await;
            }
            self.inner.subscribe(request).await
        })
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

/// Declares both durability modes so strict checks require both cleanup hooks.
#[cfg(feature = "conformance")]
fn durable_test_capabilities() -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::AcceptRetryReject,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Durable,
        SubscriptionModes::BOTH,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    )
}

#[test]
fn test_bounded_channel_fixture_reports_bounded_admission_and_supports_typed_facade_delivery() {
    let spi = create_flume_spi();
    let request = subscription_request();
    let mut receiver = spi
        .subscribe(request)
        .expect("bounded-channel provider must accept the conformance subscription");
    let first = spi
        .publish(outbound_message())
        .expect("first bounded-channel publish must be admitted");
    let second = spi
        .publish(outbound_message())
        .expect("second bounded-channel publish must report its bounded rejection");
    assert!(matches!(
        first,
        PublishAcknowledgement::DestinationAdmissions(ref admissions)
            if matches!(admissions[0].status(), AdmissionStatus::Accepted)
    ));
    assert!(matches!(
        second,
        PublishAcknowledgement::DestinationAdmissions(ref admissions)
            if matches!(
                admissions[0].status(),
                AdmissionStatus::Rejected(reason) if reason.as_ref() == "subscription queue is full"
            )
    ));
    assert!(matches!(
        receiver
            .receive(Duration::ZERO)
            .expect("queued bounded-channel message must be receivable"),
        ReceiveOutcome::Message(_)
    ));
    receiver.close().expect("bounded-channel subscription must close");
    assert!(matches!(
        spi.publish(outbound_message())
            .expect("publish after close must return an empty admission result"),
        PublishAcknowledgement::DestinationAdmissions(admissions) if admissions.is_empty()
    ));
    let _ = spi
        .shutdown(ShutdownMode::Immediate)
        .expect("bounded-channel provider must shut down");

    let spi = create_flume_spi();
    let bus = EventBus::from_spi(
        ProviderId::new("bounded-channel").expect("static provider ID must be valid"),
        spi,
    )
    .expect("valid provider capabilities");
    let topic = Topic::<u32>::new("test.topic").expect("static test topic must be valid");
    let (sender, receiver) = channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("typed", topic.clone()).expect("static subscriber ID must be valid"),
            move |delivery| {
                sender
                    .send(*delivery.payload())
                    .expect("typed delivery observer must remain connected");
            },
        )
        .expect("typed facade subscription must start");
    let _ = bus
        .publish(PublishRequest::new(topic, 42).expect("typed publish request must be valid"))
        .expect("typed publication must be admitted");
    assert_eq!(
        42,
        receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("typed delivery must reach the observer")
    );
    subscription.cancel().expect("typed facade subscription must cancel");
    let report = bus
        .shutdown(ShutdownMode::Immediate)
        .expect("bounded-channel facade must shut down successfully");
    assert_eq!(ShutdownOutcome::Complete, report.outcome);
    assert_eq!(0, report.known_abandoned_deliveries);
    assert!(report.provider_may_have_abandoned_deliveries);
}

#[test]
#[cfg(feature = "conformance")]
fn test_bounded_channel_close_discards_unreceived_publication() {
    let spi = create_flume_spi();
    let request = subscription_request();
    let subscription_id = request.subscription_id();
    let mut receiver = spi
        .subscribe(request)
        .expect("bounded-channel provider must accept the subscription");
    accepted_publication(
        spi.publish(outbound_message())
            .expect("publication setup must succeed before close"),
        subscription_id,
    )
    .expect("publication must be admitted to the receiver before close");
    receiver.close().expect("subscription close must succeed");
    assert!(matches!(
        receiver
            .receive(Duration::ZERO)
            .expect("receive after close must return an outcome"),
        ReceiveOutcome::Closed
    ));
    receiver.close().expect("repeated close must remain successful");
}

/// Exercises synchronous provider behavior and records unsupported cases.
fn run_sync_conformance(
    bus: &dyn EventBusSpi,
    subscribe: impl Fn(SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError>,
    inject_gap: impl Fn(&mut dyn EventSubscriptionSpi) -> bool,
) -> Vec<&'static str> {
    let capabilities = bus.capabilities();
    let request = subscription_request();
    let subscription_id = request.subscription_id();
    let mut subscription = subscribe(request).expect("conformance provider must accept its subscription");
    let mut cases = Vec::new();

    let mut received_settlement = None;
    {
        let payload_mode = capabilities.payload_modes();
        if matches!(payload_mode, PayloadModes::Native | PayloadModes::NativeAndEncoded) {
            let acknowledgement = bus
                .publish(outbound_native())
                .expect("native conformance publication must succeed");
            accepted_publication(acknowledgement, subscription_id).unwrap_or_else(|error| panic!("{error}"));
            let ReceiveOutcome::Message(mut message) = subscription
                .receive(Duration::ZERO)
                .expect("native conformance delivery must be receivable")
            else {
                panic!("native publication should be received");
            };
            assert!(matches!(message.payload(), TransportPayload::Native(_)));
            received_settlement = message.take_settlement();
            cases.push("native-publish-receive");
        } else if payload_mode == PayloadModes::Encoded {
            let acknowledgement = bus
                .publish(outbound_encoded())
                .expect("encoded conformance publication must succeed");
            accepted_publication(acknowledgement, subscription_id).unwrap_or_else(|error| panic!("{error}"));
            let ReceiveOutcome::Message(mut message) = subscription
                .receive(Duration::ZERO)
                .expect("encoded conformance delivery must be receivable")
            else {
                panic!("encoded publication should be received");
            };
            let TransportPayload::Encoded(payload) = message.payload() else {
                panic!("broker-shaped provider must preserve encoded payloads");
            };
            assert_eq!(b"conformance-payload", payload.bytes());
            assert_eq!("application/octet-stream", payload.content_type().as_str());
            let token = message
                .take_settlement()
                .expect("broker-shaped delivery carries an opaque settlement token");
            received_settlement = Some(token);
            cases.push("encoded-publish-receive");
        }
    }
    assert!(matches!(
        subscription
            .receive(Duration::ZERO)
            .expect("zero-timeout receive must return a conformance outcome"),
        ReceiveOutcome::TimedOut
    ));
    cases.push("zero-timeout");
    assert!(matches!(
        subscription
            .receive(Duration::from_millis(25))
            .expect("finite-timeout receive must return a conformance outcome"),
        ReceiveOutcome::TimedOut
    ));
    cases.push("finite-positive-timeout");

    if inject_gap(subscription.as_mut()) {
        assert!(matches!(
            subscription
                .receive(Duration::ZERO)
                .expect("gap injection must return a conformance outcome"),
            ReceiveOutcome::Gap(_)
        ));
        cases.push("gap");
    } else {
        cases.push("skip-gap-provider-does-not-support-gap-injection");
    }

    match capabilities.settlement() {
        SettlementCapabilities::None => cases.push("skip-settlement"),
        SettlementCapabilities::AcceptOnly | SettlementCapabilities::AcceptRetryReject => {
            if let Some(token) = received_settlement {
                subscription
                    .settle(&token, DeliveryDisposition::Accept)
                    .expect("first settlement of a real delivery must succeed");
                subscription
                    .settle(&token, DeliveryDisposition::Accept)
                    .expect("repeating the same real delivery settlement is idempotent");
                let conflict = subscription
                    .settle(&token, DeliveryDisposition::Reject)
                    .expect_err("conflicting disposition must be rejected");
                assert_eq!(conflict.kind(), "invalid_settlement_token");
            } else {
                let token = SettlementToken::new(subscription_id, "conformance-token");
                subscription
                    .settle(&token, DeliveryDisposition::Accept)
                    .expect("provider must accept settlement for the supplied token");
                assert!(subscription.settle(&token, DeliveryDisposition::Accept).is_err());
            }
            cases.push("settlement");
        }
        _ => cases.push("skip-unknown-settlement-capability"),
    }

    subscription.close().expect("conformance subscription must close");
    assert!(matches!(
        subscription
            .receive(Duration::ZERO)
            .expect("receive after close must report the closed outcome"),
        ReceiveOutcome::Closed
    ));
    cases.push("close");
    let shutdown = ShutdownMode::Graceful {
        timeout: Duration::ZERO,
    };
    assert_eq!(
        bus.shutdown(shutdown)
            .expect("first conformance shutdown must complete"),
        ShutdownOutcome::Complete
    );
    assert_eq!(
        bus.shutdown(shutdown)
            .expect("repeated conformance shutdown must remain complete"),
        ShutdownOutcome::Complete
    );
    assert_eq!(bus.capabilities(), capabilities);
    cases.push("shutdown-and-stable-capabilities");
    cases
}

#[test]
fn test_async_conformance_uses_manual_time_and_preserves_in_flight_message_on_cancel() {
    let full = FakeAsyncEventBusSpi::with_capabilities(full_capabilities());
    let full_cases = run_async_conformance(&full, || full.inject_gap(), |by| full.advance_time(by));
    assert!(full_cases.contains(&"native-publish-receive"));
    assert!(full_cases.contains(&"async-cancel-redelivery"));
    assert!(full_cases.contains(&"settlement"));
    assert_eq!(full.shutdown_transition_count(), 1);
    assert!(full.operation_log().contains(&"receive"));

    let limited = FakeAsyncEventBusSpi::with_capabilities(native_no_settlement_capabilities());
    let limited_cases = run_async_conformance(&limited, || limited.inject_gap(), |by| limited.advance_time(by));
    assert!(limited_cases.contains(&"native-publish-receive"));
    assert!(!limited_cases.contains(&"settlement"));
    assert!(limited_cases.contains(&"skip-settlement"));
}

#[test]
fn test_sync_finite_timeout_rechecks_after_spurious_wake() {
    let bus = FakeEventBusSpi::new();
    let mut subscription = bus
        .subscribe(subscription_request())
        .expect("fake provider must accept the subscription");
    let receive = thread::spawn(move || subscription.receive(Duration::from_secs(2)));

    bus.wait_until_receive_is_blocked();
    bus.wake_receivers_spuriously();
    bus.wait_until_spurious_wake_is_observed();
    bus.enqueue(inbound_message(None));

    let receive_result = receive
        .join()
        .expect("receive worker thread must exit without panicking");
    assert!(matches!(
        receive_result.expect("receive must complete after the message is enqueued"),
        ReceiveOutcome::Message(_)
    ));
}

/// Drives cancellation and timeout cases without relying on a runtime.
fn run_async_conformance(
    bus: &dyn AsyncEventBusSpi,
    inject_gap: impl Fn(),
    advance_time: impl Fn(Duration),
) -> Vec<&'static str> {
    let capabilities = bus.capabilities();
    let mut cases = Vec::new();
    block_on(async {
        let request = subscription_request();
        let subscription_id = request.subscription_id();
        let mut subscription = bus
            .subscribe(request)
            .await
            .expect("async conformance provider must accept its subscription");

        if matches!(
            capabilities.payload_modes(),
            PayloadModes::Native | PayloadModes::NativeAndEncoded
        ) {
            let acknowledgement = bus
                .publish(outbound_native())
                .await
                .expect("async native conformance publication must succeed");
            accepted_publication(acknowledgement, subscription_id).unwrap_or_else(|error| panic!("{error}"));
            let mut cancelled = Box::pin(subscription.receive(Duration::from_secs(30)));
            match poll_once(cancelled.as_mut()) {
                Poll::Pending => {
                    drop(cancelled);
                    assert!(matches!(
                        subscription
                            .receive(Duration::ZERO)
                            .await
                            .expect("redelivered message must be receivable after cancellation"),
                        ReceiveOutcome::Message(_)
                    ));
                    cases.push("async-cancel-redelivery");
                }
                Poll::Ready(Ok(ReceiveOutcome::Message(_))) => {
                    drop(cancelled);
                    cases.push("async-cancel-window-unavailable");
                }
                _ => panic!("published message was not delivered or safely retained"),
            }
            cases.push("native-publish-receive");
        } else {
            cases.push("skip-native-publish-receive");
        }

        let mut timeout = Box::pin(subscription.receive(Duration::from_secs(7)));
        assert!(poll_once(timeout.as_mut()).is_pending());
        advance_time(Duration::from_secs(7));
        assert!(matches!(
            poll_once(timeout.as_mut()),
            Poll::Ready(Ok(ReceiveOutcome::TimedOut))
        ));
        drop(timeout);
        cases.push("finite-timeout");

        inject_gap();
        assert!(matches!(
            subscription
                .receive(Duration::ZERO)
                .await
                .expect("async gap injection must return a conformance outcome"),
            ReceiveOutcome::Gap(_)
        ));
        cases.push("gap");

        match capabilities.settlement() {
            SettlementCapabilities::None => cases.push("skip-settlement"),
            SettlementCapabilities::AcceptOnly | SettlementCapabilities::AcceptRetryReject => {
                let token = SettlementToken::new(subscription_id, "async-token");
                subscription
                    .settle(&token, DeliveryDisposition::Accept)
                    .await
                    .expect("async provider must accept settlement for the supplied token");
                subscription
                    .settle(&token, DeliveryDisposition::Accept)
                    .await
                    .expect("repeated identical settlement is idempotent");
                let conflict = subscription
                    .settle(&token, DeliveryDisposition::Reject)
                    .await
                    .expect_err("conflicting disposition must be rejected");
                assert_eq!(conflict.kind(), "invalid_settlement_token");
                cases.push("settlement");
            }
            _ => cases.push("skip-unknown-settlement-capability"),
        }

        subscription
            .close()
            .await
            .expect("async conformance subscription must close");
        assert!(matches!(
            subscription
                .receive(Duration::ZERO)
                .await
                .expect("async receive after close must report the closed outcome"),
            ReceiveOutcome::Closed
        ));
        cases.push("close");
        let mode = ShutdownMode::Graceful {
            timeout: Duration::ZERO,
        };
        assert_eq!(
            bus.shutdown(mode)
                .await
                .expect("first async conformance shutdown must complete"),
            ShutdownOutcome::Complete
        );
        assert_eq!(
            bus.shutdown(mode)
                .await
                .expect("repeated async conformance shutdown must remain complete"),
            ShutdownOutcome::Complete
        );
        assert_eq!(bus.capabilities(), capabilities);
        cases.push("shutdown-and-stable-capabilities");
    });
    cases
}

/// Creates the native message used by each provider fixture.
fn outbound_native() -> OutboundMessage {
    outbound_message()
}

/// Creates an encoded message with content type and schema metadata.
fn outbound_encoded() -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("test.topic").expect("static encoded topic must be valid"),
        EventId::new("event-encoded-outbound").expect("static event ID must be valid"),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"conformance-payload"[..]),
            ContentType::new("application/octet-stream").expect("static encoded content type must be valid"),
            Some(SchemaId::new("test-schema-v1").expect("static schema ID must be valid")),
        )),
    )
}

#[test]
fn test_sync_fake_supports_injected_structured_provider_failures() {
    let bus = FakeEventBusSpi::new();
    bus.fail_next_publish();
    let error = bus.publish(outbound_message()).unwrap_err();
    assert_eq!(error.provider_id(), "fake");
    assert_eq!(error.operation(), "publish");
}

#[test]
fn test_async_fake_supports_injected_structured_provider_failures() {
    let bus = FakeAsyncEventBusSpi::new();
    bus.fail_next_publish();
    block_on(async {
        let error = bus.publish(outbound_message()).await.unwrap_err();
        assert_eq!(error.provider_id(), "fake");
        assert_eq!(error.operation(), "publish");
    });
}
