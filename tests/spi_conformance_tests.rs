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
use std::sync::mpsc::channel;
use std::task::Poll;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::SpiError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
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

fn accepted_publication(
    acknowledgement: PublishAcknowledgement,
    subscription_id: Id,
) -> Result<(), String> {
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
        ephemeral_cleanup: Some(Arc::new(|| {
            panic!("durable-only provider has no ephemeral cleanup")
        })),
        durable_recovery: Some(Arc::new(|| Ok(()))),
        ..ConformanceHooks::default()
    };
    let async_hooks = AsyncConformanceHooks {
        ephemeral_cleanup: Some(Arc::new(|| {
            Box::pin(async { panic!("durable-only provider has no ephemeral cleanup") })
        })),
        durable_recovery: Some(Arc::new(|| {
            Box::pin(async { Err("recovery fixture failed".into()) })
        })),
        ..AsyncConformanceHooks::default()
    };
    let sync_report = run_sync(
        || Arc::new(FakeEventBusSpi::with_capabilities(capabilities)),
        &sync_hooks,
    );
    let async_report = block_on(run_async(
        || async {
            Arc::new(FakeAsyncEventBusSpi::with_capabilities(capabilities))
                as Arc<dyn AsyncEventBusSpi>
        },
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
        for expected in [
            "close",
            "close-idempotence",
            "shutdown",
            "shutdown-idempotence",
        ] {
            assert!(
                report.cases().iter().any(|case| matches!(case,
                    ConformanceCase::Passed { case_id } if case_id == expected
                )),
                "cleanup case {expected} must run after publish failure"
            );
        }
    }
}

#[cfg(feature = "conformance")]
#[test]
fn test_conformance_without_settlement_skips_hooks_even_when_supplied() {
    let sync_hooks = ConformanceHooks {
        settlement: Some(Arc::new(|| {
            panic!("unsupported settlement hook must not run")
        })),
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
        || {
            Arc::new(FakeEventBusSpi::with_capabilities(
                native_no_settlement_capabilities(),
            ))
        },
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
        settlement: Some(Arc::new(
            || Err("repeated settlement changed result".into()),
        )),
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
        || {
            Arc::new(FakeEventBusSpi::with_capabilities(
                encoded_settlement_capabilities(),
            ))
        },
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
        durable_recovery: None,
        ephemeral_cleanup: Some(successful_check.clone()),
        settlement_cancellation: Some(successful_check.clone()),
        close_cancellation: Some(successful_check.clone()),
        shutdown_cancellation: Some(successful_check),
    };
    let report = block_on(run_async(
        || async {
            Arc::new(FakeAsyncEventBusSpi::with_capabilities(full_capabilities()))
                as Arc<dyn AsyncEventBusSpi>
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

    let native_no_settlement =
        FakeEventBusSpi::with_capabilities(native_no_settlement_capabilities());
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
    let local_config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().provider_options());
    let local = LocalEventBusProvider
        .create_configured(&local_config)
        .expect("local provider constructs through the provider contract");
    let local_cases = run_sync_conformance(
        local.as_ref(),
        |request| local.subscribe(request),
        |_| false,
    );
    assert!(local_cases.contains(&"native-publish-receive"));
    assert!(local_cases.contains(&"settlement"));
    assert!(local_cases.contains(&"skip-gap-provider-does-not-support-gap-injection"));
}

#[cfg(feature = "conformance")]
#[test]
fn test_sync_local_passes_public_spi_conformance_publish_cases() {
    let config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().provider_options());
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
        let config = EventBusConfig::default()
            .with_provider_options(LocalEventBusConfig::new().provider_options());
        let spi = LocalEventBusProvider
            .create_configured(&config)
            .map_err(|error| error.to_string())?;
        let request = subscription_request();
        let subscription_id = request.subscription_id();
        let mut receiver = spi.subscribe(request).map_err(|error| error.to_string())?;
        let acknowledgement = spi
            .publish(outbound_message())
            .map_err(|error| error.to_string())?;
        accepted_publication(acknowledgement, subscription_id)?;
        let unsettled = receiver
            .receive(Duration::from_secs(1))
            .map_err(|error| error.to_string())?;
        if !matches!(unsettled, ReceiveOutcome::Message(_)) {
            return Err("published delivery was not received".into());
        }
        receiver.close().map_err(|error| error.to_string())?;
        let mut replacement = spi
            .subscribe(crate::support::fake_spi::subscription_request())
            .map_err(|error| error.to_string())?;
        if !matches!(
            replacement.receive(Duration::ZERO),
            Ok(ReceiveOutcome::TimedOut)
        ) {
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
                    &EventBusConfig::default()
                        .with_provider_options(LocalEventBusConfig::new().provider_options()),
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
        || {
            Arc::new(FakeEventBusSpi::with_capabilities(
                durable_test_capabilities(),
            ))
        },
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
        || {
            Arc::new(FakeEventBusSpi::with_capabilities(
                durable_test_capabilities(),
            ))
        },
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
        durable_recovery: Some(check),
        ephemeral_cleanup: Some(Arc::new(|| Box::pin(async { Ok(()) }))),
        settlement_cancellation: Some(Arc::new(|| Box::pin(async { Ok(()) }))),
        close_cancellation: Some(Arc::new(|| Box::pin(async { Ok(()) }))),
        shutdown_cancellation: Some(Arc::new(|| Box::pin(async { Ok(()) }))),
    };
    let report = block_on(run_async_with_profile(
        || async {
            Arc::new(FakeAsyncEventBusSpi::with_capabilities(
                durable_test_capabilities(),
            )) as Arc<dyn AsyncEventBusSpi>
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
    receiver
        .close()
        .expect("bounded-channel subscription must close");
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
            SubscribeRequest::new("typed", topic.clone())
                .expect("static subscriber ID must be valid"),
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
    subscription
        .cancel()
        .expect("typed facade subscription must cancel");
    let report = bus.shutdown(ShutdownMode::Immediate).unwrap();
    assert_eq!(ShutdownOutcome::Complete, report.outcome);
    assert_eq!(0, report.known_abandoned_deliveries);
    assert!(report.provider_may_have_abandoned_deliveries);
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
    let mut subscription =
        subscribe(request).expect("conformance provider must accept its subscription");
    let mut cases = Vec::new();

    let mut received_settlement = None;
    {
        let payload_mode = capabilities.payload_modes();
        if matches!(
            payload_mode,
            PayloadModes::Native | PayloadModes::NativeAndEncoded
        ) {
            let acknowledgement = bus
                .publish(outbound_native())
                .expect("native conformance publication must succeed");
            accepted_publication(acknowledgement, subscription_id)
                .unwrap_or_else(|error| panic!("{error}"));
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
            accepted_publication(acknowledgement, subscription_id)
                .unwrap_or_else(|error| panic!("{error}"));
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
                assert!(
                    subscription
                        .settle(&token, DeliveryDisposition::Accept)
                        .is_err()
                );
            }
            cases.push("settlement");
        }
        _ => cases.push("skip-unknown-settlement-capability"),
    }

    subscription
        .close()
        .expect("conformance subscription must close");
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
    let limited_cases = run_async_conformance(
        &limited,
        || limited.inject_gap(),
        |by| limited.advance_time(by),
    );
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
            accepted_publication(acknowledgement, subscription_id)
                .unwrap_or_else(|error| panic!("{error}"));
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
            ContentType::new("application/octet-stream")
                .expect("static encoded content type must be valid"),
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
