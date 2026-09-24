// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Backend-neutral SPI conformance checks shared by provider implementations.

mod support;

use std::sync::Arc;
use std::time::Duration;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::SpiError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_spi::ServiceProvider;
use support::fake_spi::FakeAsyncEventBusSpi;
use support::fake_spi::FakeEventBusSpi;

#[test]
fn sync_conformance_accepts_provider_supplied_trait_object_factory_and_gates_cases() {
    let full = FakeEventBusSpi::with_capabilities(support::fake_spi::full_capabilities());
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
        FakeEventBusSpi::with_capabilities(support::fake_spi::native_no_settlement_capabilities());
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

    let channel = support::provider_shapes::ChannelShapedEventBusSpi::new();
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

    let broker = FakeEventBusSpi::with_capabilities(support::provider_shapes::encoded_settlement_capabilities());
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
fn local_sync_provider_passes_supported_spi_conformance_cases() {
    let local_config = EventBusConfig::default().with_provider_options(LocalEventBusConfig::new().provider_options());
    let local = LocalEventBusProvider
        .create_configured(&local_config)
        .expect("local provider constructs through the provider contract");
    let local_cases = run_sync_conformance(local.as_ref(), |request| local.subscribe(request), |_| false);
    assert!(local_cases.contains(&"native-publish-receive"));
    assert!(local_cases.contains(&"settlement"));
    assert!(local_cases.contains(&"skip-gap-provider-does-not-support-gap-injection"));
}

fn run_sync_conformance(
    bus: &dyn EventBusSpi,
    subscribe: impl Fn(SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError>,
    inject_gap: impl Fn(&mut dyn EventSubscriptionSpi) -> bool,
) -> Vec<&'static str> {
    let capabilities = bus.capabilities();
    let request = support::fake_spi::subscription_request();
    let subscription_id = request.subscription_id();
    let mut subscription = subscribe(request).unwrap();
    let mut cases = Vec::new();

    let mut received_settlement = None;
    {
        let payload_mode = capabilities.payload_modes();
        if matches!(payload_mode, PayloadModes::Native | PayloadModes::NativeAndEncoded) {
            bus.publish(outbound_native()).unwrap();
            let ReceiveOutcome::Message(mut message) = subscription.receive(Duration::ZERO).unwrap() else {
                panic!("native publication should be received");
            };
            assert!(matches!(message.payload(), TransportPayload::Native(_)));
            received_settlement = message.take_settlement();
            cases.push("native-publish-receive");
        } else if payload_mode == PayloadModes::Encoded {
            bus.publish(outbound_encoded()).unwrap();
            let ReceiveOutcome::Message(mut message) = subscription.receive(Duration::ZERO).unwrap() else {
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
        subscription.receive(Duration::ZERO).unwrap(),
        ReceiveOutcome::TimedOut
    ));
    cases.push("zero-timeout");
    assert!(matches!(
        subscription.receive(Duration::from_millis(25)).unwrap(),
        ReceiveOutcome::TimedOut
    ));
    cases.push("finite-positive-timeout");

    if inject_gap(subscription.as_mut()) {
        assert!(matches!(
            subscription.receive(Duration::ZERO).unwrap(),
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
                subscription.settle(&token, DeliveryDisposition::Accept).unwrap();
                subscription
                    .settle(&token, DeliveryDisposition::Accept)
                    .expect("repeating the same real delivery settlement is idempotent");
                let conflict = subscription
                    .settle(&token, DeliveryDisposition::Reject)
                    .expect_err("conflicting disposition must be rejected");
                assert_eq!(conflict.kind(), "invalid_settlement_token");
            } else {
                let token = SettlementToken::new(subscription_id, "conformance-token");
                subscription.settle(&token, DeliveryDisposition::Accept).unwrap();
                assert!(subscription.settle(&token, DeliveryDisposition::Accept).is_err());
            }
            cases.push("settlement");
        }
        _ => cases.push("skip-unknown-settlement-capability"),
    }

    subscription.close().unwrap();
    assert!(matches!(
        subscription.receive(Duration::ZERO).unwrap(),
        ReceiveOutcome::Closed
    ));
    cases.push("close");
    let shutdown = ShutdownMode::Graceful {
        timeout: Duration::ZERO,
    };
    assert_eq!(bus.shutdown(shutdown).unwrap(), ShutdownOutcome::Complete);
    assert_eq!(bus.shutdown(shutdown).unwrap(), ShutdownOutcome::Complete);
    assert_eq!(bus.capabilities(), capabilities);
    cases.push("shutdown-and-stable-capabilities");
    cases
}

#[test]
fn async_conformance_uses_manual_time_and_preserves_in_flight_message_on_cancel() {
    let full = FakeAsyncEventBusSpi::with_capabilities(support::fake_spi::full_capabilities());
    let full_cases = run_async_conformance(&full, || full.inject_gap(), |by| full.advance_time(by));
    assert!(full_cases.contains(&"native-publish-receive"));
    assert!(full_cases.contains(&"async-cancel-redelivery"));
    assert!(full_cases.contains(&"settlement"));
    assert_eq!(full.shutdown_transition_count(), 1);
    assert!(full.operation_log().contains(&"receive"));

    let limited = FakeAsyncEventBusSpi::with_capabilities(support::fake_spi::native_no_settlement_capabilities());
    let limited_cases = run_async_conformance(&limited, || limited.inject_gap(), |by| limited.advance_time(by));
    assert!(limited_cases.contains(&"native-publish-receive"));
    assert!(!limited_cases.contains(&"settlement"));
    assert!(limited_cases.contains(&"skip-settlement"));
}

#[test]
fn sync_finite_timeout_rechecks_after_spurious_wake() {
    let bus = FakeEventBusSpi::new();
    let mut subscription = bus.subscribe(support::fake_spi::subscription_request()).unwrap();
    let receive = std::thread::spawn(move || subscription.receive(Duration::from_secs(2)));

    bus.wait_until_receive_is_blocked();
    bus.wake_receivers_spuriously();
    bus.wait_until_spurious_wake_is_observed();
    bus.enqueue(support::fake_spi::inbound_message(None));

    assert!(matches!(receive.join().unwrap().unwrap(), ReceiveOutcome::Message(_)));
}

fn run_async_conformance(
    bus: &dyn AsyncEventBusSpi,
    inject_gap: impl Fn(),
    advance_time: impl Fn(Duration),
) -> Vec<&'static str> {
    let capabilities = bus.capabilities();
    let mut cases = Vec::new();
    support::manual_async::block_on(async {
        let request = support::fake_spi::subscription_request();
        let subscription_id = request.subscription_id();
        let mut subscription = bus.subscribe(request).await.unwrap();

        if matches!(
            capabilities.payload_modes(),
            PayloadModes::Native | PayloadModes::NativeAndEncoded
        ) {
            bus.publish(outbound_native()).await.unwrap();
            let mut cancelled = Box::pin(subscription.receive(Duration::from_secs(30)));
            match support::manual_async::poll_once(cancelled.as_mut()) {
                std::task::Poll::Pending => {
                    drop(cancelled);
                    assert!(matches!(
                        subscription.receive(Duration::ZERO).await.unwrap(),
                        ReceiveOutcome::Message(_)
                    ));
                    cases.push("async-cancel-redelivery");
                }
                std::task::Poll::Ready(Ok(ReceiveOutcome::Message(_))) => {
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
        assert!(support::manual_async::poll_once(timeout.as_mut()).is_pending());
        advance_time(Duration::from_secs(7));
        assert!(matches!(
            support::manual_async::poll_once(timeout.as_mut()),
            std::task::Poll::Ready(Ok(ReceiveOutcome::TimedOut))
        ));
        drop(timeout);
        cases.push("finite-timeout");

        inject_gap();
        assert!(matches!(
            subscription.receive(Duration::ZERO).await.unwrap(),
            ReceiveOutcome::Gap(_)
        ));
        cases.push("gap");

        match capabilities.settlement() {
            SettlementCapabilities::None => cases.push("skip-settlement"),
            SettlementCapabilities::AcceptOnly | SettlementCapabilities::AcceptRetryReject => {
                let token = SettlementToken::new(subscription_id, "async-token");
                subscription.settle(&token, DeliveryDisposition::Accept).await.unwrap();
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

        subscription.close().await.unwrap();
        assert!(matches!(
            subscription.receive(Duration::ZERO).await.unwrap(),
            ReceiveOutcome::Closed
        ));
        cases.push("close");
        let mode = ShutdownMode::Graceful {
            timeout: Duration::ZERO,
        };
        assert_eq!(bus.shutdown(mode).await.unwrap(), ShutdownOutcome::Complete);
        assert_eq!(bus.shutdown(mode).await.unwrap(), ShutdownOutcome::Complete);
        assert_eq!(bus.capabilities(), capabilities);
        cases.push("shutdown-and-stable-capabilities");
    });
    cases
}

fn outbound_native() -> OutboundMessage {
    support::fake_spi::outbound_message()
}

fn outbound_encoded() -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("test.topic").unwrap(),
        qubit_event_bus::model::EventId::new("event-encoded-outbound").unwrap(),
        std::time::SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(&b"conformance-payload"[..]),
            ContentType::new("application/octet-stream").unwrap(),
            Some(SchemaId::new("test-schema-v1").unwrap()),
        )),
    )
}

#[test]
fn sync_fake_supports_injected_structured_provider_failures() {
    let bus = FakeEventBusSpi::new();
    bus.fail_next_publish();
    let error = bus.publish(support::fake_spi::outbound_message()).unwrap_err();
    assert_eq!(error.provider_id(), "fake");
    assert_eq!(error.operation(), "publish");
}

#[test]
fn async_fake_supports_injected_structured_provider_failures() {
    let bus = FakeAsyncEventBusSpi::new();
    bus.fail_next_publish();
    support::manual_async::block_on(async {
        let error = bus.publish(support::fake_spi::outbound_message()).await.unwrap_err();
        assert_eq!(error.provider_id(), "fake");
        assert_eq!(error.operation(), "publish");
    });
}
