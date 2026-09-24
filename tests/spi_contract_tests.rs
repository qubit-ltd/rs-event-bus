// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
mod support;

use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryGap;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::SettlementToken;
use qubit_id::Id;

fn assert_sync_object_safe(_: Option<&dyn EventBusSpi>) {}
fn assert_async_object_safe(_: Option<&dyn AsyncEventBusSpi>) {}

#[test]
fn test_spi_traits_are_object_safe() {
    assert_sync_object_safe(None);
    assert_async_object_safe(None);
}

#[test]
fn test_backend_capabilities_preserve_declared_dimensions() {
    let capabilities = EventBusCapabilities::new(
        PayloadModes::NativeAndEncoded,
        SettlementCapabilities::AcceptRetryReject,
        OrderingCapability::PerKey,
        DelayedDeliveryCapability::Native,
        DurabilityCapability::Durable,
        true,
        ReplayCapability::Timestamp,
        PublishGuarantee::DurablyStored,
        PublishVisibility::DestinationAdmissions,
    );

    assert_eq!(capabilities.payload_modes(), PayloadModes::NativeAndEncoded);
    assert_eq!(capabilities.settlement(), SettlementCapabilities::AcceptRetryReject);
    assert_eq!(capabilities.ordering(), OrderingCapability::PerKey);
    assert_eq!(capabilities.delayed_delivery(), DelayedDeliveryCapability::Native);
    assert_eq!(capabilities.durability(), DurabilityCapability::Durable);
    assert!(capabilities.consumer_groups());
    assert_eq!(capabilities.replay(), ReplayCapability::Timestamp);
    assert_eq!(capabilities.publish_guarantee(), PublishGuarantee::DurablyStored);
    assert_eq!(
        capabilities.publish_visibility(),
        PublishVisibility::DestinationAdmissions
    );
}

#[test]
fn test_settlement_token_rejects_a_different_subscription_origin() {
    let owning_subscription = Id::new(17);
    let another_subscription = Id::new(18);
    let token = SettlementToken::new(owning_subscription, "provider-token");

    assert!(token.belongs_to(owning_subscription));
    assert!(!token.belongs_to(another_subscription));
}

#[test]
fn test_external_provider_can_construct_a_gap_receive_outcome() {
    let gap = DeliveryGap::new("provider reported lag", Some(3));
    let outcome = ReceiveOutcome::Gap(gap);

    let ReceiveOutcome::Gap(gap) = outcome else {
        panic!("expected a gap outcome");
    };
    assert_eq!(gap.reason.as_ref(), "provider reported lag");
    assert_eq!(gap.missed, Some(3));
}

#[test]
fn test_inbound_message_can_transfer_native_payload_and_settlement_to_facade() {
    use std::sync::Arc;
    use std::time::SystemTime;

    use qubit_event_bus::model::EventId;
    use qubit_event_bus::model::Headers;
    use qubit_event_bus::model::ProviderMessageMetadata;
    use qubit_event_bus::spi::InboundMessage;
    use qubit_event_bus::spi::TopicAddress;
    use qubit_event_bus::spi::TransportPayload;

    let subscription_id = Id::new(29);
    let native: Arc<dyn std::any::Any + Send + Sync> = Arc::new(String::from("non-clone payload"));
    let message = InboundMessage::new(
        TopicAddress::new("events.transfer").expect("valid topic address"),
        EventId::new("event-transfer").expect("valid event ID"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        TransportPayload::Native(native),
        Some(SettlementToken::new(subscription_id, "provider-owned token")),
        ProviderMessageMetadata::from([("partition".to_owned(), "4".to_owned())]),
    );
    let (topic, event_id, timestamp, headers, ordering_key, payload, settlement, metadata) = message.into_parts();
    assert_eq!(topic.as_str(), "events.transfer");
    assert_eq!(event_id.as_str(), "event-transfer");
    assert_eq!(timestamp, SystemTime::UNIX_EPOCH);
    assert!(headers.is_empty());
    assert!(ordering_key.is_none());
    let TransportPayload::Native(payload) = payload else {
        panic!("native payload should remain native");
    };
    let payload = Arc::downcast::<String>(payload).expect("facade can downcast without cloning T");
    assert_eq!(payload.as_str(), "non-clone payload");
    assert!(settlement.expect("token is transferred").belongs_to(subscription_id));
    assert_eq!(metadata.get("partition").map(String::as_str), Some("4"));
}

#[test]
fn test_async_settlement_can_retry_same_token_after_future_cancellation() {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Poll;

    use qubit_event_bus::error::SpiError;
    use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
    use qubit_event_bus::spi::DeliveryDisposition;
    use qubit_event_bus::spi::ReceiveOutcome;
    use qubit_event_bus::spi::SettlementToken;
    use qubit_event_bus::spi::SpiFuture;

    struct Provider {
        state: Arc<Mutex<Option<DeliveryDisposition>>>,
    }

    impl AsyncEventSubscriptionSpi for Provider {
        fn receive<'a>(&'a mut self, _: std::time::Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
            Box::pin(async { Ok(ReceiveOutcome::Closed) })
        }

        fn settle<'a>(
            &'a mut self,
            _token: &SettlementToken,
            disposition: DeliveryDisposition,
        ) -> SpiFuture<'a, Result<(), SpiError>> {
            let state = self.state.clone();
            Box::pin(async move {
                let terminal: Option<Result<(), SpiError>> = {
                    let mut state = state.lock().expect("settlement state lock");
                    match *state {
                        Some(previous) if previous == disposition => Some(Ok(())),
                        Some(_) => Some(Err(SpiError::InvalidSettlementToken {
                            provider_id: "contract-test".into(),
                            operation: "settle",
                            resource: None,
                            reason: "conflicting_disposition",
                            retryable: Some(false),
                            source: Box::new(std::io::Error::other("disposition already fixed")),
                        })),
                        None => {
                            *state = Some(disposition);
                            None
                        }
                    }
                };
                match terminal {
                    Some(result) => result,
                    None => std::future::pending::<Result<(), SpiError>>().await,
                }
            })
        }

        fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
            Box::pin(async { Ok(()) })
        }
    }

    let subscription_id = Id::new(31);
    let token = SettlementToken::new(subscription_id, "retryable-provider-token");
    let mut provider = Provider {
        state: Arc::new(Mutex::new(None)),
    };

    let mut first_attempt = Box::pin(provider.settle(&token, DeliveryDisposition::Accept));
    assert!(matches!(
        support::manual_async::poll_once(first_attempt.as_mut()),
        Poll::Pending
    ));
    drop(first_attempt);

    let mut retry = Box::pin(provider.settle(&token, DeliveryDisposition::Accept));
    assert!(matches!(
        support::manual_async::poll_once(retry.as_mut()),
        Poll::Ready(Ok(()))
    ));
    drop(retry);

    let mut conflict = Box::pin(provider.settle(&token, DeliveryDisposition::Reject));
    let result = match support::manual_async::poll_once(conflict.as_mut()) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("conflicting settlement must fail promptly"),
    };
    let error = result.expect_err("conflicting disposition must be rejected");
    assert_eq!(error.kind(), "invalid_settlement_token");
    assert_eq!(error.operation(), "settle");
}
