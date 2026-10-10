// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod support;

use std::sync::Arc;

use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CapabilityError;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::facade::EventBus;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::SubscriptionModes;
use support::fake_spi::FakeAsyncEventBusSpi;
use support::fake_spi::FakeEventBusSpi;

struct StringCodec;

static CONTENT_TYPE: ContentType = ContentType::new_static("text/plain");

impl EventCodec<String> for StringCodec {
    fn content_type(&self) -> &ContentType {
        &CONTENT_TYPE
    }
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }
    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

fn capabilities(modes: PayloadModes) -> EventBusCapabilities {
    EventBusCapabilities::new(
        modes,
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
}

#[test]
fn test_sync_check_codec_is_a_pure_lookup_for_all_payload_modes() {
    for (index, modes) in [
        PayloadModes::Native,
        PayloadModes::Encoded,
        PayloadModes::NativeAndEncoded,
    ]
    .into_iter()
    .enumerate()
    {
        let spi = Arc::new(FakeEventBusSpi::with_capabilities(capabilities(modes)));
        let bus = EventBus::from_spi(
            ProviderId::new(&format!("codec-check-sync-{index}")).unwrap(),
            spi.clone(),
        )
        .unwrap();
        let plain = Topic::<String>::new("orders.created").unwrap();
        let with_topic_codec = plain.clone().with_codec(StringCodec);

        let before_calls = spi.operation_log();
        let before_metrics = bus.publish_metrics();
        let missing = bus.check_publish_codec(&plain);
        assert_eq!(missing.is_ok(), modes != PayloadModes::Encoded);
        if modes == PayloadModes::Encoded {
            assert!(matches!(missing, Err(CapabilityError::CodecRequired)));
        }
        assert!(bus.check_publish_codec(&with_topic_codec).is_ok());

        let mut registry = CodecRegistry::new();
        registry.register::<String>(Arc::new(StringCodec)).unwrap();
        let configured = EventBus::with_config(
            ProviderId::new(&format!("codec-check-registry-{index}")).unwrap(),
            spi.clone(),
            EventBusFacadeConfig::new().with_codec_registry(Arc::new(registry)),
        )
        .unwrap();
        assert!(configured.check_publish_codec(&plain).is_ok());
        assert_eq!(spi.operation_log(), before_calls);
        assert_eq!(bus.publish_metrics().attempts, before_metrics.attempts);
        assert_eq!(bus.publish_metrics().attempts, 0);
    }
}

#[test]
fn test_async_check_codec_is_a_pure_lookup_for_all_payload_modes() {
    for (index, modes) in [
        PayloadModes::Native,
        PayloadModes::Encoded,
        PayloadModes::NativeAndEncoded,
    ]
    .into_iter()
    .enumerate()
    {
        let spi = Arc::new(FakeAsyncEventBusSpi::with_capabilities(capabilities(modes)));
        let bus = AsyncEventBus::from_spi(
            ProviderId::new(&format!("codec-check-async-{index}")).unwrap(),
            spi.clone(),
        )
        .unwrap();
        let plain = Topic::<String>::new("orders.created").unwrap();
        let with_topic_codec = plain.clone().with_codec(StringCodec);

        let before_calls = spi.operation_log();
        let before_metrics = bus.publish_metrics();
        let missing = bus.check_publish_codec(&plain);
        assert_eq!(missing.is_ok(), modes != PayloadModes::Encoded);
        if modes == PayloadModes::Encoded {
            assert!(matches!(missing, Err(CapabilityError::CodecRequired)));
        }
        assert!(bus.check_publish_codec(&with_topic_codec).is_ok());

        let mut registry = CodecRegistry::new();
        registry.register::<String>(Arc::new(StringCodec)).unwrap();
        let configured = AsyncEventBus::with_config(
            ProviderId::new(&format!("codec-check-async-registry-{index}")).unwrap(),
            spi.clone(),
            EventBusFacadeConfig::new().with_codec_registry(Arc::new(registry)),
        )
        .unwrap();
        assert!(configured.check_publish_codec(&plain).is_ok());
        assert_eq!(spi.operation_log(), before_calls);
        assert_eq!(bus.publish_metrics().attempts, before_metrics.attempts);
        assert_eq!(bus.publish_metrics().attempts, 0);
    }
}
