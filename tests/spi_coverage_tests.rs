// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public data-model contracts for provider SPI message types.

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderMessageMetadata;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryGap;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::OrderingKey;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;

#[test]
fn test_inbound_message_exposes_encoded_payload_and_all_transport_metadata() {
    let subscription_id = Id::new(101);
    let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(123);
    let mut headers = Headers::new();
    headers.insert("trace-id".into(), "trace-7".into());
    let mut provider_metadata = ProviderMessageMetadata::new();
    provider_metadata.insert("partition".into(), "3".into());
    let encoded = EncodedPayload::new(
        Arc::from([0_u8, 1, 2, 255]),
        ContentType::new("application/octet-stream").expect("valid MIME type"),
        Some(SchemaId::new("schema-v2").expect("valid schema ID")),
    );
    let mut message = InboundMessage::new(
        TopicAddress::new("orders.created").expect("valid topic address"),
        EventId::new("event-inbound-1").expect("valid event ID"),
        timestamp,
        headers,
        Some(OrderingKey::new("customer-42").expect("valid ordering key")),
        TransportPayload::Encoded(encoded),
        Some(SettlementToken::new(subscription_id, 7_u64)),
        provider_metadata,
    );

    assert_eq!(message.topic().as_str(), "orders.created");
    assert_eq!(message.id().as_str(), "event-inbound-1");
    assert_eq!(message.timestamp(), timestamp);
    assert_eq!(message.headers().get("trace-id").map(String::as_str), Some("trace-7"));
    assert_eq!(message.ordering_key().map(OrderingKey::as_str), Some("customer-42"));
    assert_eq!(
        message.provider_metadata().get("partition").map(String::as_str),
        Some("3")
    );
    let TransportPayload::Encoded(payload) = message.payload() else {
        panic!("encoded transport payload should remain encoded");
    };
    assert_eq!(payload.bytes(), &[0, 1, 2, 255]);
    assert_eq!(payload.content_type().as_str(), "application/octet-stream");
    assert_eq!(payload.schema_id().map(SchemaId::as_str), Some("schema-v2"));

    let token = message.take_settlement().expect("message carries settlement token");
    assert!(token.belongs_to(subscription_id));
    assert_eq!(token.downcast_ref::<u64>(), Some(&7));
    assert!(message.take_settlement().is_none());
    assert!(message.settlement().is_none());
}

#[test]
fn test_inbound_message_into_parts_transfers_encoded_payload_and_metadata() {
    let topic = TopicAddress::new("events.archived").expect("valid topic address");
    let event_id = EventId::new("event-archived").expect("valid event ID");
    let timestamp = SystemTime::UNIX_EPOCH;
    let mut headers = Headers::new();
    headers.insert("source".into(), "archive".into());
    let mut metadata = ProviderMessageMetadata::new();
    metadata.insert("offset".into(), "12".into());
    let message = InboundMessage::new(
        topic,
        event_id,
        timestamp,
        headers,
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from([9_u8, 8]),
            ContentType::new("application/x-event").expect("valid MIME type"),
            None,
        )),
        None,
        metadata,
    );

    let (topic, id, received_at, headers, ordering_key, payload, settlement, metadata) = message.into_parts();
    assert_eq!(topic.as_str(), "events.archived");
    assert_eq!(id.as_str(), "event-archived");
    assert_eq!(received_at, timestamp);
    assert_eq!(headers.get("source").map(String::as_str), Some("archive"));
    assert!(ordering_key.is_none());
    assert!(settlement.is_none());
    assert_eq!(metadata.get("offset").map(String::as_str), Some("12"));
    let TransportPayload::Encoded(payload) = payload else {
        panic!("encoded transport payload should transfer through into_parts");
    };
    assert_eq!(payload.bytes(), &[9, 8]);
    assert_eq!(payload.content_type().as_str(), "application/x-event");
    assert!(payload.schema_id().is_none());
}

#[test]
fn test_outbound_message_exposes_transport_fields_and_consumes_payload() {
    let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(9);
    let mut headers = Headers::new();
    headers.insert("content-language".into(), "en".into());
    let delay = Duration::from_millis(250);
    let message = OutboundMessage::new(
        TopicAddress::new("notifications.ready").expect("valid topic address"),
        EventId::new("event-outbound-1").expect("valid event ID"),
        timestamp,
        headers,
        Some(OrderingKey::new("account-5").expect("valid ordering key")),
        Some(delay),
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from([42_u8]),
            ContentType::new("application/cbor").expect("valid MIME type"),
            Some(SchemaId::new("notification-v1").expect("valid schema ID")),
        )),
    );

    assert_eq!(message.topic().as_str(), "notifications.ready");
    assert_eq!(message.id().as_str(), "event-outbound-1");
    assert_eq!(message.timestamp(), timestamp);
    assert_eq!(
        message.headers().get("content-language").map(String::as_str),
        Some("en")
    );
    assert_eq!(message.ordering_key().map(OrderingKey::as_str), Some("account-5"));
    assert_eq!(message.delay(), Some(delay));
    let TransportPayload::Encoded(payload) = message.into_payload() else {
        panic!("outbound payload should be returned by into_payload");
    };
    assert_eq!(payload.bytes(), &[42]);
    assert_eq!(payload.content_type().as_str(), "application/cbor");
    assert_eq!(payload.schema_id().map(SchemaId::as_str), Some("notification-v1"));
}

#[test]
fn test_topic_address_validates_utf8_byte_length_and_whitespace() {
    let max_ascii = "a".repeat(255);
    assert_eq!(
        TopicAddress::new(&max_ascii)
            .expect("255-byte topic address is valid")
            .as_str(),
        max_ascii
    );
    assert!(matches!(
        TopicAddress::new(&"a".repeat(256)),
        Err(ConfigurationError::InvalidField { field: "topic", .. })
    ));
    assert!(matches!(
        TopicAddress::new(""),
        Err(ConfigurationError::InvalidField { field: "topic", .. })
    ));
    assert!(matches!(
        TopicAddress::new(" leading"),
        Err(ConfigurationError::InvalidField { field: "topic", .. })
    ));
    assert!(matches!(
        TopicAddress::new("trailing "),
        Err(ConfigurationError::InvalidField { field: "topic", .. })
    ));
    assert!(matches!(
        TopicAddress::new("line\nbreak"),
        Err(ConfigurationError::InvalidField { field: "topic", .. })
    ));

    assert!(TopicAddress::new(&"é".repeat(127)).is_ok());
    assert!(matches!(
        TopicAddress::new(&"é".repeat(128)),
        Err(ConfigurationError::InvalidField { field: "topic", .. })
    ));
}

#[test]
fn test_settlement_token_binds_identity_and_allows_typed_mutation() {
    let issuing_subscription = Id::new(200);
    let other_subscription = Id::new(201);
    let mut token = SettlementToken::new(issuing_subscription, String::from("offset-4"));

    assert!(token.belongs_to(issuing_subscription));
    assert!(!token.belongs_to(other_subscription));
    assert_eq!(token.downcast_ref::<String>().map(String::as_str), Some("offset-4"));
    assert!(token.downcast_ref::<u64>().is_none());
    token
        .downcast_mut::<String>()
        .expect("matching token state type")
        .push_str("-committed");
    assert_eq!(
        token.downcast_ref::<String>().map(String::as_str),
        Some("offset-4-committed")
    );
}

#[test]
fn test_receive_outcome_represents_gap_timeout_closed_and_message() {
    let gap = DeliveryGap::new("provider retention boundary", Some(6));
    let outcomes = [
        ReceiveOutcome::Gap(gap),
        ReceiveOutcome::TimedOut,
        ReceiveOutcome::Closed,
        ReceiveOutcome::Message(InboundMessage::new(
            TopicAddress::new("events.received").expect("valid topic address"),
            EventId::new("event-received").expect("valid event ID"),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Native(Arc::new(17_u32)),
            None,
            ProviderMessageMetadata::new(),
        )),
    ];

    let mut saw_gap = false;
    let mut saw_timeout = false;
    let mut saw_closed = false;
    let mut saw_message = false;
    for outcome in outcomes {
        match outcome {
            ReceiveOutcome::Gap(gap) => {
                assert_eq!(gap.reason.as_ref(), "provider retention boundary");
                assert_eq!(gap.missed, Some(6));
                saw_gap = true;
            }
            ReceiveOutcome::TimedOut => saw_timeout = true,
            ReceiveOutcome::Closed => saw_closed = true,
            ReceiveOutcome::Message(message) => {
                assert_eq!(message.id().as_str(), "event-received");
                saw_message = true;
            }
            _ => unreachable!("all non-exhaustive receive variants are covered"),
        }
    }
    assert!(saw_gap && saw_timeout && saw_closed && saw_message);
}

struct MinimalAsyncProvider;

impl AsyncEventBusSpi for MinimalAsyncProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        unreachable!("the default provider identity does not require capabilities")
    }

    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        unreachable!("the default provider identity does not publish")
    }

    fn subscribe<'a>(
        &'a self,
        _: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        unreachable!("the default provider identity does not subscribe")
    }

    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        unreachable!("the default provider identity does not shut down")
    }
}

#[test]
fn test_async_spi_provider_id_defaults_to_none_for_unregistered_provider() {
    let provider = MinimalAsyncProvider;

    assert!(provider.provider_id().is_none());
}
