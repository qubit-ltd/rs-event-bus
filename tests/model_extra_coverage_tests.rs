// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use qubit_event_bus::SubscriberId;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::model::BatchPublishResult;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::EncodedPayload;

const STATIC_SUBSCRIBER_ID: SubscriberId = SubscriberId::new_static("audit-static");
const STATIC_PROVIDER_ID: ProviderId = ProviderId::new_static("local-static");
const STATIC_SCHEMA_ID: SchemaId = SchemaId::new_static("schema-static-v1");
const STATIC_CONTENT_TYPE: ContentType = ContentType::new_static("application/json");

fn identifier_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn test_static_identifier_constants_match_runtime_identifiers() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(SubscriberId::new_static("audit-static"), STATIC_SUBSCRIBER_ID);
    assert_eq!(ProviderId::new_static("local-static"), STATIC_PROVIDER_ID);
    assert_eq!(SchemaId::new_static("schema-static-v1"), STATIC_SCHEMA_ID);
    assert_eq!(STATIC_SUBSCRIBER_ID.as_str(), "audit-static");
    let runtime_subscriber_id = SubscriberId::new("audit-static")?;
    assert_eq!(STATIC_SUBSCRIBER_ID, runtime_subscriber_id);
    assert_eq!(
        identifier_hash(&STATIC_SUBSCRIBER_ID),
        identifier_hash(&runtime_subscriber_id)
    );
    assert_eq!(STATIC_PROVIDER_ID.as_str(), "local-static");
    let runtime_provider_id = ProviderId::new("local-static")?;
    assert_eq!(STATIC_PROVIDER_ID, runtime_provider_id);
    assert_eq!(
        identifier_hash(&STATIC_PROVIDER_ID),
        identifier_hash(&runtime_provider_id)
    );
    assert_eq!(STATIC_SCHEMA_ID.as_str(), "schema-static-v1");
    let runtime_schema_id = SchemaId::new("schema-static-v1")?;
    assert_eq!(STATIC_SCHEMA_ID, runtime_schema_id);
    assert_eq!(identifier_hash(&STATIC_SCHEMA_ID), identifier_hash(&runtime_schema_id));
    Ok(())
}

#[test]
fn test_static_content_type_matches_runtime_value_and_predefined_constants() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(ContentType::new_static("application/json"), STATIC_CONTENT_TYPE);
    assert_eq!(STATIC_CONTENT_TYPE.as_str(), "application/json");
    let runtime_content_type = ContentType::new("application/json")?;
    assert_eq!(STATIC_CONTENT_TYPE, runtime_content_type);
    assert_eq!(
        identifier_hash(&STATIC_CONTENT_TYPE),
        identifier_hash(&runtime_content_type)
    );
    assert_eq!(ContentType::TEXT_PLAIN.as_str(), "text/plain");
    assert_eq!(ContentType::TEXT_PLAIN, ContentType::new("text/plain")?);
    assert_eq!(ContentType::TEXT_HTML.as_str(), "text/html");
    assert_eq!(ContentType::TEXT_CSV.as_str(), "text/csv");
    assert_eq!(ContentType::TEXT_XML.as_str(), "text/xml");
    assert_eq!(ContentType::APPLICATION_JSON, STATIC_CONTENT_TYPE);
    assert_eq!(ContentType::APPLICATION_XML.as_str(), "application/xml");
    assert_eq!(
        ContentType::APPLICATION_OCTET_STREAM.as_str(),
        "application/octet-stream"
    );
    assert_eq!(ContentType::APPLICATION_CBOR.as_str(), "application/cbor");
    assert_eq!(ContentType::APPLICATION_PROTOBUF.as_str(), "application/protobuf");
    Ok(())
}

#[test]
#[should_panic(expected = "invalid content type")]
fn test_content_type_new_static_rejects_invalid_value() {
    let _ = ContentType::new_static("text/");
}

struct StringCodec {
    content_type: ContentType,
    schema_id: SchemaId,
}

impl EventCodec<String> for StringCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        Some(&self.schema_id)
    }

    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        let bytes = payload.bytes();
        String::from_utf8(bytes.to_vec()).map_err(|error| CodecError::Encode {
            source: Box::new(error),
        })
    }
}

#[test]
fn test_topic_codec_metadata_and_publish_options_are_accessible() {
    let codec = StringCodec {
        content_type: ContentType::new("text/plain").expect("valid MIME type"),
        schema_id: SchemaId::new("string-v1").expect("valid schema ID"),
    };
    let topic = Topic::<String>::new_with_codec("strings", codec).expect("valid topic");
    assert_eq!(topic.name(), "strings");
    assert_eq!(topic.payload_type_id(), std::any::TypeId::of::<String>());
    assert_eq!(topic.payload_type_name(), std::any::type_name::<String>());
    assert_eq!(topic.schema_id().map(SchemaId::as_str), Some("string-v1"));
    assert_eq!(topic.codec().expect("codec").content_type().as_str(), "text/plain");
    assert_eq!(topic.clone(), topic);
    assert_eq!(topic, topic.clone());
    assert!(format!("{topic:?}").contains("strings"));

    let defaults = PublishOptions::<String>::new();
    assert!(defaults.retry_policy().is_none());
    assert!(defaults.retry_rule().is_none());
    assert!(defaults.retry_cancellation_token().is_none());
    assert!(defaults.error_handlers().is_empty());
    assert_eq!(defaults.error_handler_count(), 0);
    assert!(defaults.interceptors().is_empty());
    let configured = PublishOptions::<String>::builder()
        .error_handler(|_, _| {})
        .interceptor(|event| Ok(Some(event)))
        .build();
    assert_eq!(configured.error_handler_count(), 1);
    assert_eq!(configured.interceptors().len(), 1);
}

#[test]
fn test_publish_receipts_and_batches_report_admission_outcomes() {
    let provider_id = ProviderId::new("local").expect("valid provider ID");
    let receipt = PublishReceipt::new(
        EventId::new("input").expect("valid event ID"),
        Some(EventId::new("dispatched").expect("valid event ID")),
        provider_id,
        PublishAcknowledgement::Accepted {
            provider_message_id: Some("message-1".into()),
            metadata: Default::default(),
        },
    );
    assert_eq!(receipt.input_event_id().as_str(), "input");
    assert_eq!(receipt.dispatched_event_id().map(EventId::as_str), Some("dispatched"));
    assert_eq!(receipt.provider_id().as_str(), "local");
    assert!(matches!(
        receipt.acknowledgement(),
        PublishAcknowledgement::Accepted { .. }
    ));

    let dropped = PublishReceipt::new(
        EventId::new("dropped").expect("valid event ID"),
        None,
        ProviderId::new("local").expect("valid provider ID"),
        PublishAcknowledgement::DroppedByInterceptor,
    );
    let batch = BatchPublishResult::new(vec![Ok(receipt), Ok(dropped)]);
    assert_eq!(batch.total_count(), 2);
    assert_eq!(batch.accepted_count(), 1);
    assert_eq!(batch.dropped_count(), 1);
    assert_eq!(batch.failure_count(), 0);
    assert_eq!(batch.into_items().len(), 2);
}
