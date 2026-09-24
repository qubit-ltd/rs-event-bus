// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

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

    fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(bytes.to_vec()).map_err(|error| CodecError::Encode {
            source: Box::new(error),
        })
    }
}

#[test]
fn topic_codec_metadata_and_publish_options_are_accessible() {
    let codec = StringCodec {
        content_type: ContentType::new("text/plain").expect("valid MIME type"),
        schema_id: SchemaId::new("string-v1").expect("valid schema ID"),
    };
    let topic = Topic::<String>::with_codec("strings", codec).expect("valid topic");
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
fn publish_receipts_and_batches_report_admission_outcomes() {
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
