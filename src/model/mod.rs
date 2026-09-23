// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated identifiers and type-safe event bus values.

mod acknowledgement;
mod batch_publish_result;
mod delivery;
mod destination_admission;
mod event_envelope;
mod event_id;
mod publish_acknowledgement;
mod publish_options;
mod publish_receipt;
mod publish_request;
mod publish_request_builder;
mod subscribe_options;
mod subscribe_request;
mod subscribe_request_builder;
mod subscriber_id;
mod topic;

pub use acknowledgement::Acknowledgement;
pub use acknowledgement::AcknowledgementError;
pub use acknowledgement::AcknowledgementState;
pub use batch_publish_result::BatchPublishResult;
pub use delivery::Delivery;
pub use delivery::DeliveryContext;
pub use destination_admission::AdmissionStatus;
pub use destination_admission::DestinationAdmission;
pub use event_envelope::EventEnvelope;
pub use event_envelope::Headers;
pub use event_id::EventId;
pub use publish_acknowledgement::ProviderMessageMetadata;
pub use publish_acknowledgement::PublishAcknowledgement;
pub use publish_options::FailureDirective;
pub use publish_options::PublishErrorHandler;
pub use publish_options::PublishOptions;
pub use publish_options::PublishOptionsBuilder;
pub use publish_options::PublisherInterceptor;
pub use publish_receipt::ProviderId;
pub use publish_receipt::PublishReceipt;
pub use publish_request::PublishRequest;
pub use publish_request_builder::PublishRequestBuildError;
pub use publish_request_builder::PublishRequestBuilder;
pub use subscribe_options::AckMode;
pub use subscribe_options::ConsumerGroup;
pub use subscribe_options::DeadLetterPolicy;
pub use subscribe_options::EventFilter;
pub use subscribe_options::OrderingPolicy;
pub use subscribe_options::ProviderOptions;
pub use subscribe_options::StartPosition;
pub use subscribe_options::SubscribeErrorHandler;
pub use subscribe_options::SubscribeOptions;
pub use subscribe_options::SubscribeOptionsBuilder;
pub use subscribe_options::SubscriberInterceptor;
pub use subscribe_options::SubscriptionDurability;
pub use subscribe_request::SubscribeRequest;
pub use subscribe_request_builder::SubscribeRequestBuildError;
pub use subscribe_request_builder::SubscribeRequestBuilder;
pub use subscriber_id::SubscriberId;
pub use topic::ContentType;
pub use topic::SchemaId;
pub use topic::Topic;
