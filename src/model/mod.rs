// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated identifiers and type-safe event bus values.

mod ack_mode;
mod acknowledgement;
mod acknowledgement_error;
mod acknowledgement_state;
mod admission_check_error;
mod admission_outcome;
mod admission_requirement;
mod admission_status;
mod admission_summary;
mod batch_publish_result;
mod consumer_group;
mod content_type;
mod dead_letter_admission_policy;
mod dead_letter_event;
mod dead_letter_policy;
mod delivery;
mod delivery_context;
mod destination_admission;
mod event_envelope;
mod event_id;
mod failure_directive;
mod ordering_policy;
mod provider_id;
mod publish_acknowledgement;
mod publish_failure_context;
mod publish_metadata;
mod publish_options;
mod publish_options_builder;
mod publish_receipt;
mod publish_request;
mod publish_request_build_error;
mod publish_request_builder;
mod schema_id;
mod start_position;
mod subscribe_options;
mod subscribe_options_builder;
mod subscribe_request;
mod subscribe_request_build_error;
mod subscribe_request_builder;
mod subscriber_id;
mod subscription_durability;
mod topic;

pub use ack_mode::AckMode;
pub use acknowledgement::Acknowledgement;
pub use acknowledgement_error::AcknowledgementError;
pub use acknowledgement_state::AcknowledgementState;
pub use admission_check_error::AdmissionCheckError;
pub use admission_outcome::AdmissionOutcome;
pub use admission_requirement::AdmissionRequirement;
pub use admission_status::AdmissionStatus;
pub use admission_summary::AdmissionSummary;
pub use batch_publish_result::BatchPublishResult;
pub use consumer_group::ConsumerGroup;
pub use content_type::ContentType;
pub use dead_letter_admission_policy::DeadLetterAdmissionPolicy;
pub use dead_letter_event::DeadLetterEvent;
pub use dead_letter_policy::DeadLetterPolicy;
pub use delivery::Delivery;
pub use delivery_context::DeliveryContext;
pub use destination_admission::DestinationAdmission;
pub use event_envelope::DEAD_LETTER_HEADER;
pub use event_envelope::DEAD_LETTER_HEADER_VALUE;
pub use event_envelope::EventEnvelope;
pub use event_envelope::Headers;
pub use event_id::EventId;
pub use failure_directive::FailureDirective;
pub use ordering_policy::OrderingPolicy;
pub use provider_id::ProviderId;
pub use publish_acknowledgement::ProviderMessageMetadata;
pub use publish_acknowledgement::PublishAcknowledgement;
pub use publish_failure_context::PublishFailureContext;
pub use publish_metadata::PublishMetadata;
pub use publish_options::PublishErrorHandler;
pub use publish_options::PublishOptions;
pub use publish_options::PublisherInterceptor;
pub use publish_options_builder::PublishOptionsBuilder;
pub use publish_receipt::PublishReceipt;
pub use publish_request::PublishRequest;
pub use publish_request_build_error::PublishRequestBuildError;
pub use publish_request_builder::PublishRequestBuilder;
pub use schema_id::SchemaId;
pub use start_position::StartPosition;
pub use subscribe_options::AsyncSubscriberInterceptor;
pub use subscribe_options::AsyncSubscriberNext;
pub use subscribe_options::EventFilter;
pub use subscribe_options::ProviderOptions;
pub use subscribe_options::SubscribeErrorHandler;
pub use subscribe_options::SubscribeOptions;
pub use subscribe_options::SubscriberInterceptor;
pub use subscribe_options::SubscriberNext;
pub use subscribe_options_builder::SubscribeOptionsBuilder;
pub use subscribe_request::SubscribeRequest;
pub use subscribe_request_build_error::SubscribeRequestBuildError;
pub use subscribe_request_builder::SubscribeRequestBuilder;
pub use subscriber_id::SubscriberId;
pub use subscription_durability::SubscriptionDurability;
pub use topic::Topic;
