// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Object-safe backend service provider interfaces and transport contracts.

#[cfg(feature = "conformance")]
pub mod conformance;

mod async_event_bus_spi;
mod async_event_subscription_spi;
mod delayed_delivery_capability;
mod delivery_disposition;
mod delivery_gap;
mod durability_capability;
mod encoded_payload;
mod event_bus_capabilities;
mod event_bus_spi;
mod event_subscription_spi;
mod inbound_message;
mod ordering_capability;
mod ordering_key;
mod outbound_message;
mod payload_modes;
mod publish_guarantee;
mod publish_visibility;
mod receive_outcome;
mod replay_capability;
mod settlement_capabilities;
mod settlement_token;
mod shutdown_mode;
mod shutdown_outcome;
mod spi_future;
mod spi_subscription_request;
mod topic_address;
mod transport_payload;

pub use async_event_bus_spi::AsyncEventBusSpi;
pub use async_event_subscription_spi::AsyncEventSubscriptionSpi;
pub use delayed_delivery_capability::DelayedDeliveryCapability;
pub use delivery_disposition::DeliveryDisposition;
pub use delivery_gap::DeliveryGap;
pub use durability_capability::DurabilityCapability;
pub use encoded_payload::EncodedPayload;
pub use event_bus_capabilities::EventBusCapabilities;
pub use event_bus_spi::EventBusSpi;
pub use event_subscription_spi::EventSubscriptionSpi;
pub use inbound_message::InboundMessage;
pub use ordering_capability::OrderingCapability;
pub use ordering_key::OrderingKey;
pub use outbound_message::OutboundMessage;
pub use payload_modes::PayloadModes;
pub use publish_guarantee::PublishGuarantee;
pub use publish_visibility::PublishVisibility;
pub use receive_outcome::ReceiveOutcome;
pub use replay_capability::ReplayCapability;
pub use settlement_capabilities::SettlementCapabilities;
pub use settlement_token::SettlementToken;
pub use shutdown_mode::ShutdownMode;
pub use shutdown_outcome::ShutdownOutcome;
pub use spi_future::SpiFuture;
pub use spi_subscription_request::SpiSubscriptionRequest;
pub use topic_address::TopicAddress;
pub use transport_payload::TransportPayload;
