// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Native payload weight metadata exposed to provider implementations.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::SystemTime;

use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;

/// Direct SPI messages default to no declaration and allow explicit weights.
#[test]
fn test_native_payload_weight_default_and_explicit_value() {
    let message = OutboundMessage::new(
        TopicAddress::new("weights.spi").expect("valid topic"),
        EventId::new("weight-1").expect("valid ID"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(42_u32)),
    );
    assert_eq!(message.native_payload_weight_bytes(), None);
    let weight = NonZeroUsize::new(42).expect("positive weight");
    let message = message.with_native_payload_weight_bytes(weight);
    assert_eq!(message.native_payload_weight_bytes(), Some(weight));
    assert_eq!(message.topic().as_str(), "weights.spi");
}
