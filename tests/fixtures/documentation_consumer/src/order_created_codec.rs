// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Order-event codec compiled from the bilingual user guides.

use std::io::Error;
use std::str::from_utf8;
use std::sync::Arc;

use qubit_event_bus::CodecError;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;

use crate::orders::events::OrderCreated;

/// Encodes and decodes the `OrderCreated` event for the documentation fixture.
pub struct OrderCreatedCodec(
    /// Content type advertised for encoded order events.
    pub ContentType,
);

impl EventCodec<OrderCreated> for OrderCreatedCodec {
    fn content_type(&self) -> &ContentType {
        &self.0
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &OrderCreated) -> Result<Arc<[u8]>, CodecError> {
        let text = format!(
            "{}\n{}\n{}",
            value.order_id, value.customer_id, value.total_cents
        );
        Ok(Arc::from(text.into_bytes()))
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<OrderCreated, CodecError> {
        let text = from_utf8(payload.bytes()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })?;
        let mut lines = text.lines();
        let order_id = lines.next().unwrap_or("").to_owned();
        let customer_id = lines.next().unwrap_or("").to_owned();
        let total_cents = lines
            .next()
            .unwrap_or("")
            .parse::<u64>()
            .map_err(|source| CodecError::Decode {
                source: Box::new(source),
            })?;
        if lines.next().is_some() || order_id.is_empty() || customer_id.is_empty() {
            return Err(CodecError::Decode {
                source: Box::new(Error::other(
                    "expected order_id, customer_id, and total_cents",
                )),
            });
        }
        Ok(OrderCreated {
            order_id,
            customer_id,
            total_cents,
        })
    }
}
