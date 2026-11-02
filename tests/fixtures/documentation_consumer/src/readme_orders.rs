// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;

// Published after the order transaction commits.
pub struct OrderCreated {
    pub order_id: String,
    pub customer_id: String,
    pub total_cents: u64,
}

impl OrderCreated {
    pub const TOPIC_CREATED: Topic<Self> = Topic::new_static("orders.created");
}

// The application supplies implementations backed by its audit and view stores.
pub trait AuditLog: Send + Sync {
    fn append_order_created(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}
pub trait CustomerOrderView: Send + Sync {
    fn upsert_order(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}

// Called during application startup; retain the returned subscription.
pub fn subscribe_audit(bus: &EventBus, audit: Arc<dyn AuditLog>) -> Result<Subscription, Box<dyn std::error::Error>> {
    let topic = OrderCreated::TOPIC_CREATED;
    let request = SubscribeRequest::new("audit-log", topic)?;
    Ok(bus.subscribe(request, move |delivery| audit.append_order_created(delivery.payload()))?)
}

pub fn subscribe_customer_view(
    bus: &EventBus,
    view: Arc<dyn CustomerOrderView>,
) -> Result<Subscription, Box<dyn std::error::Error>> {
    let topic = OrderCreated::TOPIC_CREATED;
    let request = SubscribeRequest::new("customer-view", topic)?;
    Ok(bus.subscribe(request, move |delivery| view.upsert_order(delivery.payload()))?)
}

// Called by the order service only after its database commit succeeds.
pub fn publish_order_created(
    bus: &EventBus,
    order_id: String,
    customer_id: String,
    total_cents: u64,
) -> Result<PublishReceipt, Box<dyn std::error::Error>> {
    let topic = OrderCreated::TOPIC_CREATED;
    let event = OrderCreated {
        order_id,
        customer_id,
        total_cents,
    };
    let receipt = bus.publish(PublishRequest::new(topic, event)?)?;
    Ok(receipt)
}
