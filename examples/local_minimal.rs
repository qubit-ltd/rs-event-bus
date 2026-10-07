// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Demonstrates local subscription, publication, receipt, cancellation, and
//! graceful shutdown with the synchronous provider.

use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = EventBus::local(LocalEventBusConfig::default())?;
    let topic = Topic::<String>::new("orders.created")?;
    let (sender, receiver) = mpsc::channel();
    let subscription = bus.subscribe(SubscribeRequest::new("audit", topic.clone())?, move |delivery| {
        let _ = sender.send(delivery.payload().clone());
    })?;

    let _ = bus.publish_checked(
        PublishRequest::new(topic, "order-42".to_owned())?,
        AdmissionRequirement::ProviderOrDestinationAccepted,
    )?;
    assert_eq!(receiver.recv_timeout(Duration::from_secs(2))?, "order-42");
    subscription.cancel()?;
    let report = bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(2),
    })?;
    assert_eq!(report.known_abandoned_deliveries, 0);
    Ok(())
}
