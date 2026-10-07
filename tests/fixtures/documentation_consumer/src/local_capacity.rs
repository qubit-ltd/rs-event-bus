// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Compiled local-provider capacity example used by both user guides.

use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

/// Publishes one weighed `String`, waits for its handler, and closes the bus.
///
/// # Errors
/// Returns an error if bus construction, subscription, admission, delivery wait,
/// or graceful shutdown fails.
pub fn publish_with_local_capacity() -> Result<(), Box<dyn std::error::Error>> {
    let budget = NonZeroUsize::new(8 * 1024 * 1024).expect("positive weight budget");
    let local = LocalEventBusConfig::new()
        .queue_capacity(2_048)
        .max_total_outstanding(20_000)
        .max_total_outstanding_weight_bytes(budget);
    let bus = EventBus::local(local)?;
    let topic = Topic::<String>::new("orders.created")?;
    let (sender, receiver) = mpsc::channel();
    let _subscription = bus.subscribe(
        SubscribeRequest::new("audit", topic.clone())?,
        move |delivery| {
            sender.send(delivery.payload().clone()).expect("receiver remains open");
        },
    )?;
    let options = PublishOptions::<String>::builder()
        .native_payload_weight(|payload| {
            NonZeroUsize::new(payload.len().max(1)).expect("positive payload weight")
        })
        .build();
    let request = PublishRequest::new(topic, "order-42".to_owned())?.with_options(options);
    let _receipt = bus.publish_checked(request, AdmissionRequirement::AtLeastOneAccepted)?;
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(3))?,
        "order-42"
    );
    let shutdown = bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(3),
    })?;
    assert_eq!(shutdown.outcome, ShutdownOutcome::Complete);
    Ok(())
}
