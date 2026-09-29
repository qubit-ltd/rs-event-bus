// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================





use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = AsyncEventBus::local(LocalEventBusConfig::new()).await?;
    let topic = Topic::<String>::new("orders.created")?;
    let mut subscription = bus
        .subscribe(SubscribeRequest::new("audit", topic.clone())?)
        .await?;
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let runner = tokio::spawn(async move {
        subscription
            .run(move |delivery| {
                let sender = sender.clone();
                async move {
                    sender.send(delivery.payload().clone()).unwrap();
                    Ok(())
                }
            })
            .await
    });
    bus.publish(PublishRequest::new(topic, "order-42".to_owned())?)
        .await?;
    let delivered = tokio::time::timeout(Duration::from_secs(3), receiver.recv()).await?;
    assert_eq!(delivered.as_deref(), Some("order-42"));
    bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(3),
    })
    .await?;
    runner.await??;
    Ok(())
}
