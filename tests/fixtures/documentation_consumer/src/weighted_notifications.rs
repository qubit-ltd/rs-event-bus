// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::io;
use std::num::NonZeroUsize;

use qubit_event_bus::EventBus;
use qubit_event_bus::NotificationOutcome;
use qubit_event_bus::NotificationRejectionReason;
use qubit_event_bus::TryPublishError;
use qubit_event_bus::WeightedNotificationPublisher;
use qubit_event_bus::model::Topic;

use crate::orders::events::OrderCreated;

pub fn create_weighted_notifier(bus: EventBus) -> io::Result<WeightedNotificationPublisher<OrderCreated>> {
    let topic = Topic::<OrderCreated>::new_static("orders.created");
    WeightedNotificationPublisher::new(
        bus,
        topic,
        NonZeroUsize::new(256).expect("item capacity is non-zero"),
        NonZeroUsize::new(64 * 1024).expect("weight limit is non-zero"),
        |outcome| match outcome {
            NotificationOutcome::Published(receipt) => {
                eprintln!("order notification receipt: {receipt:?}");
            }
            NotificationOutcome::PublishFailed(error) => {
                eprintln!("order notification publish failed: {error}");
            }
            NotificationOutcome::RequestFailed(error) => {
                eprintln!("order notification request failed: {error}");
            }
            _ => {}
        },
    )
}

pub fn enqueue_order(
    notifier: &WeightedNotificationPublisher<OrderCreated>,
    event: OrderCreated,
) -> Result<(), TryPublishError<OrderCreated>> {
    let weight = NonZeroUsize::new(event.order_id.len().max(1)).expect("weight is non-zero");
    notifier.try_publish(event, weight)
}

pub fn enqueue_order_detailed(
    notifier: &WeightedNotificationPublisher<OrderCreated>,
    event: OrderCreated,
) -> Result<(), (NotificationRejectionReason, OrderCreated)> {
    let weight = NonZeroUsize::new(event.order_id.len().max(1)).expect("weight is non-zero");
    match notifier.try_publish_detailed(event, weight) {
        Ok(()) => Ok(()),
        Err(error) => {
            let reason = error.reason();
            let event = error.into_payload();
            match reason {
                NotificationRejectionReason::QueueCapacity | NotificationRejectionReason::WeightCapacity => {
                    eprintln!("order notification capacity is temporarily unavailable");
                }
                NotificationRejectionReason::OversizedWeight => {
                    eprintln!("order notification exceeds the configured weight budget");
                }
                NotificationRejectionReason::Closed => {
                    eprintln!("order notification publisher is closed");
                }
                _ => eprintln!("order notification admission failed"),
            }
            Err((reason, event))
        }
    }
}
