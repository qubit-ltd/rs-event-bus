// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::any::TypeId;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::SystemTime;

use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::AdmissionRejectionKind;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use qubit_spi::ServiceProvider;

fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("operation must complete immediately"),
    }
}

fn topic() -> TopicAddress {
    TopicAddress::new("admission.rejection.kind").unwrap()
}
fn msg(name: &str, weight: usize) -> OutboundMessage {
    OutboundMessage::new(
        topic(),
        EventId::new(name).unwrap(),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
        None,
        TransportPayload::Native(Arc::new(1_u32)),
    )
    .with_native_payload_weight_bytes(NonZeroUsize::new(weight).unwrap())
}

#[test]
fn old_and_new_admission_constructors_preserve_reason_and_status() {
    let id = Id::new(1);
    let subscriber = SubscriberId::new("compat").unwrap();
    let old = DestinationAdmission::new(
        id,
        subscriber.clone(),
        AdmissionStatus::Rejected("legacy reason".into()),
    );
    assert_eq!(old.rejection_kind(), None);
    assert_eq!(old.status(), &AdmissionStatus::Rejected("legacy reason".into()));
    let new = DestinationAdmission::rejected(
        id,
        subscriber,
        AdmissionRejectionKind::SubscriptionClosed,
        "closed reason",
    );
    assert_eq!(new.rejection_kind(), Some(AdmissionRejectionKind::SubscriptionClosed));
    assert_eq!(new.status(), &AdmissionStatus::Rejected("closed reason".into()));
}

#[test]
fn local_sync_and_async_rejections_have_stable_categories() {
    for asynchronous in [false, true] {
        for (label, queue_capacity, global_count, weight_limit, declared, expected) in [
            ("queue", 1, 10, None, 1, AdmissionRejectionKind::SubscriptionCapacity),
            ("count", 10, 1, None, 1, AdmissionRejectionKind::ProviderCountCapacity),
            (
                "weight",
                10,
                10,
                Some(3),
                2,
                AdmissionRejectionKind::ProviderWeightCapacity,
            ),
        ] {
            let mut config = LocalEventBusConfig::new()
                .queue_capacity(queue_capacity)
                .max_total_outstanding(global_count);
            if let Some(limit) = weight_limit {
                config = config.max_total_outstanding_weight_bytes(NonZeroUsize::new(limit).unwrap());
            }
            let sync = if asynchronous {
                None
            } else {
                Some(
                    LocalEventBusProvider
                        .create_configured(&EventBusConfig::default().with_provider_options(config.provider_options()))
                        .unwrap(),
                )
            };
            let async_bus = if asynchronous {
                Some(AsyncLocalEventBusSpi::new(&config).unwrap())
            } else {
                None
            };
            let request = || {
                SpiSubscriptionRequest::new(
                    Id::new(1),
                    topic(),
                    SubscriberId::new(label).unwrap(),
                    None,
                    SubscriptionDurability::Ephemeral,
                    StartPosition::New,
                    ProviderOptions::new(),
                    TypeId::of::<u32>(),
                )
            };
            let _sync_receiver = sync.as_ref().map(|bus| bus.subscribe(request()).unwrap());
            let _async_receiver = async_bus.as_ref().map(|bus| ready(bus.subscribe(request())).unwrap());
            let publish = |name| {
                if let Some(bus) = &sync {
                    bus.publish(msg(name, declared)).unwrap()
                } else {
                    ready(async_bus.as_ref().unwrap().publish(msg(name, declared))).unwrap()
                }
            };
            let _first = publish("first");
            let second = publish("second");
            let PublishAcknowledgement::DestinationAdmissions(items) = second else {
                panic!("destination admissions")
            };
            let rejected = items
                .iter()
                .find(|item| matches!(item.status(), AdmissionStatus::Rejected(_)))
                .unwrap_or_else(|| panic!("second rejected: {label}, async={asynchronous}, items={items:?}"));
            assert_eq!(
                rejected.rejection_kind(),
                Some(expected),
                "{label}, async={asynchronous}"
            );
            let AdmissionStatus::Rejected(reason) = rejected.status() else {
                unreachable!()
            };
            assert_eq!(
                reason.as_ref(),
                match expected {
                    AdmissionRejectionKind::SubscriptionCapacity => "subscription queue is full",
                    _ => "provider outstanding capacity is full",
                }
            );
        }
    }
}
