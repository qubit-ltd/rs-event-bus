// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission rejection diagnostics retain the rejected subscription instance.

use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::pipeline::Diagnostic;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_id::Id;

#[test]
fn test_async_rejection_diagnostic_identifies_rejected_instance() {
    use support::manual_async::block_on;

    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::new().queue_capacity(1)))
        .expect("local async bus should be created");
    let topic = Topic::<String>::new("admission.identity").expect("valid topic");
    let first = block_on(bus.subscribe(SubscribeRequest::new("same-name", topic.clone()).unwrap()))
        .expect("first subscription should be created");
    let first_id = first.id();
    let first_receipt = block_on(bus.publish(PublishRequest::new(topic.clone(), "fill".to_owned()).unwrap()))
        .expect("first publication should succeed");
    assert!(matches!(
        first_receipt.acknowledgement(),
        PublishAcknowledgement::DestinationAdmissions(admissions)
            if admissions.iter().any(|admission| admission.subscription_id() == first_id
                && matches!(admission.status(), AdmissionStatus::Accepted))
    ));

    let second = block_on(bus.subscribe(SubscribeRequest::new("same-name", topic.clone()).unwrap()))
        .expect("second subscription should be created");
    let second_id = second.id();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let captured = observed.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::AdmissionRejected { subscription_id, .. } = diagnostic {
            captured.lock().unwrap().push(*subscription_id);
        }
    });
    let receipt = block_on(bus.publish(PublishRequest::new(topic, "second".to_owned()).unwrap()))
        .expect("second publication should return a receipt");
    let PublishAcknowledgement::DestinationAdmissions(admissions) = receipt.acknowledgement() else {
        panic!("local provider exposes per-destination admissions");
    };
    assert!(
        admissions
            .iter()
            .any(|item| item.subscription_id() == first_id && matches!(item.status(), AdmissionStatus::Rejected(_)))
    );
    assert!(
        admissions
            .iter()
            .any(|item| item.subscription_id() == second_id && matches!(item.status(), AdmissionStatus::Accepted))
    );
    assert_eq!(*observed.lock().unwrap(), vec![first_id]);
    drop((first, second));
}

struct FixedAdmissionSpi;

impl EventBusSpi for FixedAdmissionSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::DestinationAdmissions,
        )
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        Ok(PublishAcknowledgement::DestinationAdmissions(vec![
            DestinationAdmission::new(
                Id::new(11),
                SubscriberId::new("shared").unwrap(),
                AdmissionStatus::Rejected("full".into()),
            ),
            DestinationAdmission::new(
                Id::new(12),
                SubscriberId::new("shared").unwrap(),
                AdmissionStatus::Accepted,
            ),
        ]))
    }

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Err(unsupported("subscribe"))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

fn unsupported(operation: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: "identity-test".into(),
        operation,
        resource: None,
        kind: "unsupported",
        retryable: Some(false),
        source: Box::new(IoError::other("unsupported in identity test")),
    }
}

#[test]
fn test_sync_rejection_diagnostic_uses_destination_admission_identity() {
    let bus = EventBus::from_spi(ProviderId::new("identity-test").unwrap(), Arc::new(FixedAdmissionSpi))
        .expect("provider capabilities should be valid");
    let topic = Topic::<u32>::new("admission.identity.sync").unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let captured = observed.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::AdmissionRejected { subscription_id, .. } = diagnostic {
            captured.lock().unwrap().push(*subscription_id);
        }
    });
    let receipt = bus.publish(PublishRequest::new(topic, 1).unwrap()).unwrap();
    assert_eq!(*observed.lock().unwrap(), vec![Id::new(11)]);
    assert!(
        matches!(receipt.acknowledgement(), PublishAcknowledgement::DestinationAdmissions(items)
        if items[0].subscription_id() == Id::new(11)
            && items[1].subscription_id() == Id::new(12))
    );
}

mod support {
    pub(crate) mod manual_async {
        use std::future::Future;
        use std::task::Waker;

        pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
            let mut context = std::task::Context::from_waker(Waker::noop());
            let mut future = Box::pin(future);
            loop {
                if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut context) {
                    return value;
                }
                std::thread::yield_now();
            }
        }
    }
}
