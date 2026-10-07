// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Real local SPI lifecycle race contracts.
//!
//! Instrumented production admission and budget models live beside the private
//! primitives; these tests retain the externally observable provider races.

#[cfg(not(loom))]
mod local_spi_contract {
    use std::any::TypeId;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;
    use std::time::SystemTime;

    use qubit_event_bus::EventBusConfig;
    use qubit_event_bus::error::SpiError;
    use qubit_event_bus::local::LocalEventBusConfig;
    use qubit_event_bus::local::LocalEventBusProvider;
    use qubit_event_bus::model::AdmissionStatus;
    use qubit_event_bus::model::EventId;
    use qubit_event_bus::model::Headers;
    use qubit_event_bus::model::ProviderOptions;
    use qubit_event_bus::model::PublishAcknowledgement;
    use qubit_event_bus::model::StartPosition;
    use qubit_event_bus::model::SubscriberId;
    use qubit_event_bus::model::SubscriptionDurability;
    use qubit_event_bus::spi::EventBusSpi;
    use qubit_event_bus::spi::OutboundMessage;
    use qubit_event_bus::spi::ShutdownMode;
    use qubit_event_bus::spi::SpiSubscriptionRequest;
    use qubit_event_bus::spi::TopicAddress;
    use qubit_event_bus::spi::TransportPayload;
    use qubit_id::Id;
    use qubit_spi::ServiceProvider;

    fn create() -> Arc<dyn EventBusSpi> {
        let config = EventBusConfig::default()
            .with_provider_options(LocalEventBusConfig::default().provider_options());
        LocalEventBusProvider.create_configured(&config).unwrap()
    }

    fn create_with_total_limit(limit: usize) -> Arc<dyn EventBusSpi> {
        let config = EventBusConfig::default().with_provider_options(
            LocalEventBusConfig::new()
                .max_total_outstanding(limit)
                .provider_options(),
        );
        LocalEventBusProvider.create_configured(&config).unwrap()
    }

    fn request(id: u64, topic: &str) -> SpiSubscriptionRequest {
        SpiSubscriptionRequest::new(
            Id::new(id),
            TopicAddress::new(topic).unwrap(),
            SubscriberId::new(format!("subscriber-{id}")).unwrap(),
            None,
            SubscriptionDurability::Ephemeral,
            StartPosition::New,
            ProviderOptions::new(),
            TypeId::of::<u32>(),
        )
    }

    fn outbound(topic: &str, event_id: &str) -> OutboundMessage {
        OutboundMessage::new(
            TopicAddress::new(topic).unwrap(),
            EventId::new(event_id).unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Native(Arc::new(7_u32)),
        )
    }

    fn assert_race_result(result: Result<PublishAcknowledgement, SpiError>, expected_id: Id) {
        match result {
            Ok(PublishAcknowledgement::DestinationAdmissions(admissions)) => {
                assert!(
                    admissions.len() <= 1,
                    "one live subscription may appear at most once"
                );
                for admission in admissions {
                    assert_eq!(
                        expected_id,
                        admission.subscription_id(),
                        "unrelated destination appeared"
                    );
                    assert!(matches!(
                        admission.status(),
                        AdmissionStatus::Accepted | AdmissionStatus::Rejected(_)
                    ));
                }
            }
            Err(SpiError::Operation {
                operation: "publish",
                kind: "provider_closed",
                ..
            }) => {}
            Err(error) => panic!("unexpected publish error during lifecycle race: {error}"),
            Ok(_) => panic!("local provider must report destination admissions"),
        }
    }

    #[test]
    fn test_publish_cancel_shutdown_snapshot_contract() {
        // Each iteration starts the publisher and lifecycle operation together;
        // neither branch assumes when the provider captures the target snapshot.
        for iteration in 0..32 {
            let spi = create();
            let expected_id = Id::new(1);
            let mut receiver = spi.subscribe(request(1, "race.target")).unwrap();
            let _unrelated = spi.subscribe(request(2, "race.unrelated")).unwrap();
            let barrier = Arc::new(Barrier::new(2));
            let publish_barrier = barrier.clone();
            let publisher = spi.clone();
            let publish = thread::spawn(move || {
                publish_barrier.wait();
                publisher.publish(outbound("race.target", &format!("close-race-{iteration}")))
            });
            barrier.wait();
            receiver.close().unwrap();
            assert_race_result(publish.join().unwrap(), expected_id);
            let after_close = spi.publish(outbound("race.target", "after-close")).unwrap();
            assert!(
                matches!(after_close, PublishAcknowledgement::DestinationAdmissions(items) if items.is_empty())
            );

            let shutdown_spi = create();
            let _receiver = shutdown_spi.subscribe(request(1, "race.target")).unwrap();
            let _unrelated = shutdown_spi
                .subscribe(request(2, "race.unrelated"))
                .unwrap();
            let barrier = Arc::new(Barrier::new(2));
            let publish_barrier = barrier.clone();
            let publisher = shutdown_spi.clone();
            let publish = thread::spawn(move || {
                publish_barrier.wait();
                publisher.publish(outbound(
                    "race.target",
                    &format!("shutdown-race-{iteration}"),
                ))
            });
            barrier.wait();
            let _ = shutdown_spi.shutdown(ShutdownMode::Immediate).unwrap();
            assert_race_result(publish.join().unwrap(), expected_id);
            let error = shutdown_spi
                .publish(outbound("race.target", "after-shutdown"))
                .unwrap_err();
            assert!(matches!(
                error,
                SpiError::Operation {
                    operation: "publish",
                    kind: "provider_closed",
                    ..
                }
            ));
        }
    }

    #[test]
    fn test_concurrent_publishers_cannot_exceed_provider_wide_limit() {
        let spi = create_with_total_limit(1);
        let _first = spi.subscribe(request(11, "budget.first")).unwrap();
        let _second = spi.subscribe(request(12, "budget.second")).unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let first_barrier = barrier.clone();
        let first_spi = spi.clone();
        let first = thread::spawn(move || {
            first_barrier.wait();
            first_spi.publish(outbound("budget.first", "budget-first"))
        });
        let second_barrier = barrier.clone();
        let second_spi = spi.clone();
        let second = thread::spawn(move || {
            second_barrier.wait();
            second_spi.publish(outbound("budget.second", "budget-second"))
        });
        barrier.wait();

        let results = [
            first.join().unwrap().unwrap(),
            second.join().unwrap().unwrap(),
        ];
        let accepted = results
            .iter()
            .flat_map(|result| match result {
                PublishAcknowledgement::DestinationAdmissions(admissions) => admissions,
                _ => panic!("local provider reports per destination admissions"),
            })
            .filter(|admission| matches!(admission.status(), AdmissionStatus::Accepted))
            .count();
        assert_eq!(1, accepted);
    }
}
