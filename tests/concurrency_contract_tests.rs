// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Loom lifecycle models and real local SPI lifecycle race contracts.
//!
//! These models validate the required state transitions independently of the
//! production executor. They intentionally do not claim to instrument private
//! facade internals or prove their implementation correct by themselves.

#[cfg(loom)]
use loom::model;
#[cfg(loom)]
use loom::sync::Arc;
#[cfg(loom)]
use loom::sync::Condvar;
#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(loom)]
use loom::sync::atomic::AtomicBool;
#[cfg(loom)]
use loom::sync::atomic::AtomicUsize;
#[cfg(loom)]
use loom::sync::atomic::Ordering;
#[cfg(loom)]
use loom::thread;

#[cfg(loom)]
#[test]
fn admission_permit_is_released_exactly_once_under_competing_cleanup() {
    model(|| {
        let active = Arc::new(AtomicUsize::new(1));
        let released = Arc::new(AtomicBool::new(false));
        let release = |active: &AtomicUsize, released: &AtomicBool| {
            if !released.swap(true, Ordering::AcqRel) {
                active.fetch_sub(1, Ordering::AcqRel);
            }
        };

        let first_active = active.clone();
        let first_released = released.clone();
        let first = thread::spawn(move || release(&first_active, &first_released));
        let second_active = active.clone();
        let second_released = released.clone();
        let second = thread::spawn(move || release(&second_active, &second_released));
        first.join().unwrap();
        second.join().unwrap();

        assert_eq!(0, active.load(Ordering::Acquire));
        assert!(released.load(Ordering::Acquire));
    });
}

#[cfg(loom)]
#[test]
fn cancelling_an_ordering_lane_advances_the_next_waiter() {
    model(|| {
        let lane = Arc::new((Mutex::new((true, false)), Condvar::new()));
        let next_lane = lane.clone();
        let next = thread::spawn(move || {
            let (lock, ready) = &*next_lane;
            let mut state = lock.lock().unwrap();
            while state.0 {
                state = ready.wait(state).unwrap();
            }
            state.1 = true;
        });

        let cancel_lane = lane.clone();
        let cancel = thread::spawn(move || {
            let (lock, ready) = &*cancel_lane;
            let mut state = lock.lock().unwrap();
            state.0 = false;
            ready.notify_all();
        });

        cancel.join().unwrap();
        next.join().unwrap();
        assert!(lane.0.lock().unwrap().1);
    });
}

#[cfg(loom)]
#[test]
fn subscription_cancel_racing_receive_never_starts_after_cancel_wins() {
    model(|| {
        #[derive(Default)]
        struct State {
            cancelled: bool,
            handler_starts: usize,
            handler_starts_when_cancelled: usize,
        }

        let state = Arc::new(Mutex::new(State::default()));
        let receive_state = state.clone();
        let receive = thread::spawn(move || {
            let mut state = receive_state.lock().unwrap();
            if !state.cancelled {
                state.handler_starts += 1;
            }
        });
        let cancel_state = state.clone();
        let cancel = thread::spawn(move || {
            let mut state = cancel_state.lock().unwrap();
            state.cancelled = true;
            state.handler_starts_when_cancelled = state.handler_starts;
        });

        receive.join().unwrap();
        cancel.join().unwrap();
        let state = state.lock().unwrap();
        assert!(state.cancelled);
        assert_eq!(state.handler_starts_when_cancelled, state.handler_starts);
    });
}

#[cfg(loom)]
#[test]
fn graceful_shutdown_and_publish_have_one_admission_linearization_point() {
    model(|| {
        #[derive(Default)]
        struct State {
            closed: bool,
            admitted: usize,
        }

        let state = Arc::new(Mutex::new(State::default()));
        let publish_state = state.clone();
        let publish = thread::spawn(move || {
            let mut state = publish_state.lock().unwrap();
            if !state.closed {
                state.admitted += 1;
            }
        });
        let shutdown_state = state.clone();
        let shutdown = thread::spawn(move || {
            shutdown_state.lock().unwrap().closed = true;
        });

        publish.join().unwrap();
        shutdown.join().unwrap();
        let state = state.lock().unwrap();
        assert!(state.closed);
        assert!(state.admitted <= 1);
    });
}

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
        let config = EventBusConfig::default().with_provider_options(LocalEventBusConfig::default().provider_options());
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
                assert!(admissions.len() <= 1, "one live subscription may appear at most once");
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
            assert!(matches!(after_close, PublishAcknowledgement::DestinationAdmissions(items) if items.is_empty()));

            let shutdown_spi = create();
            let _receiver = shutdown_spi.subscribe(request(1, "race.target")).unwrap();
            let _unrelated = shutdown_spi.subscribe(request(2, "race.unrelated")).unwrap();
            let barrier = Arc::new(Barrier::new(2));
            let publish_barrier = barrier.clone();
            let publisher = shutdown_spi.clone();
            let publish = thread::spawn(move || {
                publish_barrier.wait();
                publisher.publish(outbound("race.target", &format!("shutdown-race-{iteration}")))
            });
            barrier.wait();
            shutdown_spi.shutdown(ShutdownMode::Immediate).unwrap();
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
}
