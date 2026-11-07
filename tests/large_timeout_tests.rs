// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Process-isolated checks for very large synchronous wait budgets.

use std::any::TypeId;
use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

mod support;

use qubit_event_bus::EventBus;
use qubit_event_bus::NotificationPublisher;
use qubit_event_bus::WaitOutcome;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use qubit_spi::ServiceProvider;

const CASE_ENV: &str = "QUBIT_EVENT_BUS_ISOLATED_CASE";
const WATCHDOG: Duration = Duration::from_secs(10);
const ORDINARY: Duration = Duration::from_secs(3);
const LONG_TIMEOUTS: [Duration; 2] = [Duration::MAX, Duration::from_secs(60 * 60 * 24 * 365 * 100)];

#[test]
fn test_large_timeout_local_receive_is_woken() {
    run_isolated("test_large_timeout_local_receive_is_woken", "local-receive", || {
        for timeout in LONG_TIMEOUTS.into_iter().chain([ORDINARY]) {
            let spi = local_spi();
            let mut receiver = spi.subscribe(subscription(1, "large-timeout.receive")).unwrap();
            let (ready_tx, ready_rx) = mpsc::channel();
            let worker = thread::spawn(move || {
                ready_tx.send(()).unwrap();
                let result = receiver.receive(timeout).unwrap();
                (receiver, result)
            });
            ready_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("receiver reaches call site");
            publish(&*spi, "large-timeout.receive", "receive-event");
            let (mut receiver, ReceiveOutcome::Message(message)) = worker.join().expect("receive worker returns")
            else {
                panic!("publish wakes receive with a message");
            };
            receiver
                .settle(
                    message.settlement().expect("local receive is settleable"),
                    DeliveryDisposition::Accept,
                )
                .unwrap();
            assert_eq!(
                ShutdownOutcome::Complete,
                spi.shutdown(ShutdownMode::Immediate).unwrap()
            );
            drop(receiver);
        }
    });
}

#[test]
fn test_large_timeout_topic_idle_is_woken_by_accept() {
    run_isolated("test_large_timeout_topic_idle_is_woken_by_accept", "topic-idle", || {
        for timeout in LONG_TIMEOUTS.into_iter().chain([ORDINARY]) {
            let spi = local_spi();
            let bus = EventBus::from_spi(ProviderId::new("large-timeout").unwrap(), Arc::clone(&spi))
                .expect("local SPI forms a facade");
            let mut receiver = spi.subscribe(subscription(3, "large-timeout.idle")).unwrap();
            publish(&*spi, "large-timeout.idle", "idle-event");
            let topic = Topic::<u32>::new("large-timeout.idle").unwrap();
            let (ready_tx, ready_rx) = mpsc::channel();
            let wait_bus = bus.clone();
            let waiter = thread::spawn(move || {
                ready_tx.send(()).unwrap();
                wait_bus.wait_for_idle(&topic, Some(timeout))
            });
            ready_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("idle waiter reaches call site");
            let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).unwrap() else {
                panic!("pending publish is received");
            };
            receiver
                .settle(
                    message.settlement().expect("local delivery is settleable"),
                    DeliveryDisposition::Accept,
                )
                .unwrap();
            assert_eq!(WaitOutcome::Idle, waiter.join().unwrap().unwrap());
            let _ = spi.shutdown(ShutdownMode::Immediate).unwrap();
        }
    });
}

#[test]
fn test_large_timeout_graceful_shutdown_is_woken_by_accept() {
    run_isolated(
        "test_large_timeout_graceful_shutdown_is_woken_by_accept",
        "graceful-shutdown",
        || {
            for timeout in LONG_TIMEOUTS.into_iter().chain([ORDINARY]) {
                let spi = local_spi();
                let mut receiver = spi.subscribe(subscription(4, "large-timeout.shutdown")).unwrap();
                publish(&*spi, "large-timeout.shutdown", "shutdown-event");
                let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).unwrap() else {
                    panic!("pending publish is received");
                };
                let (ready_tx, ready_rx) = mpsc::channel();
                let shutdown_spi = Arc::clone(&spi);
                let shutdown = thread::spawn(move || {
                    ready_tx.send(()).unwrap();
                    shutdown_spi.shutdown(ShutdownMode::Graceful { timeout }).unwrap()
                });
                ready_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("shutdown reaches call site");
                receiver
                    .settle(
                        message.settlement().expect("local delivery is settleable"),
                        DeliveryDisposition::Accept,
                    )
                    .unwrap();
                assert_eq!(ShutdownOutcome::Complete, shutdown.join().unwrap());
                assert_eq!(
                    ShutdownOutcome::Complete,
                    spi.shutdown(ShutdownMode::Immediate).unwrap()
                );
            }
        },
    );
}

#[test]
fn test_large_timeout_notification_close_drains_after_provider_release() {
    run_isolated(
        "test_large_timeout_notification_close_drains_after_provider_release",
        "notification-close",
        || {
            for timeout in LONG_TIMEOUTS.into_iter().chain([ORDINARY]) {
                let spi = Arc::new(ProviderGate::new());
                let bus = EventBus::from_spi(ProviderId::new("large-timeout-gate").unwrap(), spi.clone())
                    .expect("gated SPI forms a facade");
                let publisher = Arc::new(
                    NotificationPublisher::new(
                        bus,
                        Topic::<String>::new("large-timeout.notification").unwrap(),
                        NonZeroUsize::new(2).unwrap(),
                        |_| {},
                    )
                    .unwrap(),
                );
                publisher.try_publish("blocked".to_owned()).unwrap();
                assert_eq!(
                    "blocked",
                    spi.entered
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(2))
                        .unwrap()
                );
                publisher.try_publish("queued".to_owned()).unwrap();
                let (ready_tx, ready_rx) = mpsc::channel();
                let close_publisher = Arc::clone(&publisher);
                let closer = thread::spawn(move || {
                    ready_tx.send(()).unwrap();
                    close_publisher.close_with_timeout(timeout)
                });
                ready_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("close reaches call site");
                spi.release();
                closer.join().unwrap().unwrap();
                assert_eq!(vec!["blocked", "queued"], *spi.published.lock().unwrap());
                publisher.close().unwrap();
                let _ = spi.shutdown(ShutdownMode::Immediate).unwrap();
            }
        },
    );
}

#[test]
fn test_large_timeout_zero_and_finite_exhaustion() {
    run_isolated(
        "test_large_timeout_zero_and_finite_exhaustion",
        "finite-exhaustion",
        || {
            let spi = local_spi();
            let mut receiver = spi.subscribe(subscription(5, "large-timeout.finite")).unwrap();
            assert!(matches!(
                receiver.receive(Duration::ZERO).unwrap(),
                ReceiveOutcome::TimedOut
            ));
            assert!(matches!(
                receiver.receive(Duration::from_millis(40)).unwrap(),
                ReceiveOutcome::TimedOut
            ));

            let bus = EventBus::from_spi(ProviderId::new("large-timeout-finite").unwrap(), Arc::clone(&spi))
                .expect("local SPI forms a facade");
            publish(&*spi, "large-timeout.finite", "pending-event");
            let topic = Topic::<u32>::new("large-timeout.finite").unwrap();
            assert_eq!(
                WaitOutcome::TimedOut,
                bus.wait_for_idle(&topic, Some(Duration::ZERO)).unwrap()
            );
            assert_eq!(
                WaitOutcome::TimedOut,
                bus.wait_for_idle(&topic, Some(Duration::from_millis(40))).unwrap()
            );
            let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).unwrap() else {
                panic!("pending publish is received");
            };
            receiver
                .settle(
                    message.settlement().expect("local delivery is settleable"),
                    DeliveryDisposition::Accept,
                )
                .unwrap();
            assert_eq!(
                ShutdownOutcome::Complete,
                spi.shutdown(ShutdownMode::Immediate).unwrap()
            );

            assert_graceful_timeout(Duration::ZERO, 6);
            assert_graceful_timeout(Duration::from_millis(40), 7);

            let gated = Arc::new(ProviderGate::new());
            let gated_bus = EventBus::from_spi(ProviderId::new("large-timeout-zero").unwrap(), gated.clone())
                .expect("gated SPI forms a facade");
            let publisher = NotificationPublisher::new(
                gated_bus,
                Topic::<String>::new("large-timeout.zero-close").unwrap(),
                NonZeroUsize::new(1).unwrap(),
                |_| {},
            )
            .unwrap();
            publisher.try_publish("blocked".into()).unwrap();
            assert_eq!(
                "blocked",
                gated
                    .entered
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap()
            );
            assert_eq!(
                io::ErrorKind::TimedOut,
                publisher.close_with_timeout(Duration::ZERO).unwrap_err().kind()
            );
            assert_eq!(
                io::ErrorKind::TimedOut,
                publisher
                    .close_with_timeout(Duration::from_millis(40))
                    .unwrap_err()
                    .kind()
            );
            gated.release();
            publisher.close().unwrap();
            let _ = gated.shutdown(ShutdownMode::Immediate).unwrap();
        },
    );
}

fn assert_graceful_timeout(timeout: Duration, subscription_id: u64) {
    let spi = local_spi();
    let mut receiver = spi
        .subscribe(subscription(subscription_id, "large-timeout.graceful-finite"))
        .unwrap();
    publish(&*spi, "large-timeout.graceful-finite", "graceful-finite-event");
    let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).unwrap() else {
        panic!("pending publish is received");
    };
    assert_eq!(
        ShutdownOutcome::TimedOut,
        spi.shutdown(ShutdownMode::Graceful { timeout }).unwrap()
    );
    drop(message);
}

fn run_isolated(test_name: &str, case: &str, child: impl FnOnce()) {
    if std::env::var(CASE_ENV).as_deref() == Ok(case) {
        child();
    } else {
        support::isolated_process::run_case_with_timeout(test_name, case, WATCHDOG);
    }
}

fn local_spi() -> Arc<dyn EventBusSpi> {
    let config = EventBusConfig::default().with_provider_options(LocalEventBusConfig::default().provider_options());
    LocalEventBusProvider.create_configured(&config).unwrap()
}

fn subscription(id: u64, topic: &str) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new(topic).unwrap(),
        SubscriberId::new(format!("large-timeout-{id}")).unwrap(),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        Default::default(),
        TypeId::of::<u32>(),
    )
}

fn publish(spi: &dyn EventBusSpi, topic: &str, event_id: &str) {
    let acknowledgement = spi
        .publish(OutboundMessage::new(
            TopicAddress::new(topic).unwrap(),
            EventId::new(event_id).unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Native(Arc::new(7_u32)),
        ))
        .unwrap();
    assert!(matches!(
        acknowledgement,
        PublishAcknowledgement::DestinationAdmissions(_)
    ));
}

struct ProviderGate {
    entered: Mutex<mpsc::Receiver<String>>,
    entered_sender: Mutex<Option<mpsc::Sender<String>>>,
    released: (Mutex<bool>, Condvar),
    published: Mutex<Vec<String>>,
    publish_count: AtomicUsize,
}

impl ProviderGate {
    fn new() -> Self {
        let (entered_sender, entered) = mpsc::channel();
        Self {
            entered: Mutex::new(entered),
            entered_sender: Mutex::new(Some(entered_sender)),
            released: (Mutex::new(false), Condvar::new()),
            published: Mutex::new(Vec::new()),
            publish_count: AtomicUsize::new(0),
        }
    }

    fn release(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }
}

impl EventBusSpi for ProviderGate {
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
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, qubit_event_bus::error::SpiError> {
        let TransportPayload::Native(payload) = message.payload() else {
            panic!("notification publisher sends native payloads");
        };
        let value = payload.downcast_ref::<String>().expect("String payload").clone();
        if self.publish_count.fetch_add(1, Ordering::AcqRel) == 0 {
            if let Some(sender) = self.entered_sender.lock().unwrap().take() {
                sender.send(value.clone()).unwrap();
            }
            let mut released = self.released.0.lock().unwrap();
            while !*released {
                released = self.released.1.wait(released).unwrap();
            }
        }
        self.published.lock().unwrap().push(value);
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(
        &self,
        _: SpiSubscriptionRequest,
    ) -> Result<Box<dyn EventSubscriptionSpi>, qubit_event_bus::error::SpiError> {
        Err(qubit_event_bus::error::SpiError::Operation {
            provider_id: "large-timeout-gate".into(),
            operation: "subscribe",
            resource: None,
            kind: "unsupported",
            retryable: Some(false),
            source: Box::new(io::Error::other("subscriptions are unsupported")),
        })
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, qubit_event_bus::error::SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}
