// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exact wait-entry synchronization checks for the four large-timeout paths.

use std::any::TypeId;
use std::io;
use std::num::NonZeroUsize;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use qubit_id::Id;
use qubit_spi::ServiceProvider;

use crate::EventBus;
use crate::NotificationPublisher;
use crate::WaitOutcome;
use crate::error::SpiError;
use crate::internal::test_wait_probe;
use crate::local::LocalEventBusConfig;
use crate::local::LocalEventBusProvider;
use crate::model::AdmissionStatus;
use crate::model::EventId;
use crate::model::Headers;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;
use crate::model::StartPosition;
use crate::model::SubscriberId;
use crate::model::SubscriptionDurability;
use crate::model::Topic;
use crate::registry::EventBusConfig;
use crate::spi::DelayedDeliveryCapability;
use crate::spi::DeliveryDisposition;
use crate::spi::DurabilityCapability;
use crate::spi::EventBusCapabilities;
use crate::spi::EventBusSpi;
use crate::spi::EventSubscriptionSpi;
use crate::spi::OrderingCapability;
use crate::spi::OutboundMessage;
use crate::spi::PayloadModes;
use crate::spi::PublishGuarantee;
use crate::spi::PublishVisibility;
use crate::spi::ReceiveOutcome;
use crate::spi::ReplayCapability;
use crate::spi::SettlementCapabilities;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::SubscriptionModes;
use crate::spi::TopicAddress;
use crate::spi::TransportPayload;

const CASE_ENV: &str = "QUBIT_EVENT_BUS_LARGE_TIMEOUT_UNIT_CASE";
const WATCHDOG: Duration = Duration::from_secs(10);
const WAIT_TIMEOUTS: [Duration; 3] = [
    Duration::MAX,
    Duration::from_secs(60 * 60 * 24 * 365 * 100),
    Duration::from_secs(3),
];

#[test]
fn test_large_timeout_probe_local_receive() {
    isolated("test_large_timeout_probe_local_receive", "receive", || {
        for (index, timeout) in WAIT_TIMEOUTS.into_iter().enumerate() {
            let topic = format!("probe.receive.{index}");
            let event_id = format!("receive-{index}");
            let spi = local_spi_with(LocalEventBusConfig::new().queue_capacity(1));
            let mut receiver = spi.subscribe(subscription(11 + index as u64, &topic)).unwrap();
            let probe = test_wait_probe::install("local_receive");
            let (done_sender, done_receiver) = mpsc::channel();
            let worker = thread::spawn(move || {
                let result = receiver.receive(timeout).unwrap();
                done_sender.send((receiver, result)).unwrap();
            });
            probe
                .recv_timeout(Duration::from_secs(2))
                .expect("receive is at its Condvar wait site while holding the queue lock");
            publish(&*spi, &topic, &event_id);
            let (mut receiver, ReceiveOutcome::Message(message)) =
                done_receiver.recv_timeout(Duration::from_secs(2)).unwrap()
            else {
                panic!("the gated publish wakes receive");
            };
            receiver
                .settle(
                    message.settlement().expect("local receive issues a settlement token"),
                    DeliveryDisposition::Accept,
                )
                .unwrap();
            worker.join().unwrap();
            assert_eq!(
                ShutdownOutcome::Complete,
                spi.shutdown(ShutdownMode::Immediate).unwrap()
            );
        }
    });
}

#[test]
fn test_large_timeout_probe_topic_idle() {
    isolated("test_large_timeout_probe_topic_idle", "topic-idle", || {
        for (index, timeout) in WAIT_TIMEOUTS.into_iter().enumerate() {
            let topic_name = format!("probe.idle.{index}");
            let spi = local_spi_with(LocalEventBusConfig::new().queue_capacity(1));
            let bus = EventBus::from_spi(ProviderId::new("probe-idle").unwrap(), Arc::clone(&spi)).unwrap();
            let mut receiver = spi.subscribe(subscription(12 + index as u64, &topic_name)).unwrap();
            publish(&*spi, &topic_name, &format!("idle-{index}"));
            let topic = Topic::<u32>::new(&topic_name).unwrap();
            let probe = test_wait_probe::install("topic_idle");
            let wait_bus = bus.clone();
            let waiter = thread::spawn(move || wait_bus.wait_for_idle(&topic, Some(timeout)).unwrap());
            probe
                .recv_timeout(Duration::from_secs(2))
                .expect("topic-idle wait holds the shared state lock at its Condvar wait site");
            let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).unwrap() else {
                panic!("pending publish is received");
            };
            receiver
                .settle(
                    message.settlement().expect("local delivery is settleable"),
                    DeliveryDisposition::Accept,
                )
                .unwrap();
            assert_eq!(WaitOutcome::Idle, waiter.join().unwrap());
            let PublishAcknowledgement::DestinationAdmissions(admissions) =
                publish_ack(&*spi, &topic_name, &format!("idle-after-settlement-{index}"))
            else {
                panic!("local publish reports destination admissions");
            };
            assert_eq!(1, admissions.len());
            assert_eq!(&AdmissionStatus::Accepted, admissions[0].status());
            let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).unwrap() else {
                panic!("capacity is available after settlement");
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
        }
    });
}

#[test]
fn test_large_timeout_probe_graceful_shutdown() {
    isolated(
        "test_large_timeout_probe_graceful_shutdown",
        "graceful-shutdown",
        || {
            for (index, timeout) in WAIT_TIMEOUTS.into_iter().enumerate() {
                let topic = format!("probe.shutdown.{index}");
                let spi = local_spi();
                let mut receiver = spi.subscribe(subscription(13 + index as u64, &topic)).unwrap();
                publish(&*spi, &topic, &format!("shutdown-{index}"));
                let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).unwrap() else {
                    panic!("pending publish is received");
                };
                let probe = test_wait_probe::install("graceful_shutdown");
                let shutdown_spi = Arc::clone(&spi);
                let shutdown =
                    thread::spawn(move || shutdown_spi.shutdown(ShutdownMode::Graceful { timeout }).unwrap());
                probe
                    .recv_timeout(Duration::from_secs(2))
                    .expect("graceful shutdown holds the shared state lock at its Condvar wait site");
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
fn test_large_timeout_probe_notification_close() {
    isolated(
        "test_large_timeout_probe_notification_close",
        "notification-close",
        || {
            for (index, timeout) in WAIT_TIMEOUTS.into_iter().enumerate() {
                let spi = Arc::new(ProviderGate::new());
                let bus = EventBus::from_spi(ProviderId::new("probe-notification").unwrap(), spi.clone()).unwrap();
                let publisher = Arc::new(
                    NotificationPublisher::new(
                        bus,
                        Topic::<String>::new(&format!("probe.notification.{index}")).unwrap(),
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
                let probe = test_wait_probe::install("notification_close");
                let close_publisher = Arc::clone(&publisher);
                let closer = thread::spawn(move || close_publisher.close_with_timeout(timeout));
                probe
                    .recv_timeout(Duration::from_secs(2))
                    .expect("close holds worker state at its Condvar wait site");
                spi.release();
                closer.join().unwrap().unwrap();
                assert_eq!(vec!["blocked", "queued"], *spi.published.lock().unwrap());
                publisher.close().unwrap();
            }
        },
    );
}

fn isolated(test_name: &str, case: &str, child: impl FnOnce()) {
    if std::env::var(CASE_ENV).as_deref() == Ok(case) {
        child();
        return;
    }
    let executable = std::env::current_exe().expect("unit test executable exists");
    let listing = Command::new(&executable)
        .args(["--list", "--format", "terse"])
        .output()
        .expect("unit test names can be listed");
    assert!(listing.status.success(), "unit tests can be listed");
    let listing = String::from_utf8(listing.stdout).expect("unit test list is UTF-8");
    let matching = listing
        .lines()
        .filter(|line| line.ends_with(&format!("::{test_name}: test")))
        .collect::<Vec<_>>();
    assert_eq!(1, matching.len(), "test name resolves exactly once");
    let exact_name = matching[0].trim_end_matches(": test");
    let mut child = Command::new(executable)
        .args(["--exact", exact_name, "--nocapture", "--test-threads=1"])
        .env(CASE_ENV, case)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("isolated unit test starts");
    let deadline = Instant::now() + WATCHDOG;
    loop {
        if let Some(status) = child.try_wait().expect("child status is available") {
            assert!(status.success(), "isolated wait probe failed: {case}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().expect("hung unit test is killed");
            child.wait().expect("killed unit test is reaped");
            panic!("isolated wait probe exceeded its 10-second watchdog: {case}");
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn local_spi() -> Arc<dyn EventBusSpi> {
    local_spi_with(LocalEventBusConfig::default())
}

fn local_spi_with(local: LocalEventBusConfig) -> Arc<dyn EventBusSpi> {
    let config = EventBusConfig::default().with_provider_options(local.provider_options());
    LocalEventBusProvider.create_configured(&config).unwrap()
}

fn subscription(id: u64, topic: &str) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new(topic).unwrap(),
        SubscriberId::new(format!("probe-{id}")).unwrap(),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        Default::default(),
        TypeId::of::<u32>(),
    )
}

fn publish(spi: &dyn EventBusSpi, topic: &str, id: &str) {
    let acknowledgement = publish_ack(spi, topic, id);
    assert!(matches!(
        acknowledgement,
        PublishAcknowledgement::DestinationAdmissions(_)
    ));
}

fn publish_ack(spi: &dyn EventBusSpi, topic: &str, id: &str) -> PublishAcknowledgement {
    spi.publish(OutboundMessage::new(
        TopicAddress::new(topic).unwrap(),
        EventId::new(id).unwrap(),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(1_u32)),
    ))
    .unwrap()
}

struct ProviderGate {
    entered: Mutex<mpsc::Receiver<String>>,
    entered_sender: Mutex<Option<mpsc::Sender<String>>>,
    released: (Mutex<bool>, Condvar),
    published: Mutex<Vec<String>>,
    calls: AtomicUsize,
}

impl ProviderGate {
    fn new() -> Self {
        let (entered_sender, entered) = mpsc::channel();
        Self {
            entered: Mutex::new(entered),
            entered_sender: Mutex::new(Some(entered_sender)),
            released: (Mutex::new(false), Condvar::new()),
            published: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
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

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let TransportPayload::Native(payload) = message.payload() else {
            panic!("notification payload is native");
        };
        let value = payload.downcast_ref::<String>().expect("String payload").clone();
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
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

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Err(SpiError::Operation {
            provider_id: "probe-notification".into(),
            operation: "subscribe",
            resource: None,
            kind: "unsupported",
            retryable: Some(false),
            source: Box::new(io::Error::other("subscriptions are unsupported")),
        })
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}
