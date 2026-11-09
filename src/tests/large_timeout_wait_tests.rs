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
use std::sync::Barrier;
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
use crate::local::LocalEventBusSpi;
use crate::model::AdmissionStatus;
use crate::model::EventId;
use crate::model::Headers;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;
use crate::model::StartPosition;
use crate::model::SubscribeRequest;
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
fn test_sync_cancel_causal_probe_and_poll_intervals() {
    isolated_with_watchdog(
        "test_sync_cancel_causal_probe_and_poll_intervals",
        "sync-cancel-causal",
        Duration::from_secs(120),
        || {
            const CONFIGURATIONS: [(&str, u64, usize); 12] = [
                ("serial", 10, 16),
                ("direct-shutdown", 50, 64),
                ("serial", 50, 256),
                ("direct-shutdown", 10, 16),
                ("serial", 10, 64),
                ("direct-shutdown", 50, 256),
                ("serial", 50, 16),
                ("direct-shutdown", 10, 64),
                ("serial", 10, 256),
                ("direct-shutdown", 50, 16),
                ("serial", 50, 64),
                ("direct-shutdown", 10, 256),
            ];
            for trial in 1..=3 {
                let offset = (trial - 1) * 5;
                for step in 0..CONFIGURATIONS.len() {
                    let (mode, interval_ms, count) = CONFIGURATIONS[(offset + step) % CONFIGURATIONS.len()];
                    run_idle_causal_probe(trial, mode, interval_ms, count);
                }
            }
            run_blocked_handler_controls();
        },
    );
}

fn run_idle_causal_probe(trial: usize, mode: &str, interval_ms: u64, count: usize) {
    let interval = Duration::from_millis(interval_ms);
    let provider = local_spi();
    let bus = EventBus::from_spi(ProviderId::new("cancel-probe").unwrap(), Arc::clone(&provider)).unwrap();
    let mut subscriptions = Vec::with_capacity(count);
    for index in 0..count {
        let topic = Topic::<u32>::new(&format!("cancel-probe.{trial}.{interval_ms}.{index}")).unwrap();
        let request = SubscribeRequest::new(&format!("cancel-probe-{trial}-{index}"), topic).unwrap();
        let subscription = bus.subscribe(request, |_| {}).unwrap();
        subscription.test_control().set_receive_poll_interval(interval);
        subscription.test_control().enable_cancellation_probe();
        subscriptions.push(subscription);
    }
    thread::sleep(Duration::from_millis(100));
    let cpu_before = process_cpu_ticks();
    let started = Instant::now();
    match mode {
        "serial" => {
            for subscription in &subscriptions {
                subscription.cancel().unwrap();
            }
            let elapsed = started.elapsed();
            let cpu_after = process_cpu_ticks();
            print_causal_probe_summary(mode, trial, interval_ms, count, elapsed, cpu_before, cpu_after);
            let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
        }
        "direct-shutdown" => {
            let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
            let elapsed = started.elapsed();
            let cpu_after = process_cpu_ticks();
            print_causal_probe_summary(mode, trial, interval_ms, count, elapsed, cpu_before, cpu_after);
        }
        _ => unreachable!("probe mode is fixed by the configuration table"),
    }
    assert!(subscriptions.iter().all(|subscription| {
        subscription
            .test_control()
            .cancellation_probe_snapshot()
            .is_some_and(|sample| sample.worker_finished.is_some())
    }));
    for (index, subscription) in subscriptions.iter().enumerate() {
        let sample = subscription.test_control().cancellation_probe_snapshot().unwrap();
        let request_ns = sample.requested.expect("cancellation request recorded").as_nanos();
        println!(
            "CANCEL_PROBE_SAMPLE mode={mode} trial={trial} interval_ms={interval_ms} count={count} index={index} request_ns={request_ns} receive_return_after_request_ns={:?} worker_finished_after_request_ns={:?}",
            sample
                .receive_returns_after_request
                .iter()
                .map(|timestamp| timestamp.as_nanos().saturating_sub(request_ns))
                .collect::<Vec<_>>(),
            sample
                .worker_finished
                .map(|duration| duration.as_nanos().saturating_sub(request_ns))
        );
    }
}

fn print_causal_probe_summary(
    mode: &str,
    trial: usize,
    interval_ms: u64,
    count: usize,
    elapsed: Duration,
    cpu_before: Option<u64>,
    cpu_after: Option<u64>,
) {
    println!(
        "CANCEL_PROBE mode={mode} trial={trial} interval_ms={interval_ms} count={count} elapsed_ns={} process_cpu_ticks_delta={:?}",
        elapsed.as_nanos(),
        cpu_before
            .zip(cpu_after)
            .map(|(before, after)| after.saturating_sub(before))
    );
}

fn run_blocked_handler_controls() {
    for mode in ["cancel", "shutdown"] {
        let provider = local_spi();
        let bus = EventBus::from_spi(ProviderId::new("blocked-handler").unwrap(), Arc::clone(&provider)).unwrap();
        let (entered_sender, entered_receiver) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let handler_release = Arc::clone(&release);
        let topic_name = format!("blocked-handler.{mode}");
        let request = SubscribeRequest::new(mode, Topic::<u32>::new(&topic_name).unwrap()).unwrap();
        let subscription = bus
            .subscribe(request, move |_| {
                entered_sender.send(()).unwrap();
                let mut released = handler_release.0.lock().unwrap();
                while !*released {
                    released = handler_release.1.wait(released).unwrap();
                }
            })
            .unwrap();
        publish(&*provider, &topic_name, &format!("blocked-{mode}"));
        entered_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("handler reached its blocking gate");

        let wait_site = if mode == "cancel" {
            "subscription_worker_join"
        } else {
            "active_workers_wait"
        };
        let waiting = crate::internal::test_wait_probe::install(wait_site);
        let (done_sender, done_receiver) = mpsc::channel();
        let waiter = if mode == "cancel" {
            let subscription = Arc::new(subscription);
            let cancel_subscription = Arc::clone(&subscription);
            thread::spawn(move || done_sender.send(cancel_subscription.cancel().is_ok()).unwrap())
        } else {
            let shutdown_bus = bus.clone();
            thread::spawn(move || {
                done_sender
                    .send(shutdown_bus.shutdown(ShutdownMode::Immediate).is_ok())
                    .unwrap()
            })
        };
        waiting
            .recv_timeout(Duration::from_secs(2))
            .expect("lifecycle waiter reached its condition-variable wait while the handler is blocked");
        assert!(
            done_receiver.try_recv().is_err(),
            "waiter cannot finish before the handler gate opens"
        );
        {
            let mut released = release.0.lock().unwrap();
            *released = true;
            release.1.notify_all();
        }
        assert!(done_receiver.recv_timeout(Duration::from_secs(2)).unwrap());
        waiter.join().unwrap();
        println!(
            "CANCEL_PROBE_BLOCKED mode={mode} wait_site={wait_site} released_after_wait_signal=true result=success"
        );
    }
}

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
fn test_local_receive_spurious_wakes_do_not_reset_timeout() {
    isolated(
        "test_local_receive_spurious_wakes_do_not_reset_timeout",
        "receive-spurious-wakes",
        || {
            let topic_name = "probe.receive.spurious";
            let topic = TopicAddress::new(topic_name).unwrap();
            let config = LocalEventBusConfig::new();
            let spi = LocalEventBusSpi::new(&config).unwrap();
            let mut receiver = spi.subscribe(subscription(19, topic_name)).unwrap();
            let (done_sender, done_receiver) = mpsc::channel();
            let started = Instant::now();
            let probe = test_wait_probe::install("local_receive");
            let timeout = Duration::from_millis(1200);
            let worker = thread::spawn(move || {
                let result = receiver.receive(timeout);
                done_sender.send(result).unwrap();
            });

            probe
                .recv_timeout(Duration::from_secs(2))
                .expect("receive entered its first condition-variable wait");
            for _ in 0..2 {
                thread::sleep(Duration::from_millis(300));
                let next_wait = test_wait_probe::install("local_receive");
                spi.wake_topic_waiters_for_test(&topic);
                next_wait
                    .recv_timeout(Duration::from_secs(2))
                    .expect("spurious wake returned to the receive wait loop");
            }

            let result = done_receiver
                .recv_timeout(Duration::from_millis(900))
                .expect("spurious wakes do not restart the 1.2 second total budget");
            assert!(matches!(result, Ok(ReceiveOutcome::TimedOut)));
            assert!(started.elapsed() < Duration::from_millis(1950));
            worker.join().unwrap();
            assert_eq!(
                ShutdownOutcome::Complete,
                spi.shutdown(ShutdownMode::Immediate).unwrap()
            );
        },
    );
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

#[test]
fn test_notification_close_and_join_share_total_budget() {
    isolated(
        "test_notification_close_and_join_share_total_budget",
        "notification-close-join-budget",
        || {
            let spi = Arc::new(ProviderGate::new());
            let bus = EventBus::from_spi(ProviderId::new("close-join-budget").unwrap(), spi.clone()).unwrap();
            let publisher = Arc::new(
                NotificationPublisher::new(
                    bus,
                    Topic::<String>::new("probe.notification.close-join").unwrap(),
                    NonZeroUsize::new(1).unwrap(),
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

            let worker_exit_barrier = Arc::new(Barrier::new(2));
            publisher.hold_worker_after_completion_for_test(Arc::clone(&worker_exit_barrier));
            let close_probe = test_wait_probe::install("notification_close");
            let (done_sender, done_receiver) = mpsc::channel();
            let close_publisher = Arc::clone(&publisher);
            let total_budget = Duration::from_millis(1200);
            let closer = thread::spawn(move || {
                done_sender
                    .send(close_publisher.close_with_timeout(total_budget))
                    .unwrap();
            });
            close_probe
                .recv_timeout(Duration::from_secs(2))
                .expect("close entered its completion condition-variable wait");

            // Consume half of the one close budget in the completion phase.
            thread::sleep(Duration::from_millis(600));
            let join_probe = test_wait_probe::install("notification_join");
            spi.release();
            join_probe
                .recv_timeout(Duration::from_secs(2))
                .expect("completion woke close and it entered join observation");

            let error = done_receiver
                .recv_timeout(Duration::from_millis(900))
                .expect("join uses only the remainder of the original close budget")
                .expect_err("the worker is held after completion until join times out");
            assert_eq!(io::ErrorKind::TimedOut, error.kind());

            worker_exit_barrier.wait();
            closer.join().unwrap();
            publisher
                .close_with_timeout(Duration::from_secs(2))
                .expect("a later close can join the completed worker");
            assert_eq!(vec!["blocked"], *spi.published.lock().unwrap());
        },
    );
}

fn isolated(test_name: &str, case: &str, child: impl FnOnce()) {
    isolated_with_watchdog(test_name, case, WATCHDOG, child);
}

fn isolated_with_watchdog(test_name: &str, case: &str, watchdog: Duration, child: impl FnOnce()) {
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
    let deadline = Instant::now() + watchdog;
    loop {
        if let Some(status) = child.try_wait().expect("child status is available") {
            assert!(status.success(), "isolated wait probe failed: {case}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().expect("hung unit test is killed");
            child.wait().expect("killed unit test is reaped");
            panic!("isolated wait probe exceeded its {watchdog:?} watchdog: {case}");
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn process_cpu_ticks() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
        let fields = stat.rsplit_once(')')?.1.split_whitespace().collect::<Vec<_>>();
        let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
        let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
        Some(user_ticks.saturating_add(system_ticks))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
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
