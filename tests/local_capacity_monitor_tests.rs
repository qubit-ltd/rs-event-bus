// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::error::FacadeBuildError;
use qubit_event_bus::error::ProviderError;
use qubit_event_bus::error::SubscribeError;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionRejectionKind;
use qubit_event_bus::model::AsyncSubscriberNext;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("construction must complete immediately"),
    }
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test capacities are positive")
}

fn scheduling_with_one_subscription() -> DeliverySchedulingConfig {
    DeliverySchedulingConfig::new(nonzero(1), nonzero(2), nonzero(1), nonzero(1))
        .expect("test scheduling limits are valid")
}

#[test]
fn configured_public_constructors_apply_facade_limits_and_share_local_monitor() {
    let local = LocalEventBusConfig::new().max_total_outstanding(1);
    let facade = EventBusFacadeConfig::default().with_delivery_scheduling(scheduling_with_one_subscription());
    let (bus, monitor) = EventBus::local_with_config_and_monitor(local.clone(), facade.clone()).unwrap();
    assert_eq!(monitor.snapshot().max_total_outstanding, 1);
    let topic = Topic::<u32>::new("local.capacity.configured.sync").unwrap();
    let first = bus
        .subscribe(SubscribeRequest::new("first", topic.clone()).unwrap(), |_| {})
        .unwrap();
    assert!(matches!(
        bus.subscribe(SubscribeRequest::new("second", topic).unwrap(), |_| {}),
        Err(SubscribeError::ResourceLimit {
            resource: "subscriptions",
            limit: 1
        })
    ));
    first.cancel().unwrap();
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();

    let (bus, monitor) = ready(AsyncEventBus::local_with_config_and_monitor(local, facade)).unwrap();
    assert_eq!(monitor.snapshot().max_total_outstanding, 1);
    let topic = Topic::<u32>::new("local.capacity.configured.async").unwrap();
    let mut first = ready(bus.subscribe(SubscribeRequest::new("first", topic.clone()).unwrap())).unwrap();
    assert!(matches!(
        ready(bus.subscribe(SubscribeRequest::new("second", topic).unwrap())),
        Err(SubscribeError::ResourceLimit {
            resource: "subscriptions",
            limit: 1
        })
    ));
    ready(first.close()).unwrap();
    let _ = ready(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn configured_public_constructors_preserve_facade_configuration_source_chain() {
    let sync_config = EventBusFacadeConfig::new()
        .async_subscriber_interceptor(|delivery: Delivery<u32>, next: AsyncSubscriberNext<u32>| next(delivery));
    let error = match EventBus::local_with_config_and_monitor(LocalEventBusConfig::new(), sync_config) {
        Ok(_) => panic!("sync facade must reject async subscriber middleware"),
        Err(error) => error,
    };
    let ProviderError::Creation { source } = error else {
        panic!("invalid facade configuration is a creation error");
    };
    let build_error = source
        .downcast_ref::<FacadeBuildError>()
        .expect("facade build source retained");
    assert!(matches!(
        build_error,
        FacadeBuildError::Configuration(ConfigurationError::InvalidField {
            field: "async_subscriber_interceptor",
            ..
        })
    ));
    assert!(
        std::error::Error::source(build_error)
            .expect("configuration error remains in source chain")
            .is::<ConfigurationError>()
    );

    let async_config = EventBusFacadeConfig::new().subscriber_interceptor(
        |delivery: Delivery<u32>, next: qubit_event_bus::model::SubscriberNext<u32>| next(delivery),
    );
    let error = match ready(AsyncEventBus::local_with_config_and_monitor(
        LocalEventBusConfig::new(),
        async_config,
    )) {
        Ok(_) => panic!("async facade must reject sync subscriber middleware"),
        Err(error) => error,
    };
    let ProviderError::Creation { source } = error else {
        panic!("invalid facade configuration is a creation error");
    };
    let build_error = source
        .downcast_ref::<FacadeBuildError>()
        .expect("facade build source retained");
    assert!(matches!(
        build_error,
        FacadeBuildError::Configuration(ConfigurationError::InvalidField {
            field: "sync_subscriber_interceptor",
            ..
        })
    ));
    assert!(
        std::error::Error::source(build_error)
            .expect("configuration error remains in source chain")
            .is::<ConfigurationError>()
    );
}

#[test]
fn sync_and_async_monitors_report_config_and_share_across_clones() {
    let config = LocalEventBusConfig::new()
        .queue_capacity(3)
        .max_total_outstanding(7)
        .max_total_outstanding_weight_bytes(NonZeroUsize::new(64).unwrap());
    let (_bus, monitor) = EventBus::local_with_monitor(config.clone()).unwrap();
    let snapshot = monitor.snapshot();
    assert_eq!(snapshot.outstanding_count, 0);
    assert_eq!(snapshot.max_total_outstanding, 7);
    assert_eq!(snapshot.outstanding_weight_bytes, 0);
    assert_eq!(snapshot.max_total_outstanding_weight_bytes, NonZeroUsize::new(64));
    assert_eq!(snapshot.queue_capacity, 3);
    assert_eq!(monitor.clone().snapshot(), snapshot);

    let (_bus, monitor) = ready(AsyncEventBus::local_with_monitor(config)).unwrap();
    assert_eq!(monitor.snapshot(), snapshot);
}

#[test]
fn disabled_weight_budget_is_reported_as_none_and_zero() {
    let (_bus, monitor) = EventBus::local_with_monitor(LocalEventBusConfig::new()).unwrap();
    let snapshot = monitor.snapshot();
    assert_eq!(snapshot.outstanding_weight_bytes, 0);
    assert_eq!(snapshot.max_total_outstanding_weight_bytes, None);
}

#[test]
fn provider_rejection_counts_once_and_settlement_releases_the_snapshot() {
    let config = LocalEventBusConfig::new().max_total_outstanding(1);
    let (bus, monitor) = EventBus::local_with_config_and_monitor(config, EventBusFacadeConfig::default()).unwrap();
    let topic = Topic::<u32>::new("local.capacity.monitor").unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("capacity-monitor", topic.clone()).unwrap(),
            move |_| {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            },
        )
        .unwrap();
    let _ = bus.publish(PublishRequest::new(topic.clone(), 1).unwrap()).unwrap();
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let receipt = bus.publish(PublishRequest::new(topic, 2).unwrap()).unwrap();
    let PublishAcknowledgement::DestinationAdmissions(items) = receipt.acknowledgement() else {
        panic!("local provider reports destination admissions");
    };
    assert_eq!(
        items[0].rejection_kind(),
        Some(AdmissionRejectionKind::ProviderCountCapacity)
    );
    let snapshot = monitor.snapshot();
    assert_eq!(snapshot.outstanding_count, 1);
    assert_eq!(snapshot.rejected_provider_count_capacity, 1);
    assert_eq!(snapshot.rejected_provider_weight_capacity, 0);

    release_tx.send(()).unwrap();
    drop(subscription);
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
    drop(bus);
    assert_eq!(monitor.snapshot().outstanding_count, 0);
}

#[test]
fn subscription_capacity_rejection_is_counted_once() {
    let config = LocalEventBusConfig::new().queue_capacity(1).max_total_outstanding(5);
    let (bus, monitor) = EventBus::local_with_monitor(config).unwrap();
    let topic = Topic::<u32>::new("local.capacity.monitor.queue").unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("capacity-monitor-queue", topic.clone()).unwrap(),
            move |_| {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            },
        )
        .unwrap();
    let _ = bus.publish(PublishRequest::new(topic.clone(), 1).unwrap()).unwrap();
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let receipt = bus.publish(PublishRequest::new(topic, 2).unwrap()).unwrap();
    let PublishAcknowledgement::DestinationAdmissions(items) = receipt.acknowledgement() else {
        panic!("local provider reports destination admissions");
    };
    assert_eq!(
        items[0].rejection_kind(),
        Some(AdmissionRejectionKind::SubscriptionCapacity)
    );
    let snapshot = monitor.snapshot();
    assert_eq!(snapshot.outstanding_count, 1);
    assert_eq!(snapshot.rejected_subscription_capacity, 1);

    release_tx.send(()).unwrap();
    drop(subscription);
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
    assert_eq!(monitor.snapshot().outstanding_count, 0);
}

#[test]
fn async_monitor_tracks_weight_rejection_and_terminal_settlement() {
    use std::num::NonZeroUsize;

    use qubit_event_bus::AsyncEventBus;

    let config = LocalEventBusConfig::new()
        .max_total_outstanding(5)
        .max_total_outstanding_weight_bytes(NonZeroUsize::new(5).unwrap());
    let (bus, monitor) = ready(AsyncEventBus::local_with_monitor(config)).unwrap();
    let topic = Topic::<u32>::new("local.capacity.monitor.async").unwrap();
    let subscription =
        ready(bus.subscribe(SubscribeRequest::new("async-capacity-monitor", topic.clone()).unwrap())).unwrap();
    let weighted = |payload| {
        PublishRequest::builder()
            .topic(topic.clone())
            .payload(payload)
            .options(
                PublishOptions::builder()
                    .native_payload_weight(|_| NonZeroUsize::new(3).unwrap())
                    .build(),
            )
            .build()
            .unwrap()
    };
    let _ = ready(bus.publish(weighted(1))).unwrap();
    let rejected = ready(bus.publish(weighted(2))).unwrap();
    let PublishAcknowledgement::DestinationAdmissions(items) = rejected.acknowledgement() else {
        panic!("local provider reports destination admissions");
    };
    assert_eq!(
        items[0].rejection_kind(),
        Some(AdmissionRejectionKind::ProviderWeightCapacity)
    );
    assert_eq!(monitor.snapshot().outstanding_count, 1);
    assert_eq!(monitor.snapshot().outstanding_weight_bytes, 3);
    assert_eq!(monitor.snapshot().rejected_provider_weight_capacity, 1);

    let (handled_tx, handled_rx) = mpsc::channel();
    let runner = thread::spawn(move || {
        manual_async::block_on(subscription.run(move |_| {
            handled_tx.send(()).unwrap();
            async { Ok(()) }
        }))
    });
    handled_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let _ = manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
    drop(bus);
    let snapshot = monitor.snapshot();
    assert_eq!(snapshot.outstanding_count, 0);
    assert_eq!(snapshot.outstanding_weight_bytes, 0);
}

#[test]
fn fanout_and_repeated_attempts_count_each_rejected_target() {
    let (bus, monitor) = EventBus::local_with_monitor(LocalEventBusConfig::new().max_total_outstanding(1)).unwrap();
    let topic = Topic::<u32>::new("local.capacity.monitor.fanout").unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = std::sync::Arc::new(std::sync::Mutex::new(release_rx));
    let handler = {
        let entered_tx = entered_tx.clone();
        let release_rx = release_rx.clone();
        move |_| {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }
    };
    let first = bus
        .subscribe(SubscribeRequest::new("fanout-one", topic.clone()).unwrap(), handler)
        .unwrap();
    let second = bus
        .subscribe(SubscribeRequest::new("fanout-two", topic.clone()).unwrap(), move |_| {
            entered_tx.send(()).unwrap();
            // Only the accepted callback blocks. A rejected destination
            // never invokes its handler.
            release_rx.lock().unwrap().recv().unwrap();
        })
        .unwrap();
    let first_receipt = bus.publish(PublishRequest::new(topic.clone(), 1).unwrap()).unwrap();
    let PublishAcknowledgement::DestinationAdmissions(admissions) = first_receipt.acknowledgement() else {
        panic!("local provider reports destination admissions");
    };
    assert_eq!(
        admissions
            .iter()
            .filter(|item| item.rejection_kind() == Some(AdmissionRejectionKind::ProviderCountCapacity))
            .count(),
        1
    );
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(monitor.snapshot().rejected_provider_count_capacity, 1);

    for attempt in 2..=3 {
        let receipt = bus
            .publish(PublishRequest::new(topic.clone(), attempt).unwrap())
            .unwrap();
        let PublishAcknowledgement::DestinationAdmissions(items) = receipt.acknowledgement() else {
            panic!("local provider reports destination admissions");
        };
        assert_eq!(
            items
                .iter()
                .filter(|item| item.rejection_kind() == Some(AdmissionRejectionKind::ProviderCountCapacity))
                .count(),
            2
        );
    }
    assert_eq!(monitor.snapshot().rejected_provider_count_capacity, 5);
    release_tx.send(()).unwrap();
    drop((first, second));
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
    drop(bus);
    assert!(monitor.snapshot().outstanding_count <= 1);
}

struct DropProbe(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn monitor_does_not_keep_payload_alive_after_provider_shutdown() {
    let (bus, monitor) = EventBus::local_with_monitor(LocalEventBusConfig::new()).unwrap();
    let topic = Topic::<DropProbe>::new("local.capacity.monitor.drop").unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("drop-probe", topic.clone()).unwrap(),
            move |delivery| {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
                drop(delivery);
            },
        )
        .unwrap();
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _ = bus
        .publish(PublishRequest::new(topic, DropProbe(dropped.clone())).unwrap())
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
    release_tx.send(()).unwrap();
    drop(subscription);
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
    drop(bus);
    assert_eq!(monitor.snapshot().outstanding_count, 0);
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
}

// There is no deterministic public closed-target test: local subscription
// close unregisters its queue before the next publish. Only the race between
// close and an already enumerated destination reaches that rejection branch.

struct StopWritersOnDrop(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Drop for StopWritersOnDrop {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

#[test]
fn concurrent_publication_snapshots_never_exceed_provider_count_limit() {
    let limit = 8;
    let weight_limit = NonZeroUsize::new(limit * 5).unwrap();
    let config = LocalEventBusConfig::new()
        .max_total_outstanding(limit)
        .max_total_outstanding_weight_bytes(weight_limit);
    let (bus, monitor) = EventBus::local_with_monitor(config).unwrap();
    let topic = Topic::<u32>::new("local.capacity.monitor.concurrent").unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    let first_delivery = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let first_delivery_for_handler = first_delivery.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("concurrent-monitor", topic.clone()).unwrap(),
            move |_| {
                if first_delivery_for_handler.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    entered_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                }
            },
        )
        .unwrap();
    let weighted_publish = |payload| {
        PublishRequest::builder()
            .topic(topic.clone())
            .payload(payload)
            .options(
                PublishOptions::builder()
                    .native_payload_weight(|_| NonZeroUsize::new(5).unwrap())
                    .build(),
            )
            .build()
            .unwrap()
    };
    let _ = bus.publish(weighted_publish(0)).unwrap();
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _stop_on_panic = StopWritersOnDrop(stop.clone());
    let (active_tx, active_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let mut writers = Vec::new();
    for writer in 0..4 {
        let bus = bus.clone();
        let topic = topic.clone();
        let stop = stop.clone();
        let active_tx = active_tx.clone();
        let done_tx = done_tx.clone();
        writers.push(thread::spawn(move || {
            let publish = |payload| {
                let _ = bus
                    .publish(
                        PublishRequest::builder()
                            .topic(topic.clone())
                            .payload(payload)
                            .options(
                                PublishOptions::builder()
                                    .native_payload_weight(|_| NonZeroUsize::new(5).unwrap())
                                    .build(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .unwrap();
            };
            publish(writer * 10_000);
            active_tx.send(()).unwrap();
            let mut item = 0;
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                publish(writer * 10_000 + item + 1);
                item += 1;
            }
            done_tx.send(()).unwrap();
        }));
    }
    drop(active_tx);
    drop(done_tx);
    for _ in 0..4 {
        active_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("each writer enters its publish loop");
    }
    for _ in 0..32 {
        let snapshot = monitor.snapshot();
        assert!(snapshot.outstanding_count <= snapshot.max_total_outstanding);
        assert!(snapshot.outstanding_weight_bytes <= weight_limit.get());
    }
    stop.store(true, std::sync::atomic::Ordering::Release);
    for _ in 0..4 {
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("each writer observes stop and exits");
    }
    for writer in writers {
        writer.join().unwrap();
    }
    assert!(monitor.snapshot().outstanding_count <= limit);
    release_tx.send(()).unwrap();
    drop(subscription);
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
}
mod manual_async {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread::Thread;
    use std::thread::current;
    use std::thread::park;

    struct ThreadWake(Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
        let waker = Waker::from(Arc::new(ThreadWake(current())));
        let mut context = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => park(),
            }
        }
    }
}
