use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::mpsc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::{AdmissionRejectionKind, PublishAcknowledgement, PublishRequest, SubscribeRequest, Topic};
use qubit_event_bus::spi::ShutdownMode;

fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("construction must complete immediately"),
    }
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
    let (bus, monitor) = EventBus::local_with_monitor(config).unwrap();
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
