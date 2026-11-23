// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::TypeId;
use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use qubit_spi::ServiceProvider;

struct PanicOnDrop(Arc<AtomicBool>);

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        if !self.0.swap(true, Ordering::SeqCst) {
            panic!("payload destructor panic");
        }
    }
}

struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn request(id: u64, name: &'static str) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new(name).expect("valid topic"),
        SubscriberId::new(format!("panic-{id}")).expect("valid subscriber"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<PanicOnDrop>(),
    )
}

fn message() -> (OutboundMessage, Arc<AtomicBool>) {
    let dropped = Arc::new(AtomicBool::new(false));
    (weighted_message(Arc::clone(&dropped), "panic-event"), dropped)
}

fn weighted_message(dropped: Arc<AtomicBool>, id: &str) -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("local.close.panic").expect("valid topic"),
        EventId::new(id).expect("valid event id"),
        SystemTime::UNIX_EPOCH,
        BTreeMap::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(PanicOnDrop(dropped))),
    )
    .with_native_payload_weight_bytes(NonZeroUsize::new(7).expect("positive weight"))
}

fn non_panicking_message(id: &str) -> OutboundMessage {
    let already_dropped = Arc::new(AtomicBool::new(true));
    weighted_message(already_dropped, id)
}

fn poll<F: Future>(future: F, waker: &Waker) -> Poll<F::Output> {
    pin!(future).as_mut().poll(&mut Context::from_waker(waker))
}

fn sync_spi() -> Arc<dyn EventBusSpi> {
    LocalEventBusProvider
        .create_configured(
            &EventBusConfig::default().with_provider_options(
                LocalEventBusConfig::new()
                    .queue_capacity(4)
                    .max_total_outstanding(1)
                    .max_total_outstanding_weight_bytes(NonZeroUsize::new(7).expect("positive weight"))
                    .provider_options(),
            ),
        )
        .expect("valid sync SPI")
}

fn assert_payload_was_released(dropped: &AtomicBool) {
    assert!(
        dropped.load(Ordering::SeqCst),
        "close must release the final payload owner"
    );
}

#[test]
fn test_sync_close_unregisters_and_notifies_before_payload_drop_panics() {
    let spi = sync_spi();
    let mut receiver = spi.subscribe(request(77, "local.close.panic")).expect("subscription");
    let (message, dropped) = message();
    let _acknowledgement = spi.publish(message).expect("publish");

    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| receiver.close())).is_err());
    assert_payload_was_released(&dropped);
    assert!(
        spi.subscribe(request(77, "local.close.panic")).is_ok(),
        "close must unregister before payload drop"
    );
    assert!(
        receiver.close().is_ok(),
        "repeated close remains safe after destructor panic"
    );
    let mut followup = spi
        .subscribe(request(78, "local.close.panic"))
        .expect("follow-up subscription");
    let followup_message = OutboundMessage::new(
        TopicAddress::new("local.close.panic").expect("valid topic"),
        EventId::new("follow-up").expect("valid event id"),
        SystemTime::UNIX_EPOCH,
        BTreeMap::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(PanicOnDrop(Arc::clone(&dropped)))),
    )
    .with_native_payload_weight_bytes(NonZeroUsize::new(7).expect("positive weight"));
    assert!(
        matches!(
            spi.publish(followup_message),
            Ok(qubit_event_bus::model::PublishAcknowledgement::DestinationAdmissions(ref admissions))
                if admissions.iter().any(|admission| *admission.status() == qubit_event_bus::model::AdmissionStatus::Accepted)
        ),
        "count and weight reservations were released before payload destruction"
    );
    assert!(followup.close().is_ok());
    assert_eq!(
        ShutdownOutcome::Complete,
        spi.shutdown(ShutdownMode::Immediate)
            .expect("shutdown after close panic")
    );
}

#[test]
fn test_async_close_wakes_shutdown_and_unregisters_before_payload_drop_panics() {
    let config = LocalEventBusConfig::new()
        .max_total_outstanding(4)
        .max_total_outstanding_weight_bytes(NonZeroUsize::new(7).expect("positive weight"));
    let spi = AsyncLocalEventBusSpi::new(&config).expect("valid async SPI");
    let mut receiver = match poll(spi.subscribe(request(78, "local.close.panic")), Waker::noop()) {
        Poll::Ready(Ok(receiver)) => receiver,
        _ => panic!("subscribe completes immediately"),
    };
    let wake_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(wake_counter.clone());
    let (message, dropped) = message();
    assert!(matches!(poll(spi.publish(message), Waker::noop()), Poll::Ready(Ok(_))));
    let mut shutdown = Box::pin(spi.shutdown(ShutdownMode::Graceful { timeout: Duration::MAX }));
    assert!(shutdown.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| poll(receiver.close(), &waker)));
    assert!(result.is_err(), "payload panic must propagate from close");
    assert_payload_was_released(&dropped);
    assert!(
        wake_counter.0.load(Ordering::SeqCst) > 0,
        "close wakes graceful shutdown"
    );
    assert!(
        matches!(poll(receiver.close(), Waker::noop()), Poll::Ready(Ok(()))),
        "repeated close remains safe after destructor panic"
    );
    assert!(shutdown.as_mut().poll(&mut Context::from_waker(&waker)).is_ready());
    assert!(
        matches!(
            poll(spi.shutdown(ShutdownMode::Immediate), Waker::noop()),
            Poll::Ready(Ok(_))
        ),
        "shutdown completes after a panicking close"
    );
}

#[test]
fn test_sync_shutdown_finishes_queue_before_payload_drop_panics() {
    let spi = sync_spi();
    let mut receiver = spi.subscribe(request(81, "local.close.panic")).expect("subscription");
    let (message, dropped) = message();
    let _acknowledgement = spi.publish(message).expect("publish");

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| spi.shutdown(ShutdownMode::Immediate)));
    assert!(result.is_err(), "payload panic must propagate from shutdown");
    assert_payload_was_released(&dropped);
    assert!(matches!(
        receiver.receive(Duration::ZERO),
        Ok(qubit_event_bus::spi::ReceiveOutcome::Closed)
    ));
    assert_eq!(
        ShutdownOutcome::Complete,
        spi.shutdown(ShutdownMode::Graceful {
            timeout: Duration::ZERO,
        })
        .expect("shutdown outcome was saved before payload destruction")
    );
}

#[test]
fn test_sync_close_discards_in_flight_payload_after_owner_drop() {
    let spi = sync_spi();
    let mut receiver = spi.subscribe(request(82, "local.close.panic")).expect("subscription");
    let (message, dropped) = message();
    let _acknowledgement = spi.publish(message).expect("publish");
    let delivery = receiver.receive(Duration::ZERO).expect("receive");
    assert!(matches!(&delivery, ReceiveOutcome::Message(_)));
    drop(delivery);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| receiver.close()));
    assert!(result.is_err(), "in-flight payload destructor panic propagates");
    assert_payload_was_released(&dropped);
    let mut replacement = spi.subscribe(request(83, "local.close.panic")).expect("replacement");
    let followup = weighted_message(Arc::clone(&dropped), "sync-in-flight-follow-up");
    assert!(
        matches!(
            spi.publish(followup),
            Ok(qubit_event_bus::model::PublishAcknowledgement::DestinationAdmissions(ref admissions))
                if admissions.iter().any(|admission| *admission.status() == qubit_event_bus::model::AdmissionStatus::Accepted)
        ),
        "in-flight count and weight reservations release exactly once"
    );
    assert!(replacement.close().is_ok());
}

#[test]
fn test_async_close_discards_in_flight_payload_after_owner_drop() {
    let config = LocalEventBusConfig::new()
        .max_total_outstanding(1)
        .max_total_outstanding_weight_bytes(NonZeroUsize::new(7).expect("positive weight"));
    let spi = AsyncLocalEventBusSpi::new(&config).expect("valid async SPI");
    let mut receiver = match poll(spi.subscribe(request(84, "local.close.panic")), Waker::noop()) {
        Poll::Ready(Ok(receiver)) => receiver,
        _ => panic!("subscribe completes immediately"),
    };
    let (message, dropped) = message();
    assert!(matches!(poll(spi.publish(message), Waker::noop()), Poll::Ready(Ok(_))));
    let delivery = match poll(receiver.receive(Duration::ZERO), Waker::noop()) {
        Poll::Ready(Ok(ReceiveOutcome::Message(delivery))) => delivery,
        _ => panic!("delivery is immediately available"),
    };
    drop(delivery);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| poll(receiver.close(), Waker::noop())));
    assert!(result.is_err(), "in-flight payload destructor panic propagates");
    assert_payload_was_released(&dropped);
    let mut replacement = match poll(spi.subscribe(request(85, "local.close.panic")), Waker::noop()) {
        Poll::Ready(Ok(receiver)) => receiver,
        _ => panic!("close unregisters before dropping the in-flight payload"),
    };
    let followup = weighted_message(Arc::clone(&dropped), "async-in-flight-follow-up");
    assert!(
        matches!(
            poll(spi.publish(followup), Waker::noop()),
            Poll::Ready(Ok(qubit_event_bus::model::PublishAcknowledgement::DestinationAdmissions(ref admissions)))
                if admissions.iter().any(|admission| *admission.status() == qubit_event_bus::model::AdmissionStatus::Accepted)
        ),
        "in-flight count and weight reservations release exactly once"
    );
    assert!(matches!(poll(replacement.close(), Waker::noop()), Poll::Ready(Ok(()))));
}

#[test]
fn test_sync_drop_fallback_finishes_close_before_payload_drop_panics() {
    let spi = sync_spi();
    let receiver = spi.subscribe(request(86, "local.close.panic")).expect("subscription");
    let (message, dropped) = message();
    let _acknowledgement = spi.publish(message).expect("publish");

    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(receiver))).is_err());
    assert_payload_was_released(&dropped);
    assert!(
        spi.subscribe(request(86, "local.close.panic")).is_ok(),
        "Drop fallback unregisters first"
    );
}

#[test]
fn test_async_drop_fallback_wakes_shutdown_before_payload_drop_panics() {
    let config = LocalEventBusConfig::new()
        .max_total_outstanding(1)
        .max_total_outstanding_weight_bytes(NonZeroUsize::new(7).expect("positive weight"));
    let spi = AsyncLocalEventBusSpi::new(&config).expect("valid async SPI");
    let receiver = match poll(spi.subscribe(request(87, "local.close.panic")), Waker::noop()) {
        Poll::Ready(Ok(receiver)) => receiver,
        _ => panic!("subscribe completes immediately"),
    };
    let (message, dropped) = message();
    assert!(matches!(poll(spi.publish(message), Waker::noop()), Poll::Ready(Ok(_))));
    let wake_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(wake_counter.clone());
    let mut shutdown = Box::pin(spi.shutdown(ShutdownMode::Graceful { timeout: Duration::MAX }));
    assert!(shutdown.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(receiver))).is_err());
    assert_payload_was_released(&dropped);
    assert!(
        wake_counter.0.load(Ordering::SeqCst) > 0,
        "Drop fallback wakes graceful shutdown"
    );
    assert!(shutdown.as_mut().poll(&mut Context::from_waker(&waker)).is_ready());
}

#[test]
fn test_async_shutdown_finishes_state_before_payload_drop_panics() {
    let config = LocalEventBusConfig::new()
        .max_total_outstanding(4)
        .max_total_outstanding_weight_bytes(NonZeroUsize::new(7).expect("positive weight"));
    let spi = AsyncLocalEventBusSpi::new(&config).expect("valid async SPI");
    let _receiver = match poll(spi.subscribe(request(79, "local.close.panic")), Waker::noop()) {
        Poll::Ready(Ok(receiver)) => receiver,
        _ => panic!("subscribe completes immediately"),
    };
    let mut waiter = match poll(spi.subscribe(request(80, "local.close.waiter")), Waker::noop()) {
        Poll::Ready(Ok(receiver)) => receiver,
        _ => panic!("waiter subscribe completes immediately"),
    };
    let (message, dropped) = message();
    assert!(matches!(poll(spi.publish(message), Waker::noop()), Poll::Ready(Ok(_))));
    let wake_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(wake_counter.clone());
    let mut receive = Box::pin(waiter.receive(Duration::MAX));
    assert!(receive.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        poll(spi.shutdown(ShutdownMode::Immediate), &waker)
    }));
    assert!(result.is_err(), "payload panic must propagate from shutdown");
    assert_payload_was_released(&dropped);
    assert!(
        wake_counter.0.load(Ordering::SeqCst) > 0,
        "shutdown wakes pending receive"
    );
    drop(receive);
    assert!(poll(spi.shutdown(ShutdownMode::Immediate), Waker::noop()).is_ready());
    assert!(poll(waiter.receive(Duration::ZERO), Waker::noop()).is_ready());
}

#[test]
fn test_sync_close_releases_each_panicking_payload_in_isolated_process() {
    const CHILD: &str = "LOCAL_CLOSE_PANIC_BATCH_CHILD";
    if std::env::var_os(CHILD).is_some() {
        run_batch_case(std::env::var("LOCAL_CLOSE_PANIC_BATCH_CASE").expect("case"));
        return;
    }
    for case in [
        "sync_close",
        "sync_drop",
        "sync_shutdown",
        "async_close",
        "async_drop",
        "async_shutdown",
        "unwind_drop",
    ] {
        let status = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "test_sync_close_releases_each_panicking_payload_in_isolated_process",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("LOCAL_CLOSE_PANIC_BATCH_CASE", case)
            .status()
            .expect("spawn isolated regression child");
        assert!(status.success(), "{case} batch cleanup child must survive: {status}");
    }
}

fn batch_spi_config() -> LocalEventBusConfig {
    LocalEventBusConfig::new()
        .max_total_outstanding(2)
        .max_total_outstanding_weight_bytes(NonZeroUsize::new(14).expect("positive weight"))
}

fn publish_two_sync(spi: &dyn EventBusSpi) -> [Arc<AtomicBool>; 2] {
    let (first, first_dropped) = message();
    let (second, second_dropped) = message();
    let _first_ack = spi.publish(first).expect("first publish");
    let _second_ack = spi.publish(second).expect("second publish");
    [first_dropped, second_dropped]
}

fn publish_two_async(spi: &AsyncLocalEventBusSpi) -> [Arc<AtomicBool>; 2] {
    let (first, first_dropped) = message();
    let (second, second_dropped) = message();
    assert!(matches!(poll(spi.publish(first), Waker::noop()), Poll::Ready(Ok(_))));
    assert!(matches!(poll(spi.publish(second), Waker::noop()), Poll::Ready(Ok(_))));
    [first_dropped, second_dropped]
}

fn assert_both_payloads_released(dropped: &[Arc<AtomicBool>; 2]) {
    for observed in dropped {
        assert_payload_was_released(observed);
    }
}

fn run_batch_case(case: String) {
    if case == "unwind_drop" {
        let spi = sync_spi();
        let receiver = spi.subscribe(request(904, "local.close.panic")).expect("subscription");
        let (message, dropped) = message();
        let _ack = spi.publish(message).expect("publish");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _receiver = receiver;
            panic!("original unwind must remain primary");
        }));
        let original = result.expect_err("outer panic is preserved");
        assert_eq!(
            original.downcast_ref::<&str>(),
            Some(&"original unwind must remain primary")
        );
        assert_payload_was_released(&dropped);
        return;
    }
    match case.as_str() {
        "sync_close" | "sync_drop" | "sync_shutdown" => {
            let spi = LocalEventBusProvider
                .create_configured(
                    &EventBusConfig::default().with_provider_options(batch_spi_config().provider_options()),
                )
                .expect("valid sync SPI");
            let mut receiver = spi.subscribe(request(901, "local.close.panic")).expect("subscription");
            let dropped = publish_two_sync(spi.as_ref());
            match case.as_str() {
                "sync_close" => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| receiver.close()));
                    assert!(result.is_err(), "first close destructor panic propagates");
                    assert!(receiver.close().is_ok(), "repeated close returns its completed result");
                    let mut replacement = spi.subscribe(request(905, "local.close.panic")).expect("replacement");
                    for id in ["sync-close-capacity-a", "sync-close-capacity-b"] {
                        assert!(
                            matches!(spi.publish(non_panicking_message(id)), Ok(qubit_event_bus::model::PublishAcknowledgement::DestinationAdmissions(ref admissions)) if admissions.iter().any(|a| a.status() == &qubit_event_bus::model::AdmissionStatus::Accepted)),
                            "close returns count and weight capacity"
                        );
                    }
                    replacement.close().expect("replacement close");
                }
                "sync_drop" => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(receiver)));
                    assert!(result.is_err(), "first Drop destructor panic propagates");
                    let mut replacement = spi.subscribe(request(906, "local.close.panic")).expect("replacement");
                    for id in ["sync-drop-capacity-a", "sync-drop-capacity-b"] {
                        assert!(
                            matches!(spi.publish(non_panicking_message(id)), Ok(qubit_event_bus::model::PublishAcknowledgement::DestinationAdmissions(ref admissions)) if admissions.iter().any(|a| a.status() == &qubit_event_bus::model::AdmissionStatus::Accepted)),
                            "Drop returns count and weight capacity"
                        );
                    }
                    replacement.close().expect("replacement close");
                }
                "sync_shutdown" => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        spi.shutdown(ShutdownMode::Immediate)
                    }));
                    assert!(result.is_err(), "first shutdown destructor panic propagates");
                    assert_eq!(
                        spi.shutdown(ShutdownMode::Graceful {
                            timeout: Duration::ZERO
                        })
                        .unwrap(),
                        ShutdownOutcome::Complete,
                        "shutdown outcome is cached"
                    );
                    assert!(
                        matches!(receiver.receive(Duration::ZERO), Ok(ReceiveOutcome::Closed)),
                        "shutdown closes receiver"
                    );
                }
                _ => unreachable!(),
            }
            assert_both_payloads_released(&dropped);
        }
        "async_close" | "async_drop" | "async_shutdown" => {
            let spi = AsyncLocalEventBusSpi::new(&batch_spi_config()).expect("valid async SPI");
            let receiver = poll(spi.subscribe(request(902, "local.close.panic")), Waker::noop());
            let mut receiver = match receiver {
                Poll::Ready(Ok(receiver)) => receiver,
                _ => panic!("subscription is immediate"),
            };
            let dropped = publish_two_async(&spi);
            let wake_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
            let waker = Waker::from(wake_counter.clone());
            match case.as_str() {
                "async_close" => {
                    let mut shutdown = Box::pin(spi.shutdown(ShutdownMode::Graceful { timeout: Duration::MAX }));
                    assert!(shutdown.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
                    let result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| poll(receiver.close(), &waker)));
                    assert!(result.is_err(), "first close destructor panic propagates");
                    assert!(
                        wake_counter.0.load(Ordering::SeqCst) > 0,
                        "close notifies shutdown waiter"
                    );
                    assert!(
                        matches!(poll(receiver.close(), Waker::noop()), Poll::Ready(Ok(()))),
                        "repeated close is cached"
                    );
                    assert!(
                        shutdown.as_mut().poll(&mut Context::from_waker(&waker)).is_ready(),
                        "graceful shutdown completes"
                    );
                }
                "async_drop" => {
                    let mut shutdown = Box::pin(spi.shutdown(ShutdownMode::Graceful { timeout: Duration::MAX }));
                    assert!(shutdown.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(receiver)));
                    assert!(result.is_err(), "first Drop destructor panic propagates");
                    assert!(
                        wake_counter.0.load(Ordering::SeqCst) > 0,
                        "Drop fallback notifies shutdown waiter"
                    );
                    assert!(shutdown.as_mut().poll(&mut Context::from_waker(&waker)).is_ready());
                }
                "async_shutdown" => {
                    let mut waiter = match poll(spi.subscribe(request(903, "local.close.waiter")), Waker::noop()) {
                        Poll::Ready(Ok(receiver)) => receiver,
                        _ => panic!("waiter subscription is immediate"),
                    };
                    let mut receive = Box::pin(waiter.receive(Duration::MAX));
                    assert!(receive.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        poll(spi.shutdown(ShutdownMode::Immediate), &waker)
                    }));
                    assert!(result.is_err(), "first shutdown destructor panic propagates");
                    assert!(
                        wake_counter.0.load(Ordering::SeqCst) > 0,
                        "shutdown notifies receive waiter"
                    );
                    drop(receive);
                    assert!(
                        poll(spi.shutdown(ShutdownMode::Immediate), Waker::noop()).is_ready(),
                        "shutdown outcome is cached"
                    );
                    assert!(
                        poll(waiter.receive(Duration::ZERO), Waker::noop()).is_ready(),
                        "waiter observes closure"
                    );
                }
                _ => unreachable!(),
            }
            assert_both_payloads_released(&dropped);
        }
        _ => panic!("unknown batch case: {case}"),
    }
}
