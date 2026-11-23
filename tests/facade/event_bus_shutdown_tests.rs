// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public shutdown request and reusable generation ticket contracts.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Wake;
use std::task::Waker;
use std::thread::spawn;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusShutdown;
use qubit_event_bus::ShutdownError;
use qubit_event_bus::ShutdownReport;
use qubit_event_bus::Subscription;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

const LIMIT: Duration = Duration::from_secs(5);

type ShutdownFuture = Pin<Box<dyn Future<Output = Result<ShutdownReport, ShutdownError>> + Send>>;
type WakerCallback = Box<dyn FnOnce() + Send>;

struct CoordinatorWakerProbe {
    on_clone: Mutex<Option<WakerCallback>>,
    on_drop: Mutex<Option<WakerCallback>>,
}

impl Drop for CoordinatorWakerProbe {
    fn drop(&mut self) {
        if let Some(callback) = self.on_drop.get_mut().expect("drop callback lock").take() {
            callback();
        }
    }
}

unsafe fn clone_coordinator_probe(pointer: *const ()) -> RawWaker {
    // SAFETY: Each raw waker owns one Arc reference represented by pointer.
    let probe = unsafe { &*pointer.cast::<CoordinatorWakerProbe>() };
    if let Some(callback) = probe.on_clone.lock().expect("clone callback lock").take() {
        callback();
    }
    // SAFETY: The returned RawWaker owns one newly incremented Arc reference.
    unsafe { Arc::<CoordinatorWakerProbe>::increment_strong_count(pointer.cast()) };
    RawWaker::new(pointer, &COORDINATOR_PROBE_VTABLE)
}

unsafe fn drop_coordinator_probe(pointer: *const ()) {
    // SAFETY: This consumes the Arc reference owned by the raw waker.
    drop(unsafe { Arc::<CoordinatorWakerProbe>::from_raw(pointer.cast()) });
}

unsafe fn wake_coordinator_probe(pointer: *const ()) {
    // SAFETY: wake consumes the Arc reference just like drop.
    unsafe { drop_coordinator_probe(pointer) };
}

unsafe fn wake_coordinator_probe_by_ref(_: *const ()) {}

static COORDINATOR_PROBE_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_coordinator_probe,
    wake_coordinator_probe,
    wake_coordinator_probe_by_ref,
    drop_coordinator_probe,
);

fn coordinator_probe_waker(
    on_clone: Option<WakerCallback>,
    on_drop: Option<WakerCallback>,
) -> (Waker, Arc<CoordinatorWakerProbe>) {
    let probe = Arc::new(CoordinatorWakerProbe {
        on_clone: Mutex::new(on_clone),
        on_drop: Mutex::new(on_drop),
    });
    let pointer = Arc::into_raw(probe.clone()).cast();
    // SAFETY: The vtable owns and releases the Arc reference represented by
    // pointer.
    let waker = unsafe { Waker::from_raw(RawWaker::new(pointer, &COORDINATOR_PROBE_VTABLE)) };
    (waker, probe)
}

fn blocked_shutdown_ticket() -> (Arc<EventBusShutdown>, Gate, Subscription) {
    let (bus, gate, subscription) = blocked_bus();
    let ticket = Arc::new(bus.request_shutdown(ShutdownMode::Immediate).expect("ticket"));
    (ticket, gate, subscription)
}

/// Releases the handler even when a test unwinds.
struct Gate(Arc<(Mutex<bool>, Condvar)>);

impl Gate {
    /// Allows the blocked handler to finish.
    fn release(&self) {
        let (lock, changed) = &*self.0;
        *lock.lock().expect("gate lock") = true;
        changed.notify_all();
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        self.release();
    }
}

/// Creates a real local handler blocked until the returned gate is released.
fn blocked_bus() -> (EventBus, Gate, Subscription) {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("local bus");
    let topic = Topic::<String>::new("shutdown.requests").expect("topic");
    let gate = Gate(Arc::new((Mutex::new(false), Condvar::new())));
    let handler_gate = gate.0.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("shutdown-handler", topic.clone()).expect("request"),
            move |_| {
                entered_tx.send(()).expect("entered signal");
                let (lock, changed) = &*handler_gate;
                let mut released = lock.lock().expect("gate lock");
                while !*released {
                    released = changed.wait(released).expect("gate wait");
                }
            },
        )
        .expect("subscription");
    let _ = bus
        .publish(PublishRequest::new(topic, "payload".to_owned()).expect("publish request"))
        .expect("publish");
    entered_rx.recv_timeout(LIMIT).expect("handler entered");
    (bus, gate, subscription)
}

/// Sends a notification whenever the test executor is woken.
struct Signal(mpsc::Sender<()>);

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(());
    }
}

/// Polls a pinned observer with the supplied waker.
fn poll<F: Future + ?Sized>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[test]
fn test_request_returns_before_handler_gate_releases() {
    let (bus, gate, _subscription) = blocked_bus();
    let (returned_tx, returned_rx) = mpsc::channel();
    let caller = spawn(move || {
        let result = bus.request_shutdown(ShutdownMode::Immediate);
        returned_tx.send(result).expect("request result");
    });
    let returned = returned_rx.recv_timeout(LIMIT);
    gate.release();
    caller.join().expect("caller joins");
    let ticket = returned
        .expect("request must return while handler remains blocked")
        .expect("ticket");
    let report = ticket.wait(Some(LIMIT)).expect("shutdown after release");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
}

#[test]
fn test_pending_observers_cancel_independently_and_timeout_preserves_ticket() {
    let (bus, gate, _subscription) = blocked_bus();
    let ticket = bus
        .request_shutdown(ShutdownMode::Graceful { timeout: LIMIT })
        .expect("ticket");
    let strengthened = bus.request_shutdown(ShutdownMode::Immediate).expect("joined ticket");
    drop(strengthened);
    assert!(matches!(
        ticket.wait(Some(Duration::ZERO)),
        Err(ShutdownError::TimedOut { .. })
    ));
    let (tx, rx) = mpsc::channel();
    let waker = Waker::from(Arc::new(Signal(tx)));
    let mut cancelled = Box::pin(ticket.wait_async());
    let mut observer = Box::pin(ticket.wait_async());
    assert!(poll(cancelled.as_mut(), &waker).is_pending());
    assert!(poll(observer.as_mut(), &waker).is_pending());
    drop(cancelled);
    gate.release();
    rx.recv_timeout(LIMIT).expect("remaining observer wakes");
    assert!(matches!(poll(observer.as_mut(), &waker), Poll::Ready(Ok(_))));
    drop(observer);
    let report = ticket.wait(Some(LIMIT)).expect("repeated sync wait");
    let mut again = Box::pin(ticket.wait_async());
    assert!(matches!(poll(again.as_mut(), &waker), Poll::Ready(Ok(value)) if value == report));
}

#[test]
fn test_max_timeout_waits_for_completion_and_ticket_remains_reusable() {
    let (bus, gate, _subscription) = blocked_bus();
    let ticket = bus.request_shutdown(ShutdownMode::Immediate).expect("ticket");
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let waiter = spawn(move || {
        started_tx.send(()).expect("send started signal");
        let first = ticket.wait(Some(Duration::MAX));
        let second = ticket.wait(Some(Duration::MAX));
        result_tx.send((first, second)).expect("send wait results");
    });

    started_rx.recv_timeout(LIMIT).expect("ticket waiter starts");
    gate.release();

    let (first, second) = result_rx.recv_timeout(LIMIT).expect("ticket waiter completes");
    waiter.join().expect("ticket waiter joins");
    let report = first.expect("completion within finite budget");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(second.expect("reused ticket"), report);
}

#[test]
fn test_finish_before_first_poll_and_closed_ready_ticket() {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("local bus");
    let ticket = bus.request_shutdown(ShutdownMode::Immediate).expect("ticket");
    let report = ticket.wait(Some(LIMIT)).expect("completed");
    let mut future = Box::pin(ticket.wait_async());
    assert!(matches!(poll(future.as_mut(), Waker::noop()), Poll::Ready(Ok(value)) if value == report));
    let ready = bus.request_shutdown(ShutdownMode::Immediate).expect("ready ticket");
    assert_eq!(ready.wait(Some(Duration::ZERO)).expect("cached"), report);
    let mut future = Box::pin(ready.wait_async());
    assert!(matches!(poll(future.as_mut(), Waker::noop()), Poll::Ready(Ok(value)) if value == report));
}

#[test]
fn test_request_from_handler_is_allowed_but_sync_wait_would_deadlock() {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("bus");
    let topic = Topic::<String>::new("shutdown.reentrant").expect("topic");
    let callback_bus = bus.clone();
    let (tx, rx) = mpsc::channel();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("reentrant", topic.clone()).expect("request"),
            move |_| {
                let ticket = callback_bus
                    .request_shutdown(ShutdownMode::Immediate)
                    .expect("nonblocking request");
                let rejected = matches!(ticket.wait(Some(Duration::ZERO)), Err(ShutdownError::Lifecycle(_)));
                tx.send((ticket, rejected)).expect("send ticket");
            },
        )
        .expect("subscribe");
    let _ = bus
        .publish(PublishRequest::new(topic, "payload".to_owned()).expect("request"))
        .expect("publish");
    let (ticket, rejected) = rx.recv_timeout(LIMIT).expect("callback returns");
    assert!(rejected);
    let report = ticket.wait(Some(LIMIT)).expect("external wait");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
}

#[test]
fn test_async_wait_from_handler_is_rejected_but_ticket_remains_observable() {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("bus");
    let topic = Topic::<String>::new("shutdown.async-reentrant").expect("topic");
    let callback_bus = bus.clone();
    let (tx, rx) = mpsc::channel();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("async-reentrant", topic.clone()).expect("request"),
            move |_| {
                let ticket = callback_bus
                    .request_shutdown(ShutdownMode::Immediate)
                    .expect("nonblocking request");
                let mut future = Box::pin(ticket.wait_async());
                let rejected = matches!(
                    poll(future.as_mut(), Waker::noop()),
                    Poll::Ready(Err(ShutdownError::Lifecycle(
                        qubit_event_bus::LifecycleError::WouldDeadlock { operation: "shutdown" }
                    )))
                );
                drop(future);
                tx.send((ticket, rejected)).expect("send ticket and observation");
            },
        )
        .expect("subscribe");
    let _ = bus
        .publish(PublishRequest::new(topic, "payload".to_owned()).expect("request"))
        .expect("publish");
    let (ticket, rejected) = rx.recv_timeout(LIMIT).expect("callback returns");
    assert!(rejected, "same-bus async observation must be rejected");
    let report = ticket.wait(Some(LIMIT)).expect("external wait");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
}

#[test]
fn test_pending_async_wait_repolled_from_handler_is_rejected_and_ticket_remains_observable() {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("bus");
    let topic = Topic::<String>::new("shutdown.async-repoll").expect("topic");
    let callback_bus = bus.clone();
    let (ticket_tx, ticket_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let resume_rx = Arc::new(Mutex::new(resume_rx));
    let (rejected_tx, rejected_rx) = mpsc::channel();
    let future_slot = Arc::new(Mutex::new(None::<ShutdownFuture>));
    let callback_slot = future_slot.clone();
    let callback_resume = resume_rx.clone();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("async-repoll", topic.clone()).expect("request"),
            move |_| {
                let ticket = Arc::new(
                    callback_bus
                        .request_shutdown(ShutdownMode::Immediate)
                        .expect("nonblocking request"),
                );
                ticket_tx.send(ticket).expect("send ticket to external observer");
                callback_resume
                    .lock()
                    .expect("resume receiver lock")
                    .recv()
                    .expect("external observer permits callback poll");
                let mut future = callback_slot
                    .lock()
                    .expect("future slot")
                    .take()
                    .expect("pending future");
                let rejected = matches!(
                    poll(future.as_mut(), Waker::noop()),
                    Poll::Ready(Err(ShutdownError::Lifecycle(
                        qubit_event_bus::LifecycleError::WouldDeadlock { operation: "shutdown" }
                    )))
                );
                rejected_tx.send(rejected).expect("send nested poll result");
            },
        )
        .expect("subscribe");
    let _ = bus
        .publish(PublishRequest::new(topic, "payload".to_owned()).expect("request"))
        .expect("publish");

    let ticket = ticket_rx.recv_timeout(LIMIT).expect("handler starts shutdown");
    let future_ticket = ticket.clone();
    let mut future: ShutdownFuture = Box::pin(async move { future_ticket.wait_async().await });
    assert!(poll(future.as_mut(), Waker::noop()).is_pending());
    *future_slot.lock().expect("future slot") = Some(future);
    resume_tx.send(()).expect("resume handler");

    assert!(rejected_rx.recv_timeout(LIMIT).expect("nested poll completes"));
    let report = ticket.wait(Some(LIMIT)).expect("external wait observes completion");
    let mut fresh = Box::pin(ticket.wait_async());
    assert!(matches!(poll(fresh.as_mut(), Waker::noop()), Poll::Ready(Ok(actual)) if actual == report));
}

#[test]
fn test_shutdown_coordinator_waker_clone_reenters_ticket_wait_without_blocking_poll() {
    let (ticket, gate, _subscription) = blocked_shutdown_ticket();
    let callback_ticket = ticket.clone();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (waker, _probe) = coordinator_probe_waker(
        Some(Box::new(move || {
            observed_tx
                .send(callback_ticket.wait(Some(Duration::ZERO)))
                .expect("receive clone callback result");
        })),
        None,
    );
    let future_ticket = ticket.clone();
    let mut future: ShutdownFuture = Box::pin(async move { future_ticket.wait_async().await });

    assert!(poll(future.as_mut(), &waker).is_pending());
    assert!(matches!(
        observed_rx.recv_timeout(LIMIT).expect("clone callback runs during poll"),
        Err(ShutdownError::TimedOut { timeout }) if timeout == Duration::ZERO
    ));
    drop(future);
    gate.release();
    let report = ticket
        .wait(Some(LIMIT))
        .expect("external waiter observes shutdown progress");
    assert_eq!(ShutdownOutcome::Complete, report.outcome);
}

#[test]
fn test_shutdown_coordinator_waker_replacement_drop_reenters_ticket_wait() {
    let (ticket, gate, _subscription) = blocked_shutdown_ticket();
    let callback_ticket = ticket.clone();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (old_waker, old_probe) = coordinator_probe_waker(
        None,
        Some(Box::new(move || {
            observed_tx
                .send(callback_ticket.wait(Some(Duration::ZERO)))
                .expect("receive retired waker drop result");
        })),
    );
    let future_ticket = ticket.clone();
    let mut future: ShutdownFuture = Box::pin(async move { future_ticket.wait_async().await });

    assert!(poll(future.as_mut(), &old_waker).is_pending());
    drop(old_waker);
    drop(old_probe);
    assert!(poll(future.as_mut(), Waker::noop()).is_pending());
    assert!(matches!(
        observed_rx.recv_timeout(LIMIT).expect("replacement drops old waker during poll"),
        Err(ShutdownError::TimedOut { timeout }) if timeout == Duration::ZERO
    ));
    drop(future);
    gate.release();
    let report = ticket
        .wait(Some(LIMIT))
        .expect("external waiter observes shutdown progress");
    assert_eq!(ShutdownOutcome::Complete, report.outcome);
}

#[test]
fn test_shutdown_coordinator_cancel_cleanup_drop_reenters_ticket_wait() {
    let (ticket, gate, _subscription) = blocked_shutdown_ticket();
    let callback_ticket = ticket.clone();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (waker, probe) = coordinator_probe_waker(
        None,
        Some(Box::new(move || {
            observed_tx
                .send(callback_ticket.wait(Some(Duration::ZERO)))
                .expect("receive cancellation cleanup result");
        })),
    );
    let future_ticket = ticket.clone();
    let mut future: ShutdownFuture = Box::pin(async move { future_ticket.wait_async().await });

    assert!(poll(future.as_mut(), &waker).is_pending());
    drop(waker);
    drop(probe);
    drop(future);
    assert!(matches!(
        observed_rx.recv_timeout(LIMIT).expect("cancel cleanup drops registered waker"),
        Err(ShutdownError::TimedOut { timeout }) if timeout == Duration::ZERO
    ));
    gate.release();
    let report = ticket
        .wait(Some(LIMIT))
        .expect("external waiter observes shutdown progress");
    assert_eq!(ShutdownOutcome::Complete, report.outcome);
}

#[test]
fn test_shutdown_coordinator_cancel_cleanup_from_handler_rejects_self_wait() {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("bus");
    let topic = Topic::<String>::new("shutdown.cancel-cleanup-reentry").expect("topic");
    let callback_bus = bus.clone();
    let (ticket_tx, ticket_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let resume_rx = Arc::new(Mutex::new(resume_rx));
    let future_slot = Arc::new(Mutex::new(None::<ShutdownFuture>));
    let callback_slot = future_slot.clone();
    let callback_resume = resume_rx.clone();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("cancel-cleanup-reentry", topic.clone()).expect("request"),
            move |_| {
                let ticket = Arc::new(
                    callback_bus
                        .request_shutdown(ShutdownMode::Immediate)
                        .expect("nonblocking request"),
                );
                ticket_tx.send(ticket).expect("send ticket to external observer");
                callback_resume
                    .lock()
                    .expect("resume receiver lock")
                    .recv()
                    .expect("external observer permits cancellation cleanup");
                drop(
                    callback_slot
                        .lock()
                        .expect("future slot")
                        .take()
                        .expect("pending future"),
                );
            },
        )
        .expect("subscribe");
    let _ = bus
        .publish(PublishRequest::new(topic, "payload".to_owned()).expect("request"))
        .expect("publish");

    let ticket = ticket_rx.recv_timeout(LIMIT).expect("handler starts shutdown");
    let callback_ticket = ticket.clone();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (waker, probe) = coordinator_probe_waker(
        None,
        Some(Box::new(move || {
            observed_tx
                .send(callback_ticket.wait(Some(Duration::from_millis(250))))
                .expect("receive cleanup waker result");
        })),
    );
    let future_ticket = ticket.clone();
    let mut future: ShutdownFuture = Box::pin(async move { future_ticket.wait_async().await });
    assert!(poll(future.as_mut(), &waker).is_pending());
    *future_slot.lock().expect("future slot") = Some(future);
    drop(waker);
    drop(probe);
    resume_tx.send(()).expect("resume handler");

    assert!(matches!(
        observed_rx
            .recv_timeout(LIMIT)
            .expect("cleanup drop callback completes"),
        Err(ShutdownError::Lifecycle(
            qubit_event_bus::LifecycleError::WouldDeadlock { operation: "shutdown" }
        ))
    ));
    let report = ticket
        .wait(Some(LIMIT))
        .expect("external wait observes shutdown completion");
    assert_eq!(ShutdownOutcome::Complete, report.outcome);
}

#[test]
fn test_async_wait_from_other_bus_handler_can_observe_completed_ticket() {
    let first_bus = EventBus::local(LocalEventBusConfig::default()).expect("first bus");
    let completed_ticket = first_bus
        .request_shutdown(ShutdownMode::Immediate)
        .expect("first ticket");
    let report = completed_ticket.wait(Some(LIMIT)).expect("first bus closes");

    let second_bus = EventBus::local(LocalEventBusConfig::default()).expect("second bus");
    let topic = Topic::<String>::new("shutdown.other-bus").expect("topic");
    let ticket = completed_ticket;
    let (tx, rx) = mpsc::channel();
    let _subscription = second_bus
        .subscribe(
            SubscribeRequest::new("other-bus", topic.clone()).expect("request"),
            move |_| {
                let mut future = Box::pin(ticket.wait_async());
                let observed = matches!(
                    poll(future.as_mut(), Waker::noop()),
                    Poll::Ready(Ok(ref actual)) if *actual == report
                );
                tx.send(observed).expect("send observation");
            },
        )
        .expect("subscribe");
    let _ = second_bus
        .publish(PublishRequest::new(topic, "payload".to_owned()).expect("request"))
        .expect("publish");
    assert!(rx.recv_timeout(LIMIT).expect("callback returns"));
    let _ = second_bus.shutdown(ShutdownMode::Immediate).expect("second bus closes");
}

#[test]
fn test_dropping_observer_does_not_stop_its_shutdown_generation() {
    let (bus, gate, _subscription) = blocked_bus();
    let ticket = bus.request_shutdown(ShutdownMode::Immediate).expect("ticket");
    drop(ticket);
    gate.release();
    let report = bus.shutdown(ShutdownMode::Immediate).expect("shutdown continues");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
}
