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
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::ShutdownError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

const LIMIT: Duration = Duration::from_secs(5);

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
fn blocked_bus() -> (EventBus, Gate, qubit_event_bus::Subscription) {
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
    bus.publish(PublishRequest::new(topic, "payload".to_owned()).expect("publish request"))
        .expect("publish");
    entered_rx.recv_timeout(LIMIT).expect("handler entered");
    (bus, gate, subscription)
}
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
fn poll<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[test]
fn test_request_returns_before_handler_gate_releases() {
    let (bus, gate, _subscription) = blocked_bus();
    let (returned_tx, returned_rx) = mpsc::channel();
    let caller = std::thread::spawn(move || {
        let result = bus.request_shutdown(ShutdownMode::Immediate);
        returned_tx.send(result).expect("request result");
    });
    let returned = returned_rx.recv_timeout(LIMIT);
    gate.release();
    caller.join().expect("caller joins");
    let ticket = returned
        .expect("request must return while handler remains blocked")
        .expect("ticket");
    ticket.wait(Some(LIMIT)).expect("shutdown after release");
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
    bus.publish(PublishRequest::new(topic, "payload".to_owned()).expect("request"))
        .expect("publish");
    let (ticket, rejected) = rx.recv_timeout(LIMIT).expect("callback returns");
    assert!(rejected);
    ticket.wait(Some(LIMIT)).expect("external wait");
}
