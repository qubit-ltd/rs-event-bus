// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancelable asynchronous shutdown-observer contracts.

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
use qubit_event_bus::Subscription;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

const LIMIT: Duration = Duration::from_secs(5);

/// Releases a handler blocked until the shutdown test advances.
struct Gate(Arc<(Mutex<bool>, Condvar)>);
impl Gate {
    /// Opens the handler gate and wakes its worker.
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

/// Builds a local bus with an in-flight handler to keep shutdown observable.
fn blocked_bus() -> (EventBus, Gate, Subscription) {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("local bus");
    let topic = Topic::<String>::new("shutdown.registration.mirror").expect("topic");
    let gate = Gate(Arc::new((Mutex::new(false), Condvar::new())));
    let handler_gate = gate.0.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("shutdown-registration", topic.clone()).expect("request"),
            move |_| {
                entered_tx.send(()).expect("test remains available");
                let (lock, changed) = &*handler_gate;
                let mut released = lock.lock().expect("gate lock");
                while !*released {
                    released = changed.wait(released).expect("gate wait");
                }
            },
        )
        .expect("subscription");
    let _ = bus
        .publish(PublishRequest::new(topic, "held".to_owned()).expect("publish request"))
        .expect("publish");
    entered_rx.recv_timeout(LIMIT).expect("handler entered");
    (bus, gate, subscription)
}

/// Sends a notification to a test blocked waiting for a registered waker.
struct Signal(mpsc::Sender<()>);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(());
    }
}

/// Polls a pinned future with the supplied observer waker.
fn poll<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[test]
fn test_async_shutdown_registrations_cancel_independently_and_ticket_reuses_result() {
    let (bus, gate, _subscription) = blocked_bus();
    let ticket = bus
        .request_shutdown(ShutdownMode::Graceful { timeout: LIMIT })
        .expect("shutdown ticket");
    let other_ticket = bus
        .request_shutdown(ShutdownMode::Immediate)
        .expect("joined shutdown ticket");
    drop(other_ticket);

    assert!(matches!(
        ticket.wait(Some(Duration::ZERO)),
        Err(ShutdownError::TimedOut { .. })
    ));
    let (wake_tx, wake_rx) = mpsc::channel();
    let waker = Waker::from(Arc::new(Signal(wake_tx)));
    let mut cancelled = Box::pin(ticket.wait_async());
    let mut observer = Box::pin(ticket.wait_async());
    assert!(poll(cancelled.as_mut(), &waker).is_pending());
    assert!(poll(observer.as_mut(), &waker).is_pending());

    drop(cancelled);
    gate.release();
    wake_rx.recv_timeout(LIMIT).expect("remaining observer is woken");
    assert!(matches!(poll(observer.as_mut(), &waker), Poll::Ready(Ok(_))));
    drop(observer);
    assert!(ticket.wait(Some(LIMIT)).is_ok());

    let mut completed = Box::pin(ticket.wait_async());
    assert!(matches!(poll(completed.as_mut(), Waker::noop()), Poll::Ready(Ok(_))));
    drop(completed);
}
