// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Real handlers remain blocked until the application releases their gates.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use event_bus_documentation_consumer::bounded_shutdown::try_shutdown;
use event_bus_documentation_consumer::bounded_shutdown::try_shutdown_async;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use tokio::sync::Notify;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::timeout;

#[test]
fn test_sync_uncooperative_handler_returns_control_before_gate_release() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local bus");
    let topic = Topic::<String>::new("shutdown.gate").expect("topic");
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let release_receiver = Mutex::new(release_receiver);
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("gate", topic.clone()).expect("request"),
            move |_| {
                entered_sender.send(()).expect("notify handler entry");
                release_receiver
                    .lock()
                    .expect("gate lock")
                    .recv()
                    .expect("release handler gate");
            },
        )
        .expect("subscription");
    let _receipt = bus
        .publish(PublishRequest::new(topic, String::new()).expect("publication"))
        .expect("admission");
    entered_receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("handler entered");
    let (result_sender, result_receiver) = mpsc::channel();
    let shutdown_bus = bus.clone();
    let waiter = thread::spawn(move || {
        result_sender
            .send(try_shutdown(&shutdown_bus))
            .expect("report shutdown result");
    });
    // This watchdog prevents a broken example from hanging the suite. It is
    // deliberately not a scheduler-sensitive assertion about exact elapsed time.
    let result = result_receiver.recv_timeout(Duration::from_secs(30));
    release_sender.send(()).expect("always release handler gate");
    waiter.join().expect("shutdown thread");
    assert!(
        !result.expect("bounded caller returned").expect("only deadline elapsed"),
        "shutdown should return control while the handler remains blocked"
    );
    let report = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(30),
        })
        .expect("cleanup");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    drop(subscription);
}

#[test]
fn test_sync_idle_shutdown_completes() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local bus");
    assert!(
        try_shutdown(&bus).expect("shutdown"),
        "shutdown should complete when no handler is active"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_async_uncooperative_handler_returns_control_before_gate_release() {
    let bus = AsyncEventBus::local(LocalEventBusConfig::new())
        .await
        .expect("local bus");
    let topic = Topic::<String>::new("shutdown.gate").expect("topic");
    let subscription = bus
        .subscribe(SubscribeRequest::new("gate", topic.clone()).expect("request"))
        .await
        .expect("subscription");
    let (entered_sender, mut entered_receiver) = unbounded_channel();
    let gate = Arc::new(Notify::new());
    let handler_gate = gate.clone();
    let runner = tokio::spawn(async move {
        subscription
            .run(move |_| {
                let entered_sender = entered_sender.clone();
                let gate = handler_gate.clone();
                async move {
                    entered_sender.send(()).expect("notify entry");
                    gate.notified().await;
                    Ok(())
                }
            })
            .await
    });
    let _receipt = bus
        .publish(PublishRequest::new(topic, String::new()).expect("publication"))
        .await
        .expect("admission");
    timeout(Duration::from_secs(30), entered_receiver.recv())
        .await
        .expect("handler entry timed out")
        .expect("handler exited before entry");
    let result = timeout(Duration::from_secs(30), try_shutdown_async(&bus)).await;
    gate.notify_one();
    assert!(
        !result.expect("bounded caller returned").expect("only deadline elapsed"),
        "async shutdown should return control while the handler remains blocked"
    );
    let report = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(30),
        })
        .await
        .expect("cleanup");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    timeout(Duration::from_secs(30), runner)
        .await
        .expect("runner finishes")
        .expect("runner task")
        .expect("run result");
}

#[tokio::test(flavor = "current_thread")]
async fn test_async_idle_shutdown_completes() {
    let bus = AsyncEventBus::local(LocalEventBusConfig::new())
        .await
        .expect("local bus");
    assert!(
        try_shutdown_async(&bus).await.expect("shutdown"),
        "async shutdown should complete when no handler is active"
    );
}

struct CloseGate {
    released: AtomicBool,
    notify: Notify,
}

impl CloseGate {
    async fn wait(&self) {
        while !self.released.load(Ordering::Acquire) {
            let notified = self.notify.notified();
            if self.released.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

struct CloseGatedProvider {
    inner: Arc<AsyncLocalEventBusSpi>,
    gate: Arc<CloseGate>,
}

impl AsyncEventBusSpi for CloseGatedProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        let gate = self.gate.clone();
        Box::pin(async move {
            let inner = self.inner.subscribe(request).await?;
            Ok(Box::new(CloseGatedSubscription { inner, gate }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

struct CloseGatedSubscription {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    gate: Arc<CloseGate>,
}

impl AsyncEventSubscriptionSpi for CloseGatedSubscription {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        self.inner.receive(timeout)
    }

    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        self.inner.settle(token, disposition)
    }

    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async move {
            self.gate.wait().await;
            self.inner.close().await
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_async_pending_provider_close_is_bounded_and_resumable() {
    let gate = Arc::new(CloseGate {
        released: AtomicBool::new(false),
        notify: Notify::new(),
    });
    let inner = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).expect("local SPI"));
    let provider = Arc::new(CloseGatedProvider {
        inner,
        gate: gate.clone(),
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("close-gated").expect("provider id"), provider).expect("bus");
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("close-gated", Topic::<String>::new("shutdown.close").expect("topic"))
                .expect("request"),
        )
        .await
        .expect("subscription");

    assert!(!try_shutdown_async(&bus).await.expect("bounded shutdown"));
    gate.release();
    assert!(try_shutdown_async(&bus).await.expect("resumed shutdown"));
}
