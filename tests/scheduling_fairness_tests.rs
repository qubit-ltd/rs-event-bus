// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sync handler fairness while one ordering lane consumes the old admission
//! budget.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::EventBus;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use support::fake_spi::FakeEventBusSpi;
use support::fake_spi::inbound_message;
use support::settlement_probe::CancelOnDrop;
use support::settlement_probe::Gate;
use support::settlement_probe::ProbeBus;
use support::settlement_probe::SettlementProbe;
use support::settlement_probe::Signal;

/// Publishes a keyed payload using the production facade.
fn publish(bus: &EventBus, topic: &str, payload: &str, key: &str) {
    let _ = bus
        .publish(
            PublishRequest::builder()
                .topic(Topic::<String>::new(topic).expect("valid topic"))
                .payload(payload.to_owned())
                .ordering_key(key)
                .build()
                .expect("valid keyed request"),
        )
        .expect("publish succeeds");
}

/// Exercises either one subscription with two keys or two separate
/// subscriptions.
fn assert_hot_lane_does_not_block_independent_handler(two_subscriptions: bool) {
    let spi = ProbeBus::new();
    let hot = SettlementProbe::new(true, 0);
    spi.register("hot", hot.clone());
    let independent = SettlementProbe::new(true, 0);
    if two_subscriptions {
        spi.register("independent", independent);
    }
    let bus = EventBus::from_spi(ProviderId::new("probe").expect("valid provider"), spi).expect("valid capabilities");
    let blocked = Arc::new(Gate::default());
    let barrier = Arc::new(Signal::default());
    let callback_gate = blocked.clone();
    let callback_barrier = barrier.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let same_subscription_tx = entered_tx.clone();
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let hot_subscription = bus
        .subscribe(
            SubscribeRequest::new("hot", Topic::<String>::new("hot").expect("valid topic"))
                .expect("valid subscriber")
                .with_options(options.clone()),
            move |delivery: Delivery<String>| {
                if delivery.payload() == "A0" {
                    callback_barrier.enter();
                    callback_gate.wait();
                } else if delivery.payload() == "B0" {
                    let _ = same_subscription_tx.send(());
                }
            },
        )
        .expect("hot subscription starts");
    let independent_subscription = if two_subscriptions {
        Some(
            bus.subscribe(
                SubscribeRequest::new("independent", Topic::<String>::new("independent").expect("valid topic"))
                    .expect("valid subscriber")
                    .with_options(options),
                move |_: Delivery<String>| {
                    let _ = entered_tx.send(());
                },
            )
            .expect("independent subscription starts"),
        )
    } else {
        None
    };
    let mut cleanup = vec![&hot_subscription];
    if let Some(subscription) = independent_subscription.as_ref() {
        cleanup.push(subscription);
    }
    let _cancel_on_unwind = CancelOnDrop(cleanup);
    // Declared after the bus, this guard releases A0 even if an assertion unwinds.
    let _release_on_unwind = blocked.release_on_drop();
    publish(&bus, "hot", "A0", "A");
    assert!(barrier.wait(1), "A0 enters handler at explicit startup barrier");
    for payload in ["A1", "A2", "A3", "A4"] {
        publish(&bus, "hot", payload, "A");
    }
    let received_hot = hot.received.wait(5);
    if received_hot {
        publish(&bus, if two_subscriptions { "independent" } else { "hot" }, "B0", "B");
    }
    let independent_entered = received_hot && entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
    // Release before joining: failure cannot deadlock on the still-running handler.
    blocked.release();
    hot_subscription.cancel().expect("hot cleanup");
    if let Some(subscription) = independent_subscription.as_ref() {
        subscription.cancel().expect("independent cleanup");
    }
    assert!(
        received_hot,
        "fake receive gate confirms A0 through A4 are owned before B0 is published"
    );
    assert!(
        independent_entered,
        "B0 must enter its handler while A0 is blocked; queued same-key work must not exhaust the running budget"
    );
}

#[test]
fn test_sync_hot_key_preserves_independent_key_progress() {
    assert_hot_lane_does_not_block_independent_handler(false);
}

#[test]
fn test_sync_hot_subscription_preserves_independent_subscription_progress() {
    assert_hot_lane_does_not_block_independent_handler(true);
}

/// Sync provider that returns two queued messages and then a natural EOF.
struct ClosedAfterTwoBus {
    fake: FakeEventBusSpi,
    receives: Arc<AtomicUsize>,
    outcomes: Arc<Mutex<Vec<&'static str>>>,
    closes: Arc<AtomicUsize>,
}
impl EventBusSpi for ClosedAfterTwoBus {
    fn capabilities(&self) -> EventBusCapabilities {
        self.fake.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.fake.publish(message)
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(ClosedAfterTwoReceiver {
            inner: self.fake.subscribe(request)?,
            receives: self.receives.clone(),
            outcomes: self.outcomes.clone(),
            closes: self.closes.clone(),
        }))
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.fake.shutdown(mode)
    }
}
struct ClosedAfterTwoReceiver {
    inner: Box<dyn EventSubscriptionSpi>,
    receives: Arc<AtomicUsize>,
    outcomes: Arc<Mutex<Vec<&'static str>>>,
    closes: Arc<AtomicUsize>,
}
impl EventSubscriptionSpi for ClosedAfterTwoReceiver {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        if self.receives.load(AtomicOrdering::SeqCst) == 2 {
            self.outcomes.lock().expect("receive log lock").push("Closed");
            return Ok(ReceiveOutcome::Closed);
        }
        let outcome = self.inner.receive(timeout)?;
        if matches!(outcome, ReceiveOutcome::Message(_)) {
            self.receives.fetch_add(1, AtomicOrdering::SeqCst);
            self.outcomes.lock().expect("receive log lock").push("Message");
        }
        Ok(outcome)
    }
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        self.inner.settle(token, disposition)
    }
    fn close(&mut self) -> Result<(), SpiError> {
        self.closes.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner.close()
    }
}

#[test]
fn test_natural_closed_observation() {
    let fake = FakeEventBusSpi::new();
    let receives = Arc::new(AtomicUsize::new(0));
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let closes = Arc::new(AtomicUsize::new(0));
    let bus = EventBus::from_spi(
        ProviderId::new("closed-after-two").expect("valid provider"),
        Arc::new(ClosedAfterTwoBus {
            fake: fake.clone(),
            receives: receives.clone(),
            outcomes: outcomes.clone(),
            closes: closes.clone(),
        }),
    )
    .expect("valid capabilities");
    let gate = Arc::new(Gate::default());
    let entered = Arc::new(Signal::default());
    let completed = Arc::new(AtomicUsize::new(0));
    let handler_gate = gate.clone();
    let handler_entered = entered.clone();
    let handler_completed = completed.clone();
    let sub = bus
        .subscribe(
            SubscribeRequest::new("natural-closed", Topic::<u32>::new("test.topic").expect("valid topic"))
                .expect("valid subscriber"),
            move |_: Delivery<u32>| {
                let first = handler_completed.load(AtomicOrdering::SeqCst) == 0;
                if first {
                    handler_entered.enter();
                    handler_gate.wait();
                }
                handler_completed.fetch_add(1, AtomicOrdering::SeqCst);
            },
        )
        .expect("subscription starts");
    let _cleanup = CancelOnDrop(vec![&sub]);
    let _release_on_unwind = gate.release_on_drop();
    fake.enqueue(inbound_message(None));
    fake.enqueue(inbound_message(None));
    assert!(entered.wait(1), "A1 handler enters its gate");
    let mut closed_observed = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if outcomes.lock().expect("receive log lock").contains(&"Closed") {
            closed_observed = true;
            break;
        }
        std::thread::yield_now();
    }
    let before_release = format!(
        "receives={}, outcomes={:?}, handlers={}, settlements=tokenless",
        receives.load(AtomicOrdering::SeqCst),
        outcomes.lock().expect("receive log lock"),
        completed.load(AtomicOrdering::SeqCst)
    );
    gate.release();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if completed.load(AtomicOrdering::SeqCst) >= 2 {
            break;
        }
        std::thread::yield_now();
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if closes.load(AtomicOrdering::SeqCst) > 0 {
            break;
        }
        std::thread::yield_now();
    }
    let snapshot = format!(
        "before_release=[{before_release}], handlers={}, closes={}, bus_metrics={:?}",
        completed.load(AtomicOrdering::SeqCst),
        closes.load(AtomicOrdering::SeqCst),
        bus.delivery_metrics()
    );
    assert!(
        closed_observed,
        "natural Closed must be consumed while A1 is gated: {snapshot}"
    );
    assert_eq!(
        receives.load(AtomicOrdering::SeqCst),
        2,
        "A1 and A2 receive sequence: {snapshot}"
    );
    assert_eq!(
        completed.load(AtomicOrdering::SeqCst),
        2,
        "both sync handlers complete: {snapshot}"
    );
    assert_eq!(
        closes.load(AtomicOrdering::SeqCst),
        1,
        "sync receiver closes once: {snapshot}"
    );
    eprintln!("sync natural Closed observation: {snapshot}");
}
