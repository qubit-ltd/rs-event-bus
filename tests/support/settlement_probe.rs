// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sync provider observations and failure-safe gates for policy regressions.

use std::collections::HashMap;
use std::io::Error;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;

use qubit_event_bus::Subscription;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::PublishAcknowledgement;
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
use qubit_id::Id;

use super::fake_spi::FakeEventBusSpi;

/// Counts provider callbacks and allows tests to wait for a minimum count.
#[derive(Default)]
pub(crate) struct Signal {
    count: Mutex<usize>,
    changed: Condvar,
}
impl Signal {
    /// Returns the exact number of observed entries.
    #[must_use]
    #[inline]
    pub(crate) fn count(&self) -> usize {
        *self.count.lock().expect("signal lock")
    }
    /// Waits for an explicit entry count; the deadline only protects teardown.
    #[must_use]
    pub(crate) fn wait(&self, minimum: usize) -> bool {
        let count = self.count.lock().expect("signal lock");
        let (count, _) = self
            .changed
            .wait_timeout_while(count, Duration::from_secs(2), |n| *n < minimum)
            .expect("signal wait");
        *count >= minimum
    }
    /// Records one entry and wakes observers.
    pub(crate) fn enter(&self) {
        *self.count.lock().expect("signal lock") += 1;
        self.changed.notify_all();
    }
}
/// Holds a callback until a test explicitly releases waiting worker threads.
#[derive(Default)]
pub(crate) struct Gate {
    released: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    /// Installs an unwind guard that releases this gate before bus teardown.
    #[must_use]
    #[inline]
    pub(crate) fn release_on_drop(self: &Arc<Self>) -> ReleaseGate {
        ReleaseGate(self.clone())
    }
    /// Blocks until explicitly released.
    pub(crate) fn wait(&self) {
        let mut released = self.released.lock().expect("gate lock");
        while !*released {
            released = self.changed.wait(released).expect("gate wait");
        }
    }
    /// Releases all current and future waiters.
    pub(crate) fn release(&self) {
        *self.released.lock().expect("gate lock") = true;
        self.changed.notify_all();
    }
}
/// Releases its gate when dropped so a failed assertion cannot strand a worker.
pub(crate) struct ReleaseGate(Arc<Gate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// Captures delivery settlement and close activity for concurrency tests.
#[derive(Default)]
pub(crate) struct SettlementProbe {
    pub(crate) received: Signal,
    pub(crate) entered: Signal,
    pub(crate) closed: Signal,
    pub(crate) settle_gate: Arc<Gate>,
    pub(crate) close_gate: Arc<Gate>,
    pub(crate) attempts: Mutex<Vec<(Id, usize, DeliveryDisposition)>>,
    pub(crate) failures: Mutex<Option<(bool, usize)>>,
}
impl SettlementProbe {
    /// Creates open gates and an explicit retryability/failure-count policy.
    #[must_use]
    pub(crate) fn new(retryable: bool, failures: usize) -> Arc<Self> {
        let probe = Arc::new(Self::default());
        *probe.failures.lock().expect("failure policy lock") = Some((retryable, failures));
        probe.settle_gate.release();
        probe.close_gate.release();
        probe
    }
}
/// Wraps the fake SPI with per-subscriber settlement and close probes.
pub(crate) struct ProbeBus {
    fake: FakeEventBusSpi,
    probes: Mutex<HashMap<String, Arc<SettlementProbe>>>,
}
impl ProbeBus {
    /// Creates a provider using the existing fake transport and capabilities.
    #[must_use]
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            fake: FakeEventBusSpi::new(),
            probes: Mutex::new(HashMap::new()),
        })
    }
    /// Assigns an observation policy to one logical subscriber before
    /// subscribe.
    pub(crate) fn register(&self, subscriber: &str, probe: Arc<SettlementProbe>) {
        self.probes
            .lock()
            .expect("probe map lock")
            .insert(subscriber.to_owned(), probe);
    }
}
impl EventBusSpi for ProbeBus {
    #[inline]
    fn capabilities(&self) -> EventBusCapabilities {
        self.fake.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.fake.publish(message)
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        let probe = self
            .probes
            .lock()
            .expect("probe map lock")
            .get(request.subscriber_id().as_str())
            .expect("subscriber probe registered")
            .clone();
        let owner = request.subscription_id();
        Ok(Box::new(ProbeSubscription {
            inner: self.fake.subscribe(request)?,
            owner,
            probe,
        }))
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.fake.shutdown(mode)
    }
}
/// Records callback activity while forwarding subscription operations.
struct ProbeSubscription {
    inner: Box<dyn EventSubscriptionSpi>,
    owner: Id,
    probe: Arc<SettlementProbe>,
}
impl EventSubscriptionSpi for ProbeSubscription {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        let outcome = self.inner.receive(timeout)?;
        if matches!(outcome, ReceiveOutcome::Message(_)) {
            self.probe.received.enter();
        }
        Ok(outcome)
    }
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        assert!(token.belongs_to(self.owner), "settlement remains on its issuing owner");
        let identity = token.downcast_ref::<String>().expect("fake issues String token") as *const String as usize;
        self.probe
            .attempts
            .lock()
            .expect("attempts lock")
            .push((self.owner, identity, disposition));
        self.probe.entered.enter();
        self.probe.settle_gate.wait();
        let mut policy = self.probe.failures.lock().expect("failure policy lock");
        if let Some((retryable, remaining)) = policy.as_mut()
            && *remaining > 0
        {
            *remaining -= 1;
            return Err(SpiError::Operation {
                provider_id: "probe".into(),
                operation: "settle",
                resource: None,
                kind: "injected_settlement_failure",
                retryable: Some(*retryable),
                source: Box::new(Error::other("explicit settlement policy failure")),
            });
        }
        self.inner.settle(token, disposition)
    }
    fn close(&mut self) -> Result<(), SpiError> {
        self.inner.close()?;
        self.probe.closed.enter();
        self.probe.close_gate.wait();
        Ok(())
    }
}

/// Cancels subscriptions on both normal exit and assertion unwind.
pub(crate) struct CancelOnDrop<'a>(pub(crate) Vec<&'a Subscription>);
impl Drop for CancelOnDrop<'_> {
    fn drop(&mut self) {
        for subscription in &self.0 {
            let _ = subscription.cancel();
        }
    }
}
