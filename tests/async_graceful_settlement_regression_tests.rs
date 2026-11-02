// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Graceful settlement retry and backoff regression coverage.
mod support;

use std::collections::VecDeque;
use std::future::Future;
use std::io::Error;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::SpiError;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::SettlementRetryConfig;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::SettlementTermination;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
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
use support::fake_spi::FakeAsyncEventBusSpi;
use support::fake_spi::inbound_message;
use support::manual_async::block_on;
use support::manual_async::poll_once;

#[derive(Default)]
struct Probe {
    failures: Mutex<VecDeque<Option<bool>>>,
    attempts: Mutex<Vec<(String, DeliveryDisposition)>>,
    handlers: std::sync::atomic::AtomicUsize,
}

struct ProbeBus {
    fake: Arc<FakeAsyncEventBusSpi>,
    probe: Arc<Probe>,
}
impl AsyncEventBusSpi for ProbeBus {
    fn capabilities(&self) -> EventBusCapabilities {
        self.fake.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.fake.publish(message)
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            Ok(Box::new(ProbeReceiver {
                receiver: self.fake.subscribe(request).await?,
                probe: self.probe.clone(),
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.fake.shutdown(mode)
    }
}
struct ProbeReceiver {
    receiver: Box<dyn AsyncEventSubscriptionSpi>,
    probe: Arc<Probe>,
}
impl AsyncEventSubscriptionSpi for ProbeReceiver {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        self.receiver.receive(timeout)
    }
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        self.probe
            .attempts
            .lock()
            .expect("attempt log mutex")
            .push((token.downcast_ref::<String>().cloned().unwrap_or_default(), disposition));
        let scripted = self.probe.failures.lock().expect("failure queue mutex").pop_front();
        let settle = self.receiver.settle(token, disposition);
        Box::pin(async move {
            if scripted == Some(Some(true)) {
                Err(SpiError::Operation {
                    provider_id: "graceful-probe".into(),
                    operation: "settle",
                    resource: None,
                    kind: "temporary",
                    retryable: Some(true),
                    source: Box::new(Error::other("temporary settlement failure")),
                })
            } else {
                settle.await
            }
        })
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        self.receiver.close()
    }
}

fn setup() -> (
    AsyncEventBus,
    Arc<FakeAsyncEventBusSpi>,
    Arc<Probe>,
    ManualMonotonicClock,
    qubit_event_bus::AsyncSubscription<u32>,
) {
    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let probe = Arc::new(Probe::default());
    probe
        .failures
        .lock()
        .expect("failure queue mutex")
        .push_back(Some(true));
    let clock = ManualMonotonicClock::new();
    let config = EventBusFacadeConfig::new()
        .with_delivery_scheduling(
            DeliverySchedulingConfig::new(
                NonZeroUsize::new(1).expect("nonzero"),
                NonZeroUsize::new(2).expect("nonzero"),
                NonZeroUsize::new(2).expect("nonzero"),
                NonZeroUsize::new(1).expect("nonzero"),
            )
            .expect("valid scheduling"),
        )
        .with_settlement_retry(
            SettlementRetryConfig::new(
                NonZeroU32::new(3).expect("nonzero"),
                Duration::from_secs(2),
                Duration::from_millis(10),
                Duration::from_millis(20),
            )
            .expect("valid retry policy"),
        );
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("graceful-probe").expect("provider id"),
        Arc::new(ProbeBus {
            fake: fake.clone(),
            probe: probe.clone(),
        }),
        config,
        clock.new_timer(),
    )
    .expect("bus");
    let topic = Topic::<u32>::new("events.test").expect("topic");
    let request = SubscribeRequest::new("settlement-regression", topic)
        .expect("request")
        .with_options(SubscribeOptions::default());
    let sub = block_on(bus.subscribe(request)).expect("subscription");
    fake.enqueue(inbound_message(Some(SettlementToken::new(sub.id(), "stable-token"))));
    (bus, fake, probe, clock, sub)
}

fn start_run<'a>(
    sub: &'a qubit_event_bus::AsyncSubscription<u32>,
    probe: &Arc<Probe>,
) -> impl Future<Output = Result<(), ReceiveError>> + Send + use<'a> {
    let probe = probe.clone();
    sub.run(move |_: Delivery<u32>| {
        probe.handlers.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    })
}

fn attempt_count(probe: &Probe) -> usize {
    probe.attempts.lock().expect("attempt log mutex").len()
}

#[test]
fn test_graceful_retries_failed_settlement() {
    let (bus, _fake, probe, clock, sub) = setup();
    let mut run = Box::pin(start_run(&sub, &probe));
    for _ in 0..32 {
        let _ = poll_once(run.as_mut());
        if attempt_count(&probe) > 0 {
            break;
        }
    }
    assert_eq!(attempt_count(&probe), 1);
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(5),
    }));
    assert!(poll_once(shutdown.as_mut()).is_pending());
    let mut run_result = Poll::Pending;
    for _ in 0..16 {
        run_result = poll_once(run.as_mut());
        if attempt_count(&probe) > 1 || run_result.is_ready() {
            break;
        }
    }
    assert_eq!(attempt_count(&probe), 1, "retry is blocked by its backoff deadline");
    assert!(
        matches!(run_result, Poll::Pending),
        "runner remains retained during backoff"
    );
    assert_eq!(attempt_count(&probe), 1, "backoff must be observed");
    clock
        .advance(Duration::from_millis(10))
        .expect("advance retry deadline");
    let mut run_result = None;
    let mut shutdown_result = None;
    for _ in 0..64 {
        if run_result.is_none()
            && let Poll::Ready(result) = poll_once(run.as_mut())
        {
            run_result = Some(result);
        }
        if shutdown_result.is_none()
            && let Poll::Ready(result) = poll_once(shutdown.as_mut())
        {
            shutdown_result = Some(result);
        }
        if run_result.is_some() && shutdown_result.is_some() {
            break;
        }
    }
    assert_eq!(attempt_count(&probe), 2, "Graceful retries after backoff");
    let attempts = probe.attempts.lock().expect("attempt log mutex");
    assert_eq!(attempts[0], attempts[1], "retry preserves token and disposition");
    drop(attempts);
    assert_eq!(probe.handlers.load(Ordering::SeqCst), 1, "handler is never replayed");
    assert!(matches!(run_result, Some(Ok(()))), "runner drains cleanly");
    let report = shutdown_result
        .expect("Graceful shutdown completes after settlement")
        .expect("shutdown succeeds");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
}

#[test]
fn test_immediate_does_not_retry_failed_settlement() {
    let (bus, _fake, probe, _clock, sub) = setup();
    let mut run = Box::pin(start_run(&sub, &probe));
    for _ in 0..32 {
        let _ = poll_once(run.as_mut());
        if attempt_count(&probe) > 0 {
            break;
        }
    }
    assert_eq!(attempt_count(&probe), 1);
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    let mut shutdown_result = Poll::Pending;
    for _ in 0..64 {
        if shutdown_result.is_pending() {
            let _ = poll_once(run.as_mut());
        }
        shutdown_result = poll_once(shutdown.as_mut());
        if shutdown_result.is_ready() {
            break;
        }
    }
    assert!(shutdown_result.is_ready(), "Immediate shutdown completes");
    assert_eq!(
        attempt_count(&probe),
        1,
        "Immediate must not retry an attempted settlement"
    );
    assert_eq!(probe.handlers.load(Ordering::SeqCst), 1);
}

#[test]
fn test_graceful_exhaustion_preserves_terminal_reason() {
    let (bus, _fake, probe, clock, sub) = setup();
    probe
        .failures
        .lock()
        .expect("failure queue mutex")
        .extend([Some(true), Some(true)]);
    let mut run = Box::pin(start_run(&sub, &probe));
    for _ in 0..32 {
        let _ = poll_once(run.as_mut());
        if attempt_count(&probe) > 0 {
            break;
        }
    }
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(5),
    }));
    assert!(poll_once(shutdown.as_mut()).is_pending());
    let mut result = poll_once(run.as_mut());
    clock
        .advance(Duration::from_millis(10))
        .expect("advance retry deadline");
    for _ in 0..64 {
        if result.is_pending() {
            result = poll_once(run.as_mut());
        }
        let _ = poll_once(shutdown.as_mut());
        if result.is_ready() {
            break;
        }
        clock
            .advance(Duration::from_millis(20))
            .expect("advance retry deadline");
    }
    assert_eq!(attempt_count(&probe), 3);
    assert_eq!(probe.handlers.load(Ordering::SeqCst), 1);
    assert!(matches!(
        result,
        Poll::Ready(Err(ReceiveError::Stopped(reason)))
            if matches!(reason.as_ref(), SubscriptionStopReason::Settlement {
                attempts: 3,
                termination: SettlementTermination::AttemptsExhausted,
                ..
            })
    ));
}

#[test]
fn test_graceful_timeout_resumes_settlement_budget() {
    let (bus, _fake, probe, clock, sub) = setup();
    let mut run = Box::pin(start_run(&sub, &probe));
    for _ in 0..32 {
        let _ = poll_once(run.as_mut());
        if attempt_count(&probe) > 0 {
            break;
        }
    }
    assert_eq!(attempt_count(&probe), 1);

    let mut first_shutdown = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_millis(5),
    }));
    assert!(poll_once(first_shutdown.as_mut()).is_pending());
    clock
        .advance(Duration::from_millis(5))
        .expect("advance shutdown deadline");
    let first_result = poll_once(first_shutdown.as_mut());
    assert!(
        matches!(first_result, Poll::Ready(Err(_))),
        "first graceful attempt times out"
    );

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(5),
    }));
    assert!(poll_once(shutdown.as_mut()).is_pending());
    clock
        .advance(Duration::from_millis(5))
        .expect("resume retained settlement deadline");
    let mut run_result = None;
    let mut shutdown_result = None;
    for _ in 0..64 {
        if run_result.is_none()
            && let Poll::Ready(result) = poll_once(run.as_mut())
        {
            run_result = Some(result);
        }
        if shutdown_result.is_none()
            && let Poll::Ready(result) = poll_once(shutdown.as_mut())
        {
            shutdown_result = Some(result);
        }
        if run_result.is_some() && shutdown_result.is_some() {
            break;
        }
    }
    assert_eq!(
        attempt_count(&probe),
        2,
        "timeout does not reset or discard retry state"
    );
    assert_eq!(
        probe.handlers.load(Ordering::SeqCst),
        1,
        "handler is not rerun after timeout"
    );
    assert!(matches!(run_result, Some(Ok(()))));
    let report = shutdown_result
        .expect("resumed shutdown completes")
        .expect("resumed shutdown succeeds");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
}
