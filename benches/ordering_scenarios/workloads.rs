// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public-facade scheduling samples with explicit backlog latency observations.
//!
//! The local provider stores unkeyed fixture messages. A thin SPI decorator
//! restores the prepared keys on receive, exposing the FIFO backlog to the
//! facade scheduler, and injects four retryable failures per token when asked.
//! These are synthetic scheduler-pressure samples, not native-local transport
//! comparisons. No production implementation is included or duplicated.

use std::future::Future;
use std::future::poll_fn;
use std::hint::black_box;
use std::io::Error as IoError;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::yield_now;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::error::ReceiveError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::SettlementRetryConfig;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::OrderingKey;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;

const OPERATIONS: usize = 256;
const FAILURES: usize = 4;
const WARMUPS: usize = 2;
const SAMPLES: usize = 7;

/// Borrowed subscription runner retained until all tokens have settled.
type Runner<'a> = Pin<Box<dyn Future<Output = Result<(), ReceiveError>> + 'a>>;

/// Fault injection counters shared with the post-timing validity check.
#[derive(Default)]
struct SettlementCounts {
    attempts: AtomicUsize,
    successes: AtomicUsize,
    wrong_dispositions: AtomicUsize,
}

/// Local provider plus a controlled settlement error decorator.
struct RetrySpi {
    inner: AsyncLocalEventBusSpi,
    keys: Arc<Vec<OrderingKey>>,
    failures: usize,
    counts: Arc<SettlementCounts>,
}

/// Receiver-owned wrapper that preserves the real local token and receiver.
struct RetryReceiver {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    keys: Arc<Vec<OrderingKey>>,
    subscription_id: Id,
    failures: usize,
    counts: Arc<SettlementCounts>,
}

/// One real provider token and its remaining synthetic retryable failures.
struct RetryToken {
    inner: SettlementToken,
    remaining: AtomicUsize,
}

impl AsyncEventBusSpi for RetrySpi {
    /// Returns the actual local provider capabilities without changing them.
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }

    /// Strips the fixture key before local publication. The prepared key table
    /// restores it on receipt; publication is outside the measured interval.
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        let unkeyed = OutboundMessage::new(
            message.topic().clone(),
            message.id().clone(),
            message.timestamp(),
            message.headers().clone(),
            None,
            message.delay(),
            message.into_payload(),
        );
        self.inner.publish(unkeyed)
    }

    /// Wraps the real receiver after successful local subscription creation.
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            let subscription_id = request.subscription_id();
            let inner = self.inner.subscribe(request).await?;
            Ok(Box::new(RetryReceiver {
                inner,
                keys: self.keys.clone(),
                subscription_id,
                failures: self.failures,
                counts: self.counts.clone(),
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }

    /// Delegates shutdown and preserves its outcome and error semantics.
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

impl AsyncEventSubscriptionSpi for RetryReceiver {
    /// Restores a prepared key and wraps the local token; other outcomes pass
    /// through. Unexpected fixture payloads return a permanent SPI error.
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        Box::pin(async move {
            let outcome = self.inner.receive(timeout).await?;
            let ReceiveOutcome::Message(message) = outcome else {
                return Ok(outcome);
            };
            let (topic, id, timestamp, headers, _, payload, token, metadata) = message.into_parts();
            let key = match &payload {
                TransportPayload::Native(value) => {
                    value.downcast_ref::<usize>().and_then(|index| self.keys.get(*index))
                }
                _ => None,
            }
            .cloned()
            .ok_or_else(|| injected_error(false))?;
            let token = token.map(|inner| {
                SettlementToken::new(
                    self.subscription_id,
                    RetryToken {
                        inner,
                        remaining: AtomicUsize::new(self.failures),
                    },
                )
            });
            Ok(ReceiveOutcome::Message(InboundMessage::new(
                topic,
                id,
                timestamp,
                headers,
                Some(key),
                payload,
                token,
                metadata,
            )))
        })
    }

    /// Fails the first configured attempts on each token, then delegates the
    /// same disposition to the real provider. Invalid wrapper tokens fail.
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        self.counts.attempts.fetch_add(1, Ordering::Relaxed);
        if disposition != DeliveryDisposition::Accept {
            self.counts.wrong_dispositions.fetch_add(1, Ordering::Relaxed);
        }
        let Some(token) = token.downcast_ref::<RetryToken>() else {
            return Box::pin(async { Err(injected_error(false)) });
        };
        if token
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| left.checked_sub(1))
            .is_ok()
        {
            return Box::pin(async { Err(injected_error(true)) });
        }
        let operation = self.inner.settle(&token.inner, disposition);
        let counts = self.counts.clone();
        Box::pin(async move {
            operation.await?;
            counts.successes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }

    /// Delegates receiver close and any provider close failure.
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        self.inner.close()
    }
}

/// Constructs a synthetic settlement failure with explicit retryability.
fn injected_error(retryable: bool) -> SpiError {
    SpiError::Operation {
        provider_id: "ordering-scenarios".into(),
        operation: "settle",
        resource: None,
        kind: "benchmark-injection",
        retryable: Some(retryable),
        source: Box::new(IoError::other("controlled settlement failure")),
    }
}

/// Opens the A0 gate when the sample releases this guard or unwinds.
///
/// Keep the guard alive until the intended A0 release point; dropping it opens
/// the gate immediately.
#[must_use = "dropping this guard releases the A0 lane gate"]
struct GateRelease(Arc<AtomicBool>);

impl Drop for GateRelease {
    /// Releases the gate; no worker waits on it after the runner is dropped.
    fn drop(&mut self) {
        let Self(gate) = self;
        gate.store(true, Ordering::Relaxed);
    }
}

/// Drives a setup or teardown future to completion.
///
/// A stuck future is bounded by the invoking process or job timeout, not by a
/// benchmark-local wall-clock threshold.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        yield_now();
    }
}

/// Raw measurement with a defined backlog-to-handler latency distribution.
#[derive(Debug)]
struct Sample {
    wall_ns: u128,
    handler_backlog_latency_ns: Vec<u64>,
    healthy_before_release: usize,
    settlement_attempts: usize,
}

/// Runs a fresh fixture. All subscriptions, requests and payloads are prepared
/// before timing. The timer covers public runner progress through successful
/// settlement of every delivery; all result assertions follow the timer.
fn sample(workload: &str, operations: usize) -> Sample {
    let cross_subscription = workload == "cross-subscription";
    let gated = matches!(workload, "hot-a0-gate" | "cross-subscription");
    let failures = if workload == "settlement-retry" { FAILURES } else { 0 };
    let subscriptions = if cross_subscription { 2 } else { 1 };
    let healthy = Arc::new((0..operations).map(|index| gated && index % 4 == 3).collect::<Vec<_>>());
    let keys = Arc::new(
        (0..operations)
            .map(|index| {
                let key = if gated {
                    if healthy[index] { "B".to_owned() } else { "A".to_owned() }
                } else {
                    format!("key-{}", index % 16)
                };
                OrderingKey::new(&key).unwrap()
            })
            .collect::<Vec<_>>(),
    );
    let counts = Arc::new(SettlementCounts::default());
    let provider = Arc::new(RetrySpi {
        inner: AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new().queue_capacity(operations)).unwrap(),
        keys: keys.clone(),
        failures,
        counts: counts.clone(),
    });
    let scheduling = DeliverySchedulingConfig::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(64).unwrap(),
        NonZeroUsize::new(32).unwrap(),
        NonZeroUsize::new(2).unwrap(),
    )
    .unwrap();
    let retry = SettlementRetryConfig::new(
        NonZeroU32::new((FAILURES + 1) as u32).unwrap(),
        Duration::from_secs(5),
        Duration::from_micros(1),
        Duration::from_micros(1),
    )
    .unwrap();
    let config = EventBusFacadeConfig::new()
        .with_delivery_scheduling(scheduling)
        .with_settlement_retry(retry);
    let bus = AsyncEventBus::with_config(ProviderId::new("ordering-scenarios").unwrap(), provider, config).unwrap();
    let mut handles = Vec::with_capacity(subscriptions);
    let topics = (0..subscriptions)
        .map(|index| Topic::<usize>::new(&format!("bench.scenario.{index}")).unwrap())
        .collect::<Vec<_>>();
    for (index, topic) in topics.iter().enumerate() {
        let options = SubscribeOptions::builder()
            .ordering_policy(OrderingPolicy::PerKey)
            .build();
        let request = SubscribeRequest::new(&format!("subscriber-{index}"), topic.clone())
            .unwrap()
            .with_options(options);
        handles.push(block_on(bus.subscribe(request)).unwrap());
    }
    let requests = (0..operations)
        .map(|index| {
            let is_healthy = healthy[index];
            let topic_index = usize::from(cross_subscription && is_healthy);
            PublishRequest::builder()
                .topic(topics[topic_index].clone())
                .payload(index)
                .ordering_key(keys[index].as_str())
                .build()
                .unwrap()
        })
        .collect::<Vec<_>>();
    for request in requests {
        black_box(block_on(bus.publish(request)).unwrap());
    }
    // Eight B completions are reachable within P=32 while A0 holds its lane.
    // Waiting for the entire provider backlog would exceed the documented
    // fairness boundary once queued A deliveries fill all owned capacity.
    let healthy_target = healthy.iter().filter(|value| **value).count().min(8);
    let released = Arc::new(AtomicBool::new(!gated));
    let gate_release = GateRelease(released.clone());
    let completed = Arc::new(AtomicUsize::new(0));
    let healthy_completed = Arc::new(AtomicUsize::new(0));
    let early_hot = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(OnceLock::<Instant>::new());
    let latencies = Arc::new((0..operations).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    let mut runners: Vec<Runner<'_>> = handles
        .iter_mut()
        .map(|handle| {
            let released = released.clone();
            let completed = completed.clone();
            let healthy_completed = healthy_completed.clone();
            let healthy = healthy.clone();
            let early_hot = early_hot.clone();
            let started = started.clone();
            let latencies = latencies.clone();
            Box::pin(handle.run(move |delivery| {
                let index = *black_box(delivery.payload());
                if gated && index != 0 && !healthy[index] && !released.load(Ordering::Relaxed) {
                    early_hot.fetch_add(1, Ordering::Relaxed);
                }
                let released = released.clone();
                let completed = completed.clone();
                let healthy_completed = healthy_completed.clone();
                let healthy = healthy.clone();
                let started = started.clone();
                let latencies = latencies.clone();
                poll_fn(move |_| {
                    if gated && index == 0 && !released.load(Ordering::Relaxed) {
                        return Poll::Pending;
                    }
                    if let Some(start) = started.get() {
                        latencies[index].store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    }
                    completed.fetch_add(1, Ordering::Relaxed);
                    if healthy[index] {
                        healthy_completed.fetch_add(1, Ordering::Relaxed);
                    }
                    Poll::Ready(Ok(()))
                })
            })) as Runner<'_>
        })
        .collect();
    let mut context = Context::from_waker(Waker::noop());
    let mut runner_stopped = false;
    let mut healthy_before_release = 0;
    let start = Instant::now();
    let _ = started.set(start);
    while counts.successes.load(Ordering::Relaxed) != operations {
        for runner in &mut runners {
            if black_box(runner.as_mut().poll(&mut context)).is_ready() {
                runner_stopped = true;
                break;
            }
        }
        if gated && !released.load(Ordering::Relaxed) && healthy_completed.load(Ordering::Relaxed) >= healthy_target {
            healthy_before_release = healthy_completed.load(Ordering::Relaxed);
            released.store(true, Ordering::Relaxed);
        }
        if runner_stopped {
            break;
        }
    }
    let wall_ns = start.elapsed().as_nanos();
    drop(gate_release);
    drop(runners);
    let _shutdown_report = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert!(!runner_stopped, "runner terminated before the sample completed");
    assert_eq!(
        completed.load(Ordering::Relaxed),
        operations,
        "incomplete handler sample"
    );
    assert_eq!(
        counts.successes.load(Ordering::Relaxed),
        operations,
        "incomplete settlement sample"
    );
    assert_eq!(counts.attempts.load(Ordering::Relaxed), operations * (failures + 1));
    assert_eq!(counts.wrong_dispositions.load(Ordering::Relaxed), 0);
    assert_eq!(
        early_hot.load(Ordering::Relaxed),
        0,
        "same-key successor started before A0 release"
    );
    assert!(healthy_before_release >= healthy_target);
    Sample {
        wall_ns,
        handler_backlog_latency_ns: latencies.iter().map(|value| value.load(Ordering::Relaxed)).collect(),
        healthy_before_release,
        settlement_attempts: counts.attempts.load(Ordering::Relaxed),
    }
}

/// Runs the additional workload matrix or a single smoke sample per case.
/// Smoke timings are discarded. Full mode emits every raw latency and a real
/// per-delivery backlog p95; this is not handler execution duration or SPI p95.
pub(crate) fn run(smoke: bool) {
    for workload in ["uniform", "hot-a0-gate", "cross-subscription", "settlement-retry"] {
        if smoke {
            black_box(sample(workload, 64));
            println!("scenario={workload} invariants=passed smoke_timings=discarded");
            continue;
        }
        for _ in 0..WARMUPS {
            black_box(sample(workload, OPERATIONS));
        }
        for iteration in 1..=SAMPLES {
            let result = sample(workload, OPERATIONS);
            let mut sorted = result.handler_backlog_latency_ns.clone();
            sorted.sort_unstable();
            let p95 = sorted[(sorted.len() * 95).div_ceil(100) - 1];
            println!(
                "scheduling provider=key-restoring-local-fixture scenario={workload} iteration={iteration} operations={OPERATIONS} running_limit=2 owned_limit=64 per_subscription_limit=32 subscriptions={} wall_ns={} ns_per_settled_delivery={} handler_backlog_p95_ns={p95} healthy_before_release={} settlement_attempts={} handler_backlog_latency_ns={:?}",
                if workload == "cross-subscription" { 2 } else { 1 },
                result.wall_ns,
                result.wall_ns / OPERATIONS as u128,
                result.healthy_before_release,
                result.settlement_attempts,
                result.handler_backlog_latency_ns,
            );
        }
    }
}
