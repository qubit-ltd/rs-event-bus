// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures synchronous handler-pool saturation while four handlers back off.

use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::ShutdownMode;
use qubit_retry::BackoffPolicy;
use qubit_retry::RetryPolicy;

const HANDLER_SLOTS: usize = 4;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_BACKOFF: Duration = Duration::from_secs(1);
const PUBLISH_OFFSET: Duration = Duration::from_millis(10);

/// Runs one fresh local bus sample and returns independent-message queue wait.
fn sample() -> io::Result<Duration> {
    let registry = EventBusRegistry::with_local().map_err(io::Error::other)?;
    let positive = |value| NonZeroUsize::new(value).expect("benchmark limits are positive");
    let scheduling = DeliverySchedulingConfig::new(
        positive(HANDLER_SLOTS),
        positive(HANDLER_SLOTS * 2),
        positive(2),
        positive(HANDLER_SLOTS + 1),
    )
    .map_err(io::Error::other)?;
    let facade = EventBusFacadeConfig::new().with_delivery_scheduling(scheduling);
    let config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::default().provider_options())
        .with_facade_config(facade);
    let bus = registry.create(&config).map_err(io::Error::other)?;
    let result = run_sample(&bus);
    let shutdown_result = bus.shutdown(ShutdownMode::Immediate).map_err(io::Error::other);
    match (result, shutdown_result) {
        (Ok(wait), Ok(_)) => Ok(wait),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

/// Creates four occupied handlers, then measures one independent delivery.
fn run_sample(bus: &EventBus) -> io::Result<Duration> {
    let barrier = Arc::new(Barrier::new(HANDLER_SLOTS + 1));
    let retry_options = SubscribeOptions::<u8>::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .backoff(BackoffPolicy::fixed(RETRY_BACKOFF))
                .build()
                .map_err(io::Error::other)?,
        )
        .build();

    for index in 0..HANDLER_SLOTS {
        let topic = Topic::<u8>::new(&format!("retry-saturation.slot-{index}")).map_err(io::Error::other)?;
        let request = SubscribeRequest::new(&format!("retry-saturation-slot-{index}"), topic.clone())
            .map_err(io::Error::other)?
            .with_options(retry_options.clone());
        let barrier = barrier.clone();
        let first_attempt = Arc::new(AtomicBool::new(true));
        let _ = bus
            .subscribe(request, move |_| {
                if first_attempt.swap(false, Ordering::AcqRel) {
                    barrier.wait();
                    Err(DeliveryError::Handler {
                        source: Box::new(io::Error::other("intentional first-attempt failure")),
                    })
                } else {
                    Ok(())
                }
            })
            .map_err(io::Error::other)?;
    }

    let (started_tx, started_rx) = mpsc::channel();
    let independent_topic = Topic::<u8>::new("retry-saturation.independent").map_err(io::Error::other)?;
    let independent_request =
        SubscribeRequest::new("retry-saturation-independent", independent_topic.clone()).map_err(io::Error::other)?;
    let _ = bus
        .subscribe(independent_request, move |_| {
            started_tx.send(Instant::now()).map_err(|error| DeliveryError::Handler {
                source: Box::new(io::Error::other(error.to_string())),
            })
        })
        .map_err(io::Error::other)?;

    for index in 0..HANDLER_SLOTS {
        let topic = Topic::<u8>::new(&format!("retry-saturation.slot-{index}")).map_err(io::Error::other)?;
        let _ = bus
            .publish(qubit_event_bus::model::PublishRequest::new(topic, index as u8).map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
    }

    barrier.wait();
    thread::sleep(PUBLISH_OFFSET);
    let publish_started = Instant::now();
    let _ = bus
        .publish(qubit_event_bus::model::PublishRequest::new(independent_topic, 0).map_err(io::Error::other)?)
        .map_err(io::Error::other)?;
    let handler_started = started_rx.recv_timeout(SAMPLE_TIMEOUT).map_err(io::Error::other)?;
    Ok(handler_started.duration_since(publish_started))
}

#[test]
fn test_retry_backoff_releases_four_handler_slots() {
    let wait = sample().expect("saturation sample completes");
    assert!(wait < Duration::from_millis(200), "independent handler waited {wait:?}");
}

use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use qubit_event_bus::Diagnostic;
use qubit_event_bus::error::DeliveryAttemptError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;

use crate::support::fake_spi::FakeEventBusSpi;

#[derive(Default)]
struct TokenLog {
    calls: Mutex<Vec<(String, DeliveryDisposition)>>,
    settled: AtomicUsize,
    fail_once: AtomicBool,
    fail_permanently: AtomicBool,
}

struct TrackedSpi {
    delegate: FakeEventBusSpi,
    log: Arc<TokenLog>,
}

struct TrackedSubscription {
    delegate: Box<dyn EventSubscriptionSpi>,
    log: Arc<TokenLog>,
    owner: Option<thread::ThreadId>,
}

impl EventBusSpi for TrackedSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.delegate.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.delegate.publish(message)
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(TrackedSubscription {
            delegate: self.delegate.subscribe(request)?,
            log: self.log.clone(),
            owner: None,
        }))
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.delegate.shutdown(mode)
    }
}

impl EventSubscriptionSpi for TrackedSubscription {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        let owner = *self.owner.get_or_insert(thread::current().id());
        assert_eq!(owner, thread::current().id());
        self.delegate.receive(timeout)
    }
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        assert_eq!(
            self.owner,
            Some(thread::current().id()),
            "only the receiving owner settles"
        );
        self.log.calls.lock().expect("token log").push((
            token.downcast_ref::<String>().expect("fake token identity").clone(),
            disposition,
        ));
        if self.log.fail_permanently.load(Ordering::Acquire) || self.log.fail_once.swap(false, Ordering::AcqRel) {
            return Err(SpiError::Operation {
                provider_id: "retry-test".into(),
                operation: "settle",
                resource: None,
                kind: "transient",
                retryable: Some(!self.log.fail_permanently.load(Ordering::Acquire)),
                source: Box::new(io::Error::other("one settlement failure")),
            });
        }
        self.delegate.settle(token, disposition)?;
        self.log.settled.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
    fn close(&mut self) -> Result<(), SpiError> {
        assert_eq!(self.owner, Some(thread::current().id()));
        self.delegate.close()
    }
}

/// Creates a facade whose token and receiving-thread identity can be checked.
fn tracked_bus() -> (EventBus, Arc<TokenLog>) {
    let log = Arc::new(TokenLog::default());
    let spi = Arc::new(TrackedSpi {
        delegate: FakeEventBusSpi::new(),
        log: log.clone(),
    });
    (
        EventBus::from_spi(ProviderId::new("retry-test").expect("provider ID"), spi).expect("fake bus"),
        log,
    )
}

/// Fails one attempt with a concrete handler error.
fn failed_attempt() -> Result<(), DeliveryError> {
    Err(DeliveryError::Handler {
        source: Box::new(io::Error::other("retry me")),
    })
}

/// Polls an externally observable condition with a bounded failure deadline.
fn wait_for(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !condition() {
        assert!(Instant::now() < deadline, "condition did not become true");
        thread::sleep(Duration::from_millis(1));
    }
}

/// Asserts all scheduler ownership is released after shutdown.
fn assert_idle(bus: &EventBus) {
    let metrics = bus.delivery_metrics();
    assert_eq!(
        (
            metrics.reserved_receives,
            metrics.queued,
            metrics.running_handlers,
            metrics.settling,
            metrics.lane_waiting
        ),
        (0, 0, 0, 0, 0)
    );
    assert_eq!(metrics.oldest_owned_age, None);
}

/// Publishes a value in a shared ordering lane.
fn publish_key(bus: &EventBus, topic: &Topic<u8>, value: u8) {
    let request = PublishRequest::builder()
        .topic(topic.clone())
        .payload(value)
        .ordering_key("same-key")
        .build()
        .expect("request");
    let _ = bus.publish(request).expect("publish");
}

#[test]
fn test_retry_same_key_waits_for_final_settlement_and_reuses_token() {
    let (bus, log) = tracked_bus();
    log.fail_once.store(true, Ordering::Release);
    let topic = Topic::<u8>::new("retry.lane").expect("topic");
    let observed = Arc::new(Mutex::new(Vec::new()));
    let received = observed.clone();
    let handler_log = log.clone();
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .backoff(BackoffPolicy::fixed(Duration::from_millis(50)))
                .build()
                .expect("policy"),
        )
        .build();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("lane", topic.clone())
                .expect("request")
                .with_options(options),
            move |delivery: Delivery<u8>| {
                let value = *delivery.payload();
                received
                    .lock()
                    .expect("observations")
                    .push((value, delivery.context().retry_attempt()));
                if value == 2 {
                    assert_eq!(
                        handler_log.settled.load(Ordering::Acquire),
                        1,
                        "successor cannot overtake settlement retry"
                    );
                }
                if value == 1 && delivery.context().retry_attempt() == 1 {
                    failed_attempt()
                } else {
                    Ok(())
                }
            },
        )
        .expect("subscription");
    publish_key(&bus, &topic, 1);
    publish_key(&bus, &topic, 2);
    wait_for(|| log.settled.load(Ordering::Acquire) == 2);
    let _ = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(2),
        })
        .expect("shutdown");
    assert_eq!(*observed.lock().expect("observations"), vec![(1, 1), (1, 2), (2, 1)]);
    let calls = log.calls.lock().expect("token calls");
    assert_eq!(calls.len(), 3);
    assert_eq!(
        calls[0], calls[1],
        "settlement retries retain identical token and intent"
    );
    assert_ne!(calls[1].0, calls[2].0);
    assert_idle(&bus);
}

#[test]
fn test_retry_cancellation_while_waiting_settles_once_without_another_attempt() {
    let (bus, log) = tracked_bus();
    let cancellation = RetryCancellationToken::new();
    let topic = Topic::<u8>::new("retry.cancel").expect("topic");
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let options = SubscribeOptions::builder()
        .retry_cancellation_token(cancellation.clone())
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .backoff(BackoffPolicy::fixed(Duration::from_secs(30)))
                .build()
                .expect("policy"),
        )
        .build();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("cancel", topic.clone())
                .expect("request")
                .with_options(options),
            move |_: Delivery<u8>| {
                handler_calls.fetch_add(1, Ordering::AcqRel);
                failed_attempt()
            },
        )
        .expect("subscription");
    publish_key(&bus, &topic, 1);
    wait_for(|| {
        calls.load(Ordering::Acquire) == 1
            && bus.delivery_metrics().running_handlers == 0
            && bus.delivery_metrics().queued == 1
    });
    cancellation.cancel();
    wait_for(|| log.settled.load(Ordering::Acquire) == 1);
    let _ = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(log.calls.lock().expect("token calls").len(), 1);
    assert_idle(&bus);
}

/// Exercises owner shutdown with a retry already in its waiting phase.
fn shutdown_waiting_retry(graceful: bool) {
    let (bus, log) = tracked_bus();
    let topic = Topic::<u8>::new("retry.shutdown").expect("topic");
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let options = SubscribeOptions::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .backoff(BackoffPolicy::fixed(Duration::from_millis(150)))
                .build()
                .expect("policy"),
        )
        .build();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("shutdown", topic.clone())
                .expect("request")
                .with_options(options),
            move |_: Delivery<u8>| {
                if handler_calls.fetch_add(1, Ordering::AcqRel) == 0 {
                    failed_attempt()
                } else {
                    Ok(())
                }
            },
        )
        .expect("subscription");
    publish_key(&bus, &topic, 1);
    wait_for(|| {
        calls.load(Ordering::Acquire) == 1
            && bus.delivery_metrics().running_handlers == 0
            && bus.delivery_metrics().queued == 1
    });
    let mode = if graceful {
        ShutdownMode::Graceful {
            timeout: Duration::from_secs(2),
        }
    } else {
        ShutdownMode::Immediate
    };
    let _ = bus.shutdown(mode).expect("shutdown");
    assert_eq!(calls.load(Ordering::Acquire), if graceful { 2 } else { 1 });
    assert_eq!(log.settled.load(Ordering::Acquire), 1);
    let token_calls = log.calls.lock().expect("token calls");
    assert_eq!(token_calls.len(), 1);
    assert_eq!(
        token_calls[0].1,
        if graceful {
            DeliveryDisposition::Accept
        } else {
            DeliveryDisposition::Retry
        }
    );
    assert_idle(&bus);
}

#[test]
fn test_retry_graceful_shutdown_drains_waiting_attempt() {
    shutdown_waiting_retry(true);
}

#[test]
fn test_retry_immediate_shutdown_requeues_waiting_without_another_attempt() {
    shutdown_waiting_retry(false);
}

/// Checks panic and retry-rule termination preserve terminal diagnostics and
/// notify error handlers exactly once per failed attempt.
fn terminal_retry(panic_handler: bool, panic_rule: bool, decision: RetryDecision, attempts: u32) {
    let (bus, log) = tracked_bus();
    let topic = Topic::<u8>::new("retry.terminal").expect("topic");
    let errors = Arc::new(AtomicUsize::new(0));
    let error_calls = errors.clone();
    let (tx, rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::DeliveryFailed { attempts, .. } = diagnostic {
            let _ = tx.send(*attempts);
        }
    });
    let options = SubscribeOptions::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .backoff(BackoffPolicy::fixed(Duration::ZERO))
                .build()
                .expect("policy"),
        )
        .retry_rule(move |_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| {
            assert!(!panic_rule, "rule panic");
            decision
        })
        .error_handler(move |_, _| {
            error_calls.fetch_add(1, Ordering::AcqRel);
            FailureDirective::Retry
        })
        .build();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("terminal", topic.clone())
                .expect("request")
                .with_options(options),
            move |_: Delivery<u8>| {
                assert!(!panic_handler, "handler panic");
                failed_attempt()
            },
        )
        .expect("subscription");
    publish_key(&bus, &topic, 1);
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)).expect("failure diagnostic"),
        attempts
    );
    wait_for(|| log.settled.load(Ordering::Acquire) == 1);
    let _ = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
    assert_eq!(errors.load(Ordering::Acquire), attempts as usize);
    assert_eq!(log.calls.lock().expect("token calls").len(), 1);
    assert_idle(&bus);
}

#[test]
fn test_retry_rule_abort_settles_once() {
    terminal_retry(false, false, RetryDecision::Abort, 1);
}

#[test]
fn test_retry_handler_panic_settles_once() {
    terminal_retry(true, false, RetryDecision::Abort, 1);
}

#[test]
fn test_retry_rule_panic_settles_once() {
    terminal_retry(false, true, RetryDecision::Abort, 1);
}

#[test]
fn test_retry_immediate_shutdown_retains_running_slot_until_job_exit() {
    let (bus, log) = tracked_bus();
    let topic = Topic::<u8>::new("retry.running-stop").expect("topic");
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let options = SubscribeOptions::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .backoff(BackoffPolicy::fixed(Duration::from_secs(30)))
                .build()
                .expect("policy"),
        )
        .build();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("running-stop", topic.clone())
                .expect("request")
                .with_options(options),
            move |_: Delivery<u8>| {
                started_tx.send(()).expect("start signal");
                release_rx
                    .lock()
                    .expect("release lock")
                    .recv_timeout(Duration::from_secs(3))
                    .expect("release handler");
                failed_attempt()
            },
        )
        .expect("subscription");
    publish_key(&bus, &topic, 1);
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("handler started");
    let ticket = bus.request_shutdown(ShutdownMode::Immediate).expect("shutdown request");
    assert_eq!(
        bus.delivery_metrics().running_handlers,
        1,
        "shutdown cannot release a live job's H slot"
    );
    assert_eq!(log.settled.load(Ordering::Acquire), 0);
    wait_for(|| bus.delivery_metrics().reserved_receives == 0);
    thread::sleep(Duration::from_millis(80));
    release_tx.send(()).expect("release job");
    let _ = ticket.wait(Some(Duration::from_secs(3))).expect("shutdown completes");
    assert_eq!(
        log.settled.load(Ordering::Acquire),
        1,
        "failed running attempt is requeued once on stop"
    );
    assert_eq!(log.calls.lock().expect("token calls")[0].1, DeliveryDisposition::Retry);
    assert_idle(&bus);
}

#[test]
fn test_retry_terminal_settlement_failure_releases_credit_and_token() {
    let (bus, log) = tracked_bus();
    log.fail_permanently.store(true, Ordering::Release);
    let topic = Topic::<u8>::new("retry.failed-settlement").expect("topic");
    let options = SubscribeOptions::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .backoff(BackoffPolicy::fixed(Duration::from_millis(10)))
                .build()
                .expect("policy"),
        )
        .build();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("failed-settlement", topic.clone())
                .expect("request")
                .with_options(options),
            move |delivery: Delivery<u8>| {
                if delivery.context().retry_attempt() == 1 {
                    failed_attempt()
                } else {
                    Ok(())
                }
            },
        )
        .expect("subscription");
    publish_key(&bus, &topic, 1);
    wait_for(|| bus.delivery_metrics().settlement_terminal_failures == 1);
    let _ = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
    assert_eq!(log.settled.load(Ordering::Acquire), 0);
    assert_eq!(log.calls.lock().expect("token calls").len(), 1);
    assert_eq!(bus.delivery_metrics().completed, 0);
    assert_idle(&bus);
}

#[test]
fn test_retry_subscription_cancel_requeues_waiting_token_once() {
    let (bus, log) = tracked_bus();
    let topic = Topic::<u8>::new("retry.subscription-cancel").expect("topic");
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let options = SubscribeOptions::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .backoff(BackoffPolicy::fixed(Duration::from_secs(30)))
                .build()
                .expect("policy"),
        )
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("subscription-cancel", topic.clone())
                .expect("request")
                .with_options(options),
            move |_: Delivery<u8>| {
                handler_calls.fetch_add(1, Ordering::AcqRel);
                failed_attempt()
            },
        )
        .expect("subscription");
    publish_key(&bus, &topic, 1);
    wait_for(|| {
        calls.load(Ordering::Acquire) == 1
            && bus.delivery_metrics().running_handlers == 0
            && bus.delivery_metrics().queued == 1
    });
    subscription.cancel().expect("cancel");
    let _ = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(log.settled.load(Ordering::Acquire), 1);
    assert_eq!(log.calls.lock().expect("token calls").len(), 1);
    assert_idle(&bus);
}

#[test]
fn test_retry_exhaustion_reports_all_attempts_and_error_callbacks() {
    terminal_retry(false, false, RetryDecision::Retry, 3);
}
