// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public async failure behavior and source preservation regressions.
mod support;

use std::error::Error;
use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::mpsc;
use std::num::NonZeroU32;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::facade::SettlementRetryConfig;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_retry::RetryPolicy;

fn topic() -> Topic<u32> {
    Topic::new("test.topic").expect("test topic is valid")
}

fn inbound(event_id: &str, token: SettlementToken) -> InboundMessage {
    InboundMessage::new(
        TopicAddress::new("test.topic").expect("test provider topic is valid"),
        EventId::new(event_id).expect("test event ID is valid"),
        std::time::SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
        TransportPayload::Native(Arc::new(42_u32)),
        Some(token),
        Default::default(),
    )
}

fn collect_failures(
    bus: &AsyncEventBus,
) -> (mpsc::Receiver<Diagnostic>, qubit_event_bus::DiagnosticObserverHandle) {
    let (sender, receiver) = mpsc::channel();
    let observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::DeliveryFailed { .. }) {
            sender.send(diagnostic.clone()).expect("observer remains connected");
        }
    });
    (receiver, observer)
}

fn assert_one_failure(receiver: &mpsc::Receiver<Diagnostic>, event_id: &str, attempts: u32) {
    let diagnostic = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("terminal handler failure diagnostic arrives");
    assert_failure(diagnostic, event_id, attempts);
}

fn assert_failure(diagnostic: Diagnostic, event_id: &str, attempts: u32) {
    let Diagnostic::DeliveryFailed {
        event_id: actual_event_id,
        attempts: actual_attempts,
        subscriber_id,
        ..
    } = diagnostic
    else {
        panic!("observer only records DeliveryFailed diagnostics");
    };
    assert_eq!(actual_event_id.as_str(), event_id);
    assert_eq!(actual_attempts, attempts);
    assert_eq!(subscriber_id.as_str(), "terminal-source");
}

fn assert_no_more_failures(receiver: &mpsc::Receiver<Diagnostic>) {
    assert!(receiver.try_recv().is_err(), "the same owned failure is reported once");
}

#[test]
fn test_dead_letter_run_error_retains_publish_effect_and_spi_source() {
    let spi = Arc::new(support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).unwrap();
    let options = SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::with_topic_name("test.dead").unwrap())
        .build();
    support::manual_async::block_on(async {
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("terminal-source", topic())
                    .unwrap()
                    .with_options(options),
            )
            .await
            .unwrap();
        spi.fail_next_publish();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "durable-source-token",
        ))));
        let error = subscription
            .run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(IoError::other("handler failed")),
                })
            })
            .await
            .expect_err("dead-letter forwarding failure is terminal");
        let source = error.source().expect("forwarding source is retained");
        assert!(source
            .downcast_ref::<qubit_event_bus::pipeline::DeadLetterForwardError>()
            .is_some());
        let mut current = Some(source);
        let mut effect = None;
        while let Some(source) = current {
            if let Some(failure) = source.downcast_ref::<qubit_event_bus::PublishFailure>() {
                effect = Some(failure.effect());
            }
            current = source.source();
        }
        assert_eq!(effect, Some(qubit_event_bus::model::PublishEffect::NotAccepted));
        assert!(error.to_string().contains("event-test"));
        assert!(spi.settlement_dispositions().is_empty(), "source token remains unsettled");
        let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await.unwrap();
    });
}

#[test]
fn test_dead_letter_retry_error_retains_terminal_context() {
    let spi = Arc::new(support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).unwrap();
    let options = SubscribeOptions::<u32>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::with_topic_name("test.dead").unwrap())
        .build();
    support::manual_async::block_on(async {
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("terminal-source", topic())
                    .unwrap()
                    .with_options(options),
            )
            .await
            .unwrap();
        spi.fail_next_publish();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "durable-source-token",
        ))));
        let error = subscription
            .run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(IoError::other("handler failed")),
                })
            })
            .await
            .expect_err("dead-letter forwarding failure is terminal");
        let source = error.source().expect("retry error is retained");
        let retry = source
            .downcast_ref::<qubit_retry::RetryError<qubit_event_bus::pipeline::DeadLetterForwardError>>()
            .expect("full retry error remains publicly inspectable");
        assert_eq!(retry.context().attempts(), 1);
        assert!(retry.last_error().is_some());
        assert!(matches!(retry.reason(), qubit_retry::RetryErrorReason::Exhausted { .. }));
        assert!(spi.settlement_dispositions().is_empty());
        let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await.unwrap();
    });
}

#[test]
fn test_handler_failure_without_token_emits_once() {
    let spi = Arc::new(support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).unwrap();
    let (failures, _observer) = collect_failures(&bus);
    support::manual_async::block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("terminal-source", topic()).unwrap())
            .await
            .unwrap();
        spi.enqueue(support::fake_spi::inbound_message(None));
        let runner = std::thread::spawn(move || {
            support::manual_async::block_on(subscription.run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(IoError::other("handler failed")),
                })
            }))
        });
        let observed = failures.recv_timeout(Duration::from_secs(2));
        let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
        assert_failure(observed.expect("terminal handler failure diagnostic arrives"), "event-test", 1);
        assert_no_more_failures(&failures);
        assert!(spi.settlement_dispositions().is_empty());
    });
}

#[test]
fn test_handler_failure_with_permanent_settlement_error_emits_once() {
    let spi = Arc::new(support::fake_spi::FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_settlement_retry(
        SettlementRetryConfig::new(
            NonZeroU32::new(1).unwrap(),
            Duration::from_secs(1),
            Duration::from_millis(1),
            Duration::from_millis(1),
        )
        .unwrap(),
    );
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config).unwrap();
    let (failures, _observer) = collect_failures(&bus);
    support::manual_async::block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("terminal-source", topic()).unwrap())
            .await
            .unwrap();
        spi.fail_all_settles();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "source-token",
        ))));
        let runner = std::thread::spawn(move || {
            support::manual_async::block_on(subscription.run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(IoError::other("handler failed")),
                })
            }))
        });
        let observed = failures.recv_timeout(Duration::from_secs(2));
        let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await.unwrap();
        assert!(matches!(runner.join().unwrap(), Err(ReceiveError::Stopped(_))));
        assert_failure(observed.expect("terminal handler failure diagnostic arrives"), "event-test", 1);
        assert_no_more_failures(&failures);
        assert!(spi.settlement_dispositions().is_empty());
    });
}

#[test]
fn test_settlement_retry_does_not_duplicate_handler_failure_diagnostic() {
    let spi = Arc::new(support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).unwrap();
    let (failures, _observer) = collect_failures(&bus);
    let options = SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| FailureDirective::Discard)
        .build();
    support::manual_async::block_on(async {
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("terminal-source", topic())
                    .unwrap()
                    .with_options(options),
            )
            .await
            .unwrap();
        spi.fail_next_settle();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "source-token",
        ))));
        let runner = std::thread::spawn(move || {
            support::manual_async::block_on(subscription.run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(IoError::other("handler failed")),
                })
            }))
        });
        let observed = failures.recv_timeout(Duration::from_secs(2));
        let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
        assert_failure(observed.expect("terminal handler failure diagnostic arrives"), "event-test", 1);
        assert_no_more_failures(&failures);
        assert_eq!(spi.settlement_dispositions(), [DeliveryDisposition::Reject]);
    });
}

#[test]
fn test_paused_runner_keeps_terminal_diagnostic_until_finalization() {
    let spi = Arc::new(support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).unwrap();
    let (failures, _observer) = collect_failures(&bus);
    let options = SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| FailureDirective::Discard)
        .build();
    support::manual_async::block_on(async {
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("terminal-source", topic())
                    .unwrap()
                    .with_options(options),
            )
            .await
            .unwrap();
        spi.fail_next_settle();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "source-token",
        ))));
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let started_handler = started.clone();
        let mut first_run = Box::pin(subscription.run(move |_| {
            let started = started_handler.clone();
            async move {
                started.store(true, std::sync::atomic::Ordering::Release);
                Err(DeliveryError::Handler {
                    source: Box::new(IoError::other("handler failed")),
                })
            }
        }));
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !started.load(std::sync::atomic::Ordering::Acquire) && std::time::Instant::now() < deadline {
            let result = support::manual_async::poll_once(first_run.as_mut());
            assert!(result.is_pending(), "paused session keeps its runner alive");
            std::thread::yield_now();
        }
        assert!(started.load(std::sync::atomic::Ordering::Acquire));
        drop(first_run);
        drop(subscription);
        assert_one_failure(&failures, "event-test", 1);
        assert_no_more_failures(&failures);
        let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await.unwrap();
        assert!(spi.settlement_dispositions().is_empty(), "cancelled settlement remains unsettled");
    });
}

#[test]
fn test_receive_failure_finalizes_all_completed_handler_failures_once() {
    let spi = Arc::new(support::fake_spi::FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_settlement_retry(
        SettlementRetryConfig::new(
            NonZeroU32::new(5).unwrap(),
            Duration::from_secs(10),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap(),
    );
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config).unwrap();
    let (failures, _observer) = collect_failures(&bus);
    let (settlement_tx, settlements) = mpsc::channel();
    let spi_in_observer = spi.clone();
    let _settlement_observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::SettlementFailed { event_id, .. } = diagnostic {
            settlement_tx.send(event_id.clone()).expect("settlement observer remains connected");
            spi_in_observer.fail_next_receive();
        }
    });
    let options = SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| FailureDirective::Discard)
        .build();
    support::manual_async::block_on(async {
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("terminal-source", topic())
                    .unwrap()
                    .with_options(options),
            )
            .await
            .unwrap();
        spi.fail_next_settle();
        spi.enqueue(inbound(
            "pending-failure-event",
            SettlementToken::new(subscription.id(), "first-token"),
        ));
        let (result_tx, result_rx) = mpsc::channel();
        let runner = std::thread::spawn(move || {
            let result = support::manual_async::block_on(subscription.run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(IoError::other("handler failed")),
                })
            }));
            let _ = result_tx.send(result);
        });
        let result = result_rx.recv_timeout(Duration::from_secs(3));
        if result.is_err() {
            let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await;
        }
        assert!(matches!(result.expect("run stops after terminal provider receive failure"), Err(ReceiveError::Stopped(_))));
        runner.join().unwrap();
        assert_eq!(
            settlements.recv_timeout(Duration::from_secs(2)).expect("completed item reports settlement failure").as_str(),
            "pending-failure-event"
        );
        assert_one_failure(&failures, "pending-failure-event", 1);
        assert_no_more_failures(&failures);
        assert!(spi.settlement_dispositions().is_empty(), "source token remains unsettled");
        let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await.unwrap();
    });
}

#[test]
fn test_dead_letter_failure_emits_once_without_settling_source() {
    let spi = Arc::new(support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).unwrap();
    let (failures, _observer) = collect_failures(&bus);
    let options = SubscribeOptions::<u32>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::with_topic_name("test.dead").unwrap())
        .build();
    support::manual_async::block_on(async {
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("terminal-source", topic())
                    .unwrap()
                    .with_options(options),
            )
            .await
            .unwrap();
        let other_subscription = bus
            .subscribe(
                SubscribeRequest::new("unaffected-subscriber", Topic::<u32>::new("unaffected.topic").unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (other_tx, other_rx) = mpsc::channel();
        let other_runner = std::thread::spawn(move || {
            support::manual_async::block_on(other_subscription.run(move |_| {
                let sender = other_tx.clone();
                async move {
                    sender.send(()).expect("other subscription remains observed");
                    Ok(())
                }
            }))
        });
        spi.fail_next_publish();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "source-token",
        ))));
        let runner = std::thread::spawn(move || {
            support::manual_async::block_on(subscription.run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(IoError::other("handler failed")),
                })
            }))
        });
        let result = runner.join().unwrap();
        assert!(matches!(result, Err(ReceiveError::DeadLetterForwardFailed { .. })));
        assert_one_failure(&failures, "event-test", 1);
        assert_no_more_failures(&failures);
        let _ = bus
            .publish(PublishRequest::new(Topic::new("unaffected.topic").unwrap(), 7_u32).unwrap())
            .await
            .unwrap();
        other_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("the unrelated subscription still processes messages");
        let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await.unwrap();
        other_runner.join().unwrap().unwrap();
        assert_eq!(spi.settlement_dispositions(), [DeliveryDisposition::Accept]);
    });
}

#[test]
fn test_cancelled_successful_delivery_does_not_report_handler_failure() {
    let spi = Arc::new(support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).unwrap();
    let (failures, _observer) = collect_failures(&bus);
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started_handler = started.clone();
    let complete = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let complete_handler = complete.clone();
    support::manual_async::block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("terminal-source", topic()).unwrap())
            .await
            .unwrap();
        spi.enqueue(support::fake_spi::inbound_message(None));
        let mut run = Box::pin(subscription.run(move |_| {
            let started = started_handler.clone();
            let complete = complete_handler.clone();
            async move {
                started.store(true, std::sync::atomic::Ordering::Release);
                std::future::poll_fn(|context| {
                    if complete.load(std::sync::atomic::Ordering::Acquire) {
                        std::task::Poll::Ready(Ok(()))
                    } else {
                        context.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                })
                .await
            }
        }));
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !started.load(std::sync::atomic::Ordering::Acquire) && std::time::Instant::now() < deadline {
            assert!(support::manual_async::poll_once(run.as_mut()).is_pending());
            std::thread::yield_now();
        }
        assert!(started.load(std::sync::atomic::Ordering::Acquire));
        drop(run);
        complete.store(true, std::sync::atomic::Ordering::Release);
        let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await.unwrap();
        assert!(failures.try_recv().is_err());
    });
}
