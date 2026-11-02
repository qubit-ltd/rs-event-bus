// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded lifecycle history for failed subscription closes.

mod support;

use std::io::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::ShutdownError;
use qubit_event_bus::SpiError;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;

const FAILURE_COUNT: usize = 300;

struct SyncCloseFailingBus;

impl EventBusSpi for SyncCloseFailingBus {
    fn capabilities(&self) -> EventBusCapabilities {
        support::fake_spi::full_capabilities()
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(SyncCloseFailingReceiver {
            subscriber_id: request.subscriber_id().as_str().to_owned(),
        }))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

struct SyncCloseFailingReceiver {
    subscriber_id: String,
}

impl EventSubscriptionSpi for SyncCloseFailingReceiver {
    fn receive(&mut self, _: Duration) -> Result<ReceiveOutcome, SpiError> {
        Ok(ReceiveOutcome::TimedOut)
    }

    fn settle(&mut self, _: &SettlementToken, _: DeliveryDisposition) -> Result<(), SpiError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), SpiError> {
        Err(close_error(&self.subscriber_id))
    }
}

struct AsyncCloseFailingBus;

impl AsyncEventBusSpi for AsyncCloseFailingBus {
    fn capabilities(&self) -> EventBusCapabilities {
        support::fake_spi::full_capabilities()
    }

    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async {
            Ok(PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            })
        })
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        let subscriber_id = request.subscriber_id().as_str().to_owned();
        Box::pin(async move {
            Ok(Box::new(AsyncCloseFailingReceiver {
                subscriber_id,
                close_attempts: AtomicUsize::new(0),
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }

    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

struct AsyncCloseFailingReceiver {
    subscriber_id: String,
    close_attempts: AtomicUsize,
}

impl AsyncEventSubscriptionSpi for AsyncCloseFailingReceiver {
    fn receive<'a>(&'a mut self, _: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        Box::pin(async { Ok(ReceiveOutcome::TimedOut) })
    }

    fn settle<'a>(&'a mut self, _: &SettlementToken, _: DeliveryDisposition) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async { Ok(()) })
    }

    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        let attempt = self.close_attempts.fetch_add(1, Ordering::AcqRel);
        let subscriber_id = self.subscriber_id.clone();
        Box::pin(async move {
            if attempt == 0 {
                Err(close_error(&subscriber_id))
            } else {
                Ok(())
            }
        })
    }
}

fn close_error(subscriber_id: &str) -> SpiError {
    SpiError::Operation {
        provider_id: "close-history".into(),
        operation: "close",
        resource: Some(subscriber_id.into()),
        kind: "close_failed",
        retryable: Some(false),
        source: Box::new(Error::other(format!("cannot close {subscriber_id}"))),
    }
}

fn topic() -> Topic<String> {
    Topic::new("close-history.events").expect("valid topic")
}

#[test]
fn test_sync_close_failure_history_is_bounded_and_counted() {
    let bus = EventBus::from_spi(
        ProviderId::new("close-history").expect("valid provider ID"),
        Arc::new(SyncCloseFailingBus),
    )
    .expect("valid provider capabilities");
    let topic = topic();

    for index in 0..FAILURE_COUNT {
        let subscriber_id = format!("sync-{index}");
        let subscription = bus
            .subscribe(
                SubscribeRequest::new(&subscriber_id, topic.clone()).expect("valid subscriber"),
                |_| (),
            )
            .expect("subscription starts");
        let error = subscription.cancel().expect_err("provider close fails");
        assert!(matches!(error, LifecycleError::SubscriptionClose(_)));
    }

    let ShutdownError::SubscriptionClose(errors) = bus
        .shutdown(ShutdownMode::Immediate)
        .expect_err("retained close failures remain visible")
    else {
        panic!("shutdown must report subscription close failures");
    };
    assert_eq!(errors.len(), 256);
    assert_eq!(errors.total_count(), 300);
    assert_eq!(errors.omitted_count(), 44);
    assert!(errors.is_truncated());
    assert_eq!(
        errors.iter().next().expect("first sample").subscriber_id().as_str(),
        "sync-0"
    );
    assert!(std::error::Error::source(&errors).is_some());
}

#[test]
fn test_async_close_failure_history_is_bounded_and_counted() {
    support::manual_async::block_on(async {
        let bus = AsyncEventBus::from_spi(
            ProviderId::new("close-history").expect("valid provider ID"),
            Arc::new(AsyncCloseFailingBus),
        )
        .expect("valid provider capabilities");
        let topic = topic();

        for index in 0..FAILURE_COUNT {
            let subscriber_id = format!("async-{index}");
            let mut subscription = bus
                .subscribe(SubscribeRequest::new(&subscriber_id, topic.clone()).expect("valid subscriber"))
                .await
                .expect("subscription starts");
            let error = subscription.close().await.expect_err("first provider close fails");
            assert!(matches!(error, LifecycleError::SubscriptionClose(_)));
            subscription.close().await.expect("retry succeeds");
        }

        let ShutdownError::SubscriptionClose(errors) = bus
            .shutdown(ShutdownMode::Immediate)
            .await
            .expect_err("retained close failures remain visible")
        else {
            panic!("shutdown must report subscription close failures");
        };
        assert_eq!(errors.len(), 256);
        assert_eq!(errors.total_count(), 300);
        assert_eq!(errors.omitted_count(), 44);
        assert!(errors.is_truncated());
        assert_eq!(
            errors.iter().next().expect("first sample").subscriber_id().as_str(),
            "async-0"
        );
        assert!(std::error::Error::source(&errors).is_some());
    });
}
