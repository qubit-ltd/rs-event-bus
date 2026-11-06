// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public async API regression probes for future destruction panics.
mod support;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::SpiError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;

use crate::support::fake_spi::FakeAsyncEventBusSpi;
use crate::support::fake_spi::inbound_message;
use crate::support::manual_async::block_on;
use crate::support::panic_hook::PanicHookGuard;

struct DropPanicSpi {
    inner: FakeAsyncEventBusSpi,
    drops: Arc<AtomicUsize>,
    pending: bool,
}

impl AsyncEventBusSpi for DropPanicSpi {
    fn capabilities(&self) -> qubit_event_bus::spi::EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        if self.pending {
            Box::pin(PendingThenDropPanic(self.drops.clone()))
        } else {
            Box::pin(AcceptThenDropPanic(self.drops.clone()))
        }
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.inner.subscribe(request)
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

struct AcceptThenDropPanic(Arc<AtomicUsize>);
impl Future for AcceptThenDropPanic {
    type Output = Result<PublishAcknowledgement, SpiError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        }))
    }
}
impl Drop for AcceptThenDropPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("provider future cleanup panic");
    }
}

struct PendingThenDropPanic(Arc<AtomicUsize>);
impl Future for PendingThenDropPanic {
    type Output = Result<PublishAcknowledgement, SpiError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for PendingThenDropPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("pending provider future cleanup panic");
    }
}

#[test]
fn test_future_drop_panic_public_async_publish_cancellation_has_no_result() {
    let _hook = PanicHookGuard::suppress();
    let drops = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(DropPanicSpi {
        inner: FakeAsyncEventBusSpi::new(),
        drops: drops.clone(),
        pending: true,
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("cancel-drop-panic").unwrap(), spi).unwrap();
    let request = PublishRequest::new(Topic::<u32>::new("future.cancel.drop.panic").unwrap(), 7).unwrap();
    let mut publish = Box::pin(bus.publish(request));
    assert!(matches!(
        publish
            .as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop())),
        Poll::Pending
    ));
    let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(publish)));
    assert!(cleanup.is_ok(), "cancel cleanup panic must not escape Drop");
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "pending provider future is destroyed once"
    );
    assert_eq!(
        bus.publish_metrics().attempts,
        1,
        "cancelled attempted publish has no terminal result"
    );
}

#[test]
fn test_future_drop_panic_public_async_publish_ready_cleanup_becomes_uncertain_spi_error() {
    let _hook = PanicHookGuard::suppress();
    let drops = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(DropPanicSpi {
        inner: FakeAsyncEventBusSpi::new(),
        drops: drops.clone(),
        pending: false,
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("drop-panic").unwrap(), spi).unwrap();
    let request = PublishRequest::new(Topic::<u32>::new("future.drop.panic").unwrap(), 42).unwrap();

    let result = block_on(bus.publish(request));
    let Err(failure) = result else {
        panic!("provider cleanup panic must fail publication");
    };
    let PublishError::Spi(error) = failure.into_cause() else {
        panic!("provider cleanup panic must be classified as a structured SPI error");
    };
    assert!(matches!(
        &error,
        SpiError::Operation {
            kind: "provider_panicked",
            ..
        }
    ));
    assert_eq!(
        error.publish_effect(),
        qubit_event_bus::model::PublishEffect::MayHaveBeenAccepted
    );
    let source = std::error::Error::source(&error).expect("provider panic source is retained");
    assert!(source.to_string().contains("provider future cleanup panicked"));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the provider future must be destroyed once"
    );
    assert_eq!(
        bus.publish_metrics().attempts,
        1,
        "the provider call has occurred, so it cannot be reported as not accepted"
    );
}

struct HandlerDropPanic(Arc<AtomicUsize>);
impl Future for HandlerDropPanic {
    type Output = Result<(), qubit_event_bus::DeliveryError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(()))
    }
}
impl Drop for HandlerDropPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("handler future cleanup panic");
    }
}

#[test]
fn test_future_drop_panic_public_async_subscription_handler_cleanup_is_classified() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("handler-drop-panic").unwrap(), spi.clone()).unwrap();
    let topic = Topic::<u32>::new("test.topic").unwrap();
    let options = qubit_event_bus::model::SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| FailureDirective::Discard)
        .build();
    let subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("drop-panic", topic)
                .unwrap()
                .with_options(options),
        ),
    )
    .unwrap();
    let event_id = "handler-future-drop-panic-event";
    spi.enqueue(inbound_message(Some(SettlementToken::new(subscription.id(), event_id))));
    let drops = Arc::new(AtomicUsize::new(0));
    let handler_drops = drops.clone();
    let failure = Arc::new(std::sync::Mutex::new(None::<String>));
    let observed_failure = failure.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::DeliveryFailed { error, .. } = diagnostic {
            *observed_failure.lock().unwrap() = Some(error.to_string());
        }
    });
    let runner =
        spawn(move || block_on(subscription.run(move |_: Delivery<u32>| HandlerDropPanic(handler_drops.clone()))));
    let deadline = Instant::now() + Duration::from_secs(5);
    while (spi.settlement_count() == 0 || failure.lock().unwrap().is_none()) && Instant::now() < deadline {
        sleep(Duration::from_millis(2));
    }
    let settled = spi.settlement_count() > 0;
    let dispositions = spi.settlement_dispositions();
    let failure_message = failure.lock().unwrap().clone();
    let _shutdown = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    let runner_result = runner.join();
    assert!(settled, "cleanup failure must reach provider settlement");
    assert_eq!(dispositions, [DeliveryDisposition::Reject]);
    assert!(
        failure_message
            .as_deref()
            .is_some_and(|message| message.contains("future cleanup panicked")),
        "terminal delivery diagnostic must identify future cleanup panic: {failure_message:?}"
    );
    assert!(runner_result.is_ok(), "subscription runner thread must not unwind");
    assert!(runner_result.unwrap().is_ok(), "subscription run must finish cleanly");
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "handler future cleanup must be attempted once"
    );
    assert_eq!(
        bus.delivery_metrics().running_handlers,
        0,
        "handler capacity must be released"
    );
}
