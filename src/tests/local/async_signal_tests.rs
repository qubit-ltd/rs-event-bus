// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal regressions for asynchronous local waiter notification.

use std::any::TypeId;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::SystemTime;

use qubit_id::Id;

use crate::local::AsyncLocalEventBusSpi;
use crate::local::LocalEventBusConfig;
use crate::model::EventId;
use crate::model::Headers;
use crate::model::ProviderOptions;
use crate::model::StartPosition;
use crate::model::SubscriberId;
use crate::model::SubscriptionDurability;
use crate::spi::AsyncEventBusSpi;
use crate::spi::OutboundMessage;
use crate::spi::ReceiveOutcome;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TopicAddress;
use crate::spi::TransportPayload;

#[derive(Default)]
struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn subscription_request() -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(9002),
        TopicAddress::new("internal.async.signal").expect("valid topic"),
        SubscriberId::new("signal-probe").expect("valid subscriber ID"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<u32>(),
    )
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(std::task::Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("operation unexpectedly pending"),
    }
}

#[test]
fn test_pending_receive_is_woken_by_publish_and_keeps_message_available() {
    let spi = AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).expect("valid config");
    let mut receiver =
        ready(spi.subscribe(subscription_request())).expect("subscription is accepted");
    let counter = Arc::new(CountWake::default());
    let waker = Waker::from(counter.clone());
    let mut pending_receive = receiver.receive(Duration::MAX);
    assert!(matches!(
        pending_receive
            .as_mut()
            .poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));

    let message = OutboundMessage::new(
        TopicAddress::new("internal.async.signal").expect("valid topic"),
        EventId::new("signal-event").expect("valid event ID"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(17_u32)),
    );
    let _ = ready(spi.publish(message)).expect("publish succeeds");
    assert!(counter.0.load(Ordering::Relaxed) > 0);
    assert!(matches!(
        pending_receive
            .as_mut()
            .poll(&mut Context::from_waker(&waker)),
        Poll::Ready(Ok(ReceiveOutcome::Message(_)))
    ));
}
