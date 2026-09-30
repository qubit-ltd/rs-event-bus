// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async delivery contracts exercised through the caller-driven facade.

use std::future::Future;
use std::sync::Arc;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

use crate::support::manual_async::block_on;

#[test]
fn test_delivery_preserves_decoded_payload_and_transport_headers() {
    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::default()).expect("local SPI"));
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").expect("provider ID"), spi).expect("event bus");
    let topic = Topic::<String>::new("delivery.mirror").expect("topic");
    let mut subscription =
        block_on(bus.subscribe(SubscribeRequest::new("delivery-mirror", topic.clone()).expect("request")))
            .expect("subscription");
    let (delivered_tx, delivered_rx) = mpsc::channel();
    let mut runner = Box::pin(subscription.run(move |delivery| {
        let delivered_tx = delivered_tx.clone();
        async move {
            delivered_tx
                .send((
                    delivery.event().payload().clone(),
                    delivery.event().headers().get("trace").cloned(),
                ))
                .expect("test is receiving delivery");
            Ok(())
        }
    }));
    assert!(
        runner
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    let request = PublishRequest::builder()
        .topic(topic)
        .payload("decoded payload".to_owned())
        .header("trace", "delivery-42")
        .build()
        .expect("publish request");
    block_on(bus.publish(request)).expect("publish");

    let mut delivered = None;
    for _ in 0..8 {
        let _ = runner.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        if let Ok(value) = delivered_rx.try_recv() {
            delivered = Some(value);
            break;
        }
    }
    assert_eq!(
        delivered,
        Some(("decoded payload".to_owned(), Some("delivery-42".to_owned())))
    );

    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    for _ in 0..32 {
        let runner_result = runner.as_mut().poll(&mut context);
        let shutdown_result = shutdown.as_mut().poll(&mut context);
        if let Poll::Ready(result) = shutdown_result {
            let _ = result.expect("shutdown");
            assert!(matches!(runner_result, Poll::Ready(Ok(()))));
            return;
        }
    }
    panic!("runner and shutdown should finish after provider shutdown");
}
