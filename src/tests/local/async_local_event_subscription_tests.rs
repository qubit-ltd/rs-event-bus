// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal tests for asynchronous local receiver lifecycle behavior.

use std::any::TypeId;
use std::future::Future;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use qubit_id::Id;

use crate::local::AsyncLocalEventBusSpi;
use crate::local::LocalEventBusConfig;
use crate::model::ProviderOptions;
use crate::model::StartPosition;
use crate::model::SubscriberId;
use crate::model::SubscriptionDurability;
use crate::spi::AsyncEventBusSpi;
use crate::spi::ReceiveOutcome;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TopicAddress;

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

fn subscription_request(id: u64) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new("internal.async.lifecycle").expect("valid topic"),
        SubscriberId::new(format!("consumer-{id}")).expect("valid subscriber ID"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<u32>(),
    )
}

#[test]
fn test_close_is_idempotent_and_receive_reports_closed() {
    let spi = AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).expect("valid config");
    let mut receiver =
        ready(spi.subscribe(subscription_request(9001))).expect("subscription is accepted");

    ready(receiver.close()).expect("first close succeeds");
    ready(receiver.close()).expect("repeated close succeeds");
    assert!(matches!(
        ready(receiver.receive(Duration::ZERO)).expect("closed receive succeeds"),
        ReceiveOutcome::Closed
    ));
}
