// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal regression for provider-wide subscription identity cleanup.

use std::any::TypeId;

use qubit_id::Id;
use qubit_spi::ServiceProvider;

use crate::local::LocalEventBusConfig;
use crate::local::LocalEventBusProvider;
use crate::model::ProviderOptions;
use crate::model::StartPosition;
use crate::model::SubscriberId;
use crate::model::SubscriptionDurability;
use crate::registry::EventBusConfig;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TopicAddress;

fn subscription_request(id: u64, topic: &str) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new(topic).expect("valid topic"),
        SubscriberId::new(format!("internal-{id}")).expect("valid subscriber ID"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<u32>(),
    )
}

#[test]
fn test_stale_subscription_identity_can_be_reused_after_receiver_drop() {
    let config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().provider_options());
    let spi = LocalEventBusProvider
        .create_configured(&config)
        .expect("valid local configuration");
    let stale = spi
        .subscribe(subscription_request(9003, "internal.state.first"))
        .expect("first subscription is accepted");
    drop(stale);

    let replacement = spi
        .subscribe(subscription_request(9003, "internal.state.second"))
        .expect("stale provider-wide ID is released after receiver drop");
    drop(replacement);
}
