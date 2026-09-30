// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public configuration and structured settlement contracts.

use std::error::Error;
use std::io::Error as IoError;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::facade::DeliveryMetricsSnapshot;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::SettlementRetryConfig;
use qubit_event_bus::facade::SubscriptionDeliveryMetricsSnapshot;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::SettlementTermination;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::spi::DeliveryDisposition;

#[test]
fn test_new_delivery_limits_are_independent() {
    let limits = DeliverySchedulingConfig::default();
    assert_eq!(limits.max_running_handlers().get(), 4);
    assert_eq!(limits.max_owned_deliveries().get(), 256);
    assert_eq!(limits.max_owned_per_subscription().get(), 32);
    assert_eq!(limits.max_subscriptions().get(), 256);
}

#[test]
fn test_invalid_delivery_limits_report_exact_fields() {
    let n = |value| NonZeroUsize::new(value).expect("positive test value");
    assert!(matches!(
        DeliverySchedulingConfig::new(n(5), n(4), n(4), n(1)),
        Err(ConfigurationError::InvalidField {
            field: "max_running_handlers",
            ..
        })
    ));
    assert!(matches!(
        DeliverySchedulingConfig::new(n(4), n(4), n(5), n(1)),
        Err(ConfigurationError::InvalidField {
            field: "max_owned_per_subscription",
            ..
        })
    ));
    assert!(DeliverySchedulingConfig::new(n(4), n(4), n(4), n(1)).is_ok());
}

#[test]
fn test_facade_config_preserves_new_policies() {
    let n = |value| NonZeroUsize::new(value).expect("positive test value");
    let scheduling = DeliverySchedulingConfig::new(n(2), n(8), n(3), n(7)).expect("valid independent limits");
    let retry = SettlementRetryConfig::new(
        NonZeroU32::new(1).expect("positive attempt limit"),
        Duration::from_secs(2),
        Duration::from_millis(20),
        Duration::from_millis(40),
    )
    .expect("valid retry policy");
    let config = EventBusFacadeConfig::new()
        .with_delivery_scheduling(scheduling)
        .with_settlement_retry(retry);
    assert_eq!(config.delivery_scheduling(), scheduling);
    assert_eq!(config.clone().settlement_retry(), retry);
    assert_eq!(
        EventBusFacadeConfig::default().delivery_scheduling(),
        DeliverySchedulingConfig::default()
    );
}

#[test]
fn test_settlement_stop_preserves_provider_source_chain() {
    let error = Arc::new(SpiError::Operation {
        provider_id: "contract-provider".into(),
        operation: "settle",
        resource: None,
        kind: "offline",
        retryable: Some(false),
        source: Box::new(IoError::other("original provider cause")),
    });
    let reason = SubscriptionStopReason::Settlement {
        event_id: EventId::new("event-contract").expect("valid event ID"),
        disposition: DeliveryDisposition::Accept,
        attempts: 1,
        termination: SettlementTermination::PermanentError,
        error: Arc::clone(&error),
    };
    assert!(reason.to_string().contains("event-contract"));
    let source = reason.source().expect("SPI error source");
    assert!(source.to_string().contains("contract-provider"));
    assert_eq!(
        source.source().expect("original cause").to_string(),
        "original provider cause"
    );
    assert!(matches!(reason, SubscriptionStopReason::Settlement {error: actual, ..} if Arc::ptr_eq(&actual, &error)));
}

#[test]
fn test_public_contracts_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<DeliverySchedulingConfig>();
    assert_send_sync::<SettlementRetryConfig>();
    assert_send_sync::<SubscriptionStopReason>();
    assert_send_sync::<DeliveryMetricsSnapshot>();
    assert_send_sync::<SubscriptionDeliveryMetricsSnapshot>();
    assert_eq!(DeliveryMetricsSnapshot::default().oldest_owned_age, None);
}
