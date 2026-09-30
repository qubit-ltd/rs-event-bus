// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Type-safe synchronous and asynchronous event bus facades.

mod async_event_bus;
mod async_subscription;
mod diagnostic_observer_handle;
mod event_bus;
mod event_bus_facade_config;
mod event_bus_shutdown;
mod internal;
mod into_handler_result;
mod lifecycle;
#[path = "lifecycle_tracker.rs"]
mod lifecycle_tracker;
mod observer_entry;
mod publish_metrics;
mod publish_metrics_snapshot;
mod shutdown_coordinator;
mod shutdown_coordinator_state;
mod shutdown_report;
mod shutdown_result;
mod subscription;
mod sync_delivery_scheduler;
mod wait_outcome;

pub use async_event_bus::AsyncEventBus;
pub use async_subscription::AsyncSubscription;
pub use diagnostic_observer_handle::DiagnosticObserverHandle;
pub use event_bus::EventBus;
pub use event_bus_facade_config::EventBusFacadeConfig;
pub use event_bus_shutdown::EventBusShutdown;
pub use into_handler_result::IntoHandlerResult;
pub(crate) use lifecycle_tracker::DeliveryTrackerGuard;
pub(crate) use lifecycle_tracker::LifecycleTracker;
pub(crate) use publish_metrics::PublishMetrics;
pub use publish_metrics_snapshot::PublishMetricsSnapshot;
pub use shutdown_report::ShutdownReport;
pub use subscription::Subscription;
pub(crate) use subscription::SubscriptionControl;
pub use wait_outcome::WaitOutcome;

mod payload_limits;
pub use payload_limits::PayloadLimits;

mod delivery_scheduling_config;
pub use delivery_scheduling_config::DeliverySchedulingConfig;
mod settlement_retry_config;
pub use settlement_retry_config::SettlementRetryConfig;
mod delivery_metrics_snapshot;
pub use delivery_metrics_snapshot::DeliveryMetricsSnapshot;
mod subscription_delivery_metrics_snapshot;
pub use subscription_delivery_metrics_snapshot::SubscriptionDeliveryMetricsSnapshot;
