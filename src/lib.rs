// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Type-safe event bus facade and provider SPI.

#![deny(missing_docs)]

pub mod codec;
pub mod error;
pub mod facade;
pub mod local;
pub mod model;
pub mod notification;
pub mod pipeline;
pub mod registry;
pub mod spi;

pub use error::CapabilityError;
pub use error::CodecError;
pub use error::ConfigurationError;
pub use error::DeliveryError;
pub use error::EventBusError;
pub use error::EventIdGenerationError;
pub use error::LifecycleError;
pub use error::ProviderError;
pub use error::PublishError;
pub use error::ReceiveError;
pub use error::SettlementError;
pub use error::ShutdownError;
pub use error::SpiError;
pub use error::SubscribeError;
pub use facade::AsyncEventBus;
pub use facade::AsyncSubscription;
pub use facade::DeliveryAdmissionConfig;
pub use facade::DiagnosticObserverHandle;
pub use facade::EventBus;
pub use facade::EventBusFacadeConfig;
pub use facade::IntoHandlerResult;
pub use facade::PublishMetricsSnapshot;
pub use facade::Subscription;
pub use facade::WaitOutcome;
pub use model::EventId;
pub use model::SubscriberId;
pub use notification::NotificationOutcome;
pub use notification::NotificationPublisher;
pub use notification::NotificationStatsSnapshot;
pub use notification::TryPublishError;
pub use pipeline::Diagnostic;
pub use registry::AsyncEventBusProvider;
pub use registry::AsyncEventBusRegistry;
pub use registry::EventBusConfig;
pub use registry::EventBusProvider;
pub use registry::EventBusProviderError;
pub use registry::EventBusRegistry;
pub use registry::EventBusSpec;
pub use registry::RequiredCapabilities;
