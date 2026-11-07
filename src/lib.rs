// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Qubit Event Bus
//!
//! `qubit-event-bus` provides typed topics and synchronous and asynchronous
//! facades over a provider SPI. The built-in local provider is useful for
//! in-process work; provider registries and conformance helpers let an
//! application add other transports.
//!
//! ## Quick start
//!
//! This example subscribes before publishing, checks provider admission, and
//! explicitly shuts down the bus:
//!
//! <!-- event-bus-source: examples/local_delivery.rs -->
//! ```rust
//! //! Minimal local-provider example showing subscription, publication, and
//! //! graceful shutdown.
//!
//! use std::sync::mpsc;
//! use std::time::Duration;
//!
//! use qubit_event_bus::EventBus;
//! use qubit_event_bus::local::LocalEventBusConfig;
//! use qubit_event_bus::model::AdmissionRequirement;
//! use qubit_event_bus::model::PublishRequest;
//! use qubit_event_bus::model::SubscribeRequest;
//! use qubit_event_bus::model::Topic;
//! use qubit_event_bus::spi::ShutdownMode;
//! use qubit_event_bus::spi::ShutdownOutcome;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let bus = EventBus::local(LocalEventBusConfig::new())?;
//!     let topic = Topic::<String>::new("orders.created")?;
//!     let (sender, receiver) = mpsc::channel();
//!     let _subscription = bus.subscribe(SubscribeRequest::new("audit", topic.clone())?, move |delivery| {
//!         sender.send(delivery.payload().clone()).unwrap();
//!     })?;
//!     let _ = bus.publish_checked(
//!         PublishRequest::new(topic, "order-42".to_owned())?,
//!         AdmissionRequirement::ProviderOrDestinationAccepted,
//!     )?;
//!     assert_eq!(receiver.recv_timeout(Duration::from_secs(3))?, "order-42");
//!     let shutdown_report = bus.shutdown(ShutdownMode::Graceful {
//!         timeout: Duration::from_secs(3),
//!     })?;
//!     assert_eq!(shutdown_report.outcome, ShutdownOutcome::Complete);
//!     assert_eq!(shutdown_report.known_abandoned_deliveries, 0);
//!     assert!(shutdown_report.provider_may_have_abandoned_deliveries);
//!     Ok(())
//! }
//! ```
//!
//! ## Choosing a facade
//!
//! Use [`EventBus`] with [`Subscription`] for callback based synchronous
//! delivery. Use [`AsyncEventBus`] with [`AsyncSubscription`] when the
//! application already drives asynchronous tasks. An async subscription must
//! keep [`AsyncSubscription::run`] polled; pausing it retains the session and
//! its facade owned work, while dropping the handle closes the ephemeral
//! provider subscription.
//!
//! ## Admission and lifecycle
//!
//! A [`model::PublishReceipt`] reports provider admission, not handler
//! execution or business completion. With partial admission, retrying the whole
//! publish may duplicate deliveries already accepted by some destinations. A
//! synchronous [`Subscription`] handle drop does not cancel its receiver; call
//! its explicit cancellation operation. Shut down the bus explicitly with a
//! bounded graceful shutdown when the application needs a completion report.
//! The built-in local provider is in-memory and non-persistent: process exit,
//! close, or drop can discard pending work.
//!
//! [`model::AdmissionRequirement`] selects the admission condition a checked
//! publish must meet. [`EventBusRegistry`] and [`AsyncEventBusRegistry`] create
//! facades from registered providers. Providers implement the low-level
//! contracts [`spi::AsyncEventBusSpi`] and [`spi::EventBusSpi`]. The built-in
//! [`local::AsyncLocalEventBusProvider`] and [`local::LocalEventBusProvider`]
//! provide in-process implementations.
//!
//! ## Further reading
//!
//! These links point to versioned documentation snapshots. When updating user
//! guides or design documents to match an API change, commit the documents
//! first, then update all four links to the full SHA of that commit.
//!
//! - [User guide](https://github.com/qubit-ltd/rs-event-bus/blob/6e8db1c737e4cd9d98acccce9631a6f5a2690ca1/doc/user_guide.md)
//! - [用户指南](https://github.com/qubit-ltd/rs-event-bus/blob/6e8db1c737e4cd9d98acccce9631a6f5a2690ca1/doc/user_guide.zh_CN.md)
//! - [Design](https://github.com/qubit-ltd/rs-event-bus/blob/6e8db1c737e4cd9d98acccce9631a6f5a2690ca1/doc/design.md)
//! - [设计文档](https://github.com/qubit-ltd/rs-event-bus/blob/6e8db1c737e4cd9d98acccce9631a6f5a2690ca1/doc/design.zh_CN.md)

#![deny(missing_docs)]
pub use error::CapabilityError;
pub use error::CheckedPublishError;
pub use error::CodecError;
pub use error::ConfigurationError;
pub use error::DeliveryError;
pub use error::EventBusError;
pub use error::EventIdGenerationError;
pub use error::FacadeBuildError;
pub use error::LifecycleError;
pub use error::ProviderError;
pub use error::PublishError;
pub use error::PublishFailure;
pub use error::ReceiveError;
pub use error::SettlementError;
pub use error::ShutdownError;
pub use error::SpiError;
pub use error::SubscribeError;
pub use facade::AsyncEventBus;
pub use facade::AsyncSubscription;
pub use facade::AsyncSubscriptionRunState;
pub use facade::DiagnosticObserverHandle;
pub use facade::EventBus;
pub use facade::EventBusFacadeConfig;
pub use facade::EventBusShutdown;
pub use facade::IntoHandlerResult;
pub use facade::PayloadLimits;
pub use facade::PublishMetricsSnapshot;
pub use facade::ShutdownReport;
pub use facade::Subscription;
pub use facade::WaitOutcome;
pub use model::EventId;
pub use model::SubscriberId;
pub use notification::NotificationOutcome;
pub use notification::NotificationPublisher;
pub use notification::NotificationStatsSnapshot;
pub use notification::TryPublishError;
pub use notification::WeightedNotificationPublisher;
pub use pipeline::Diagnostic;
pub use registry::AsyncEventBusProvider;
pub use registry::AsyncEventBusRegistry;
pub use registry::EventBusConfig;
pub use registry::EventBusProvider;
pub use registry::EventBusProviderError;
pub use registry::EventBusRegistry;
pub use registry::EventBusSpec;
pub use registry::RequiredCapabilities;

pub mod codec;
pub mod error;
pub mod facade;
mod internal;
pub mod local;
pub mod model;
pub mod notification;
pub mod pipeline;
pub mod registry;
pub mod spi;
mod util;

#[cfg(test)]
mod tests;
