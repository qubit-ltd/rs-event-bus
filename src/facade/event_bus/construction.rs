// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event bus construction operations.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;

use qubit_clock::MonotonicClock;
use qubit_clock::StdMonotonicClock;

use crate::EventBus;
use crate::EventBusConfig;
use crate::EventBusFacadeConfig;
use crate::EventBusRegistry;
use crate::ProviderError;
use crate::facade::LifecycleTracker;
use crate::facade::PublishMetrics;
use crate::facade::event_bus::internal::EventBusInner;
use crate::facade::event_bus::internal::OperationGate;
use crate::facade::event_bus::internal::ShutdownState;
use crate::facade::event_bus::internal::SubscriptionWorkerBudget;
use crate::facade::internal::LifecycleState;
use crate::facade::shutdown_coordinator::ShutdownCoordinator;
use crate::facade::sync_delivery_scheduler::SyncDeliveryScheduler;
use crate::local::LocalEventBusConfig;
use crate::model::ProviderId;
use crate::pipeline::PublisherPipeline;
use crate::spi::EventBusCapabilities;
use crate::spi::EventBusSpi;

impl EventBus {
    /// Returns the provider capability snapshot captured during construction.
    ///
    /// # Returns
    /// The immutable capabilities reported by the provider when this facade
    /// was created.
    #[must_use]
    #[inline]
    pub fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities
    }

    /// Returns the stable identity assigned to this provider facade.
    ///
    /// # Returns
    /// The provider identity supplied to the facade constructor.
    #[must_use]
    #[inline]
    pub fn provider_id(&self) -> &ProviderId {
        &self.inner.provider_id
    }

    /// Creates a synchronous facade using the built-in local provider.
    ///
    /// This convenience path uses the same provider registry and SPI
    /// validation as explicit provider selection. The supplied configuration
    /// controls local transport queues; facade behavior retains its defaults.
    ///
    /// # Parameters
    /// - `config`: configuration for the built-in local transport.
    ///
    /// # Returns
    /// A running facade backed by the local provider.
    ///
    /// # Errors
    /// Returns an error if the local provider cannot be registered or created,
    /// or if the supplied local configuration is invalid.
    pub fn local(config: LocalEventBusConfig) -> Result<Self, ProviderError> {
        let registry = EventBusRegistry::with_local().map_err(|source| ProviderError::Resolution {
            source: Box::new(source),
        })?;
        let config = EventBusConfig::default().with_provider_options(config.provider_options());
        registry.create(&config)
    }

    /// Creates a running facade around an already-created provider SPI.
    ///
    /// The facade takes shared ownership of the provider. Each subscription has
    /// one managed coordinator that owns its SPI receiver and polls blocking
    /// receives at a finite interval; handler callbacks run on the facade-wide
    /// bounded pool. Use a registry when provider selection or creation
    /// fallback is required.
    ///
    /// # Parameters
    /// - `provider_id`: stable identifier assigned to the provider.
    /// - `spi`: provider implementation shared with the facade.
    ///
    /// # Returns
    /// A running facade with default facade configuration.
    ///
    /// # Errors
    /// Returns the provider's capability call failure. A Rust panic from that
    /// call is reported as a terminal `provider_panicked` SPI error.
    pub fn from_spi(provider_id: ProviderId, spi: Arc<dyn EventBusSpi>) -> Result<Self, crate::error::SpiError> {
        Self::with_config(provider_id, spi, EventBusFacadeConfig::default())
    }

    /// Creates a facade using application-supplied codec registrations.
    ///
    /// The codec registry is frozen into the publisher pipeline at
    /// construction; later changes require constructing a new facade.
    ///
    /// # Parameters
    /// - `provider_id`: stable identifier assigned to the provider.
    /// - `spi`: provider implementation shared with the facade.
    /// - `config`: codec, middleware, and delivery settings for the facade.
    ///
    /// # Returns
    /// A running facade using the supplied configuration.
    ///
    /// # Errors
    /// Returns the provider's capability call failure, including a terminal
    /// `provider_panicked` error when the SPI unwinds.
    pub fn with_config(
        provider_id: ProviderId,
        spi: Arc<dyn EventBusSpi>,
        config: EventBusFacadeConfig,
    ) -> Result<Self, crate::error::SpiError> {
        Self::with_config_and_clock(provider_id, spi, config, Arc::new(StdMonotonicClock::new()))
    }

    /// Creates a facade with an injectable monotonic settlement clock.
    /// Provider capability failures are returned as structured SPI errors.
    /// Owners sample this clock after wakeups; advancing a manual clock alone
    /// does not wake receiver threads.
    ///
    /// # Parameters
    /// - `provider_id`: stable provider identity included in diagnostics.
    /// - `spi`: backend whose receivers are each owned by one worker thread.
    /// - `config`: validated scheduling, payload, and settlement policies.
    /// - `clock`: monotonic source used for settlement budgets and delivery
    ///   metrics.
    ///
    /// # Returns
    /// A facade whose receiver owners share the injected clock.
    ///
    /// # Errors
    /// Returns a structured SPI error if querying provider capabilities panics.
    pub fn with_config_and_clock(
        provider_id: ProviderId,
        spi: Arc<dyn EventBusSpi>,
        config: EventBusFacadeConfig,
        clock: Arc<dyn MonotonicClock>,
    ) -> Result<Self, crate::error::SpiError> {
        let capabilities =
            crate::spi::panic_boundary::catch_spi_call(provider_id.as_str(), "capabilities", None, || {
                spi.capabilities()
            })?;
        let scheduler = SyncDeliveryScheduler::new(config.delivery_scheduling());
        let subscription_worker_budget = Arc::new(SubscriptionWorkerBudget {
            active: AtomicUsize::new(0),
            limit: config.delivery_scheduling().max_subscriptions().get(),
        });
        Ok(Self {
            inner: Arc::new(EventBusInner {
                spi,
                clock,
                delivery_metrics: Arc::new(crate::facade::internal::DeliveryMetrics::default()),
                capabilities,
                provider_id: provider_id.clone(),
                publisher: PublisherPipeline::new(
                    provider_id,
                    config.codec_registry().clone(),
                    capabilities,
                    config.payload_limits().max_publish_bytes(),
                ),
                facade_config: config,
                lifecycle: Mutex::new(LifecycleState::Running),
                operations: OperationGate::default(),
                tracker: LifecycleTracker::new(),
                subscriptions: Mutex::new(HashMap::new()),
                close_errors: Mutex::new(Vec::new()),
                close_error_snapshot: Mutex::new(None),
                next_subscription_id: AtomicU64::new(1),
                observers: Mutex::new(Vec::new()),
                shutdown_gate: Mutex::new(ShutdownState { report: None }),
                shutdown_coordinator: ShutdownCoordinator::new(),
                scheduler,
                subscription_worker_budget,
                publish_metrics: PublishMetrics::default(),
                abandoned_deliveries: AtomicU64::new(0),
            }),
        })
    }
}

impl EventBus {
    /// Returns active delivery gauges and cumulative bus counters.
    /// Snapshot clock failures stop affected subscriptions and are diagnosed;
    /// the fallback preserves exact gauges with an unknown oldest age.
    ///
    /// # Returns
    /// Current live delivery gauges combined with bus-wide cumulative counters.
    #[must_use = "delivery metrics are the current bus diagnostics"]
    #[inline]
    pub fn delivery_metrics(&self) -> crate::facade::DeliveryMetricsSnapshot {
        self.inner.delivery_metrics.snapshot(self.inner.delivery_gauges(None))
    }
}
