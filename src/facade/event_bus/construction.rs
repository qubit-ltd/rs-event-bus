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
use crate::spi::EventBusSpi;

impl EventBus {
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
        let capabilities =
            crate::spi::panic_boundary::catch_spi_call(provider_id.as_str(), "capabilities", None, || {
                spi.capabilities()
            })?;
        let scheduler = SyncDeliveryScheduler::new(config.sync_delivery_scheduler());
        let subscription_worker_budget = Arc::new(SubscriptionWorkerBudget {
            active: AtomicUsize::new(0),
            limit: config.sync_delivery_scheduler().max_subscription_workers().get(),
        });
        Ok(Self {
            inner: Arc::new(EventBusInner {
                spi,
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
