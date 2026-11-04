// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous event bus construction operations.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;

use qubit_clock::MonotonicClock;
use qubit_clock::StdMonotonicClock;
use qubit_clock::Timer;

use crate::AsyncEventBus;
use crate::AsyncEventBusRegistry;
use crate::EventBusConfig;
use crate::EventBusFacadeConfig;
use crate::error::FacadeBuildError;
use crate::error::ProviderError;
use crate::facade::PublishMetrics;
use crate::facade::async_event_bus::AsyncEventBusInner;
use crate::facade::async_event_bus::AsyncSignal;
use crate::facade::async_event_bus::AsyncTracker;
use crate::facade::async_event_bus::BusState;
use crate::facade::internal::DeliverySchedulerCore;
use crate::local::AsyncLocalEventBusSpi;
use crate::local::LocalCapacityMonitor;
use crate::local::LocalEventBusConfig;
use crate::model::ProviderId;
use crate::pipeline::PublisherPipeline;
use crate::spi::AsyncEventBusSpi;
use crate::spi::EventBusCapabilities;
use crate::spi::panic_boundary::catch_spi_call;

impl AsyncEventBus {
    /// Asynchronously creates a facade using the built-in local provider.
    ///
    /// # Parameters
    /// - `config`: Local provider configuration.
    ///
    /// # Returns
    /// A facade connected to the local asynchronous provider.
    ///
    /// # Errors
    /// Returns an error when local provider resolution or construction fails.
    pub async fn local(config: LocalEventBusConfig) -> Result<Self, ProviderError> {
        let registry = AsyncEventBusRegistry::with_local().map_err(|source| ProviderError::Resolution {
            source: Box::new(source),
        })?;
        registry
            .create(&EventBusConfig::default().with_provider_options(config.provider_options()))
            .await
    }

    /// Creates an async local facade and read-only capacity monitor using
    /// default facade settings.
    ///
    /// The monitor shares accounting state only and does not retain queues or
    /// payloads. This concrete local path still validates capabilities through
    /// the normal facade configuration path. Registry based construction does
    /// not provide a monitor. Snapshot values do not reserve capacity for a
    /// later publish, and declared weight is application supplied rather than
    /// process memory.
    /// This is shorthand for [`Self::local_with_config_and_monitor`] with the
    /// default facade configuration.
    ///
    /// # Parameters
    /// - `config`: configuration for the built-in local provider.
    ///
    /// # Returns
    /// The running local facade and a cloneable read-only monitor for its
    /// capacity budget.
    ///
    /// # Errors
    /// Returns [`ProviderError::Creation`] if local configuration validation or
    /// facade construction fails; the original provider or facade error is
    /// retained as the source.
    pub async fn local_with_monitor(
        config: LocalEventBusConfig,
    ) -> Result<(Self, LocalCapacityMonitor), ProviderError> {
        Self::local_with_config_and_monitor(config, EventBusFacadeConfig::default()).await
    }

    /// Creates an asynchronous local facade with custom facade settings and a
    /// read-only monitor for the provider's capacity budget.
    ///
    /// Local settings control provider queues and outstanding capacity;
    /// `facade_config` controls facade middleware, codecs, and delivery
    /// scheduling. The monitor observes the same local SPI used by the facade.
    /// Snapshot values do not reserve capacity for a later publish.
    ///
    /// # Parameters
    /// - `local_config`: configuration for the built-in local provider.
    /// - `facade_config`: middleware, codec, and delivery settings for the
    ///   facade.
    ///
    /// # Returns
    /// The running local facade and a cloneable read-only monitor for its
    /// capacity budget.
    ///
    /// # Errors
    /// Returns [`ProviderError::Creation`] if local configuration validation or
    /// facade construction fails; the original provider or facade error is
    /// retained as the source.
    pub async fn local_with_config_and_monitor(
        local_config: LocalEventBusConfig,
        facade_config: EventBusFacadeConfig,
    ) -> Result<(Self, LocalCapacityMonitor), ProviderError> {
        let spi = AsyncLocalEventBusSpi::new(&local_config).map_err(|source| ProviderError::Creation {
            source: Box::new(source),
        })?;
        let monitor = spi.capacity_monitor();
        let bus = Self::with_config(
            ProviderId::new("local").expect("static provider ID is valid"),
            Arc::new(spi),
            facade_config,
        )
        .map_err(|source| ProviderError::Creation {
            source: Box::new(source),
        })?;
        Ok((bus, monitor))
    }

    /// Creates a usable facade around an already-created asynchronous provider
    /// SPI.
    ///
    /// # Parameters
    /// - `provider_id`: Stable identifier assigned to the provider.
    /// - `spi`: Provider implementation to wrap.
    ///
    /// # Returns
    /// A facade configured with default middleware and a standard timer.
    ///
    /// # Errors
    /// Returns [`FacadeBuildError::Spi`] if querying provider capabilities
    /// fails or panics.
    ///
    /// Clone the facade to share this provider lifecycle. Passing the same SPI
    /// instance to separately constructed facades does not guarantee
    /// independent subscription identities or shutdown behavior.
    #[inline]
    pub fn from_spi(provider_id: ProviderId, spi: Arc<dyn AsyncEventBusSpi>) -> Result<Self, FacadeBuildError> {
        Self::with_config(provider_id, spi, EventBusFacadeConfig::default())
    }

    /// Creates a facade using application-supplied codec registrations.
    ///
    /// The codec registry is frozen into the publisher pipeline at
    /// construction; later changes require constructing a new facade.
    ///
    /// # Parameters
    /// - `provider_id`: Stable identifier assigned to the provider.
    /// - `spi`: Provider implementation to wrap.
    /// - `config`: Facade middleware, codec, and delivery configuration.
    ///
    /// # Returns
    /// A configured facade.
    ///
    /// # Errors
    /// Returns [`FacadeBuildError::Configuration`] for synchronous subscriber
    /// middleware, or [`FacadeBuildError::Spi`] if the provider capability call
    /// fails or panics.
    #[inline]
    pub fn with_config(
        provider_id: ProviderId,
        spi: Arc<dyn AsyncEventBusSpi>,
        config: EventBusFacadeConfig,
    ) -> Result<Self, FacadeBuildError> {
        Self::with_config_and_timer(provider_id, spi, config, StdMonotonicClock::new().new_timer())
    }

    /// Creates a facade using the supplied runtime-neutral timer for deadlines
    /// and asynchronous retry delays.
    ///
    /// # Parameters
    /// - `provider_id`: Stable identifier assigned to the provider.
    /// - `spi`: Provider implementation to wrap.
    /// - `timer`: Timer used for deadlines and retry delays.
    ///
    /// # Returns
    /// A facade configured with default middleware and codecs.
    ///
    /// # Errors
    /// Returns [`FacadeBuildError::Spi`] if querying provider capabilities
    /// fails or panics.
    #[inline]
    pub fn with_timer(
        provider_id: ProviderId,
        spi: Arc<dyn AsyncEventBusSpi>,
        timer: Arc<dyn Timer>,
    ) -> Result<Self, FacadeBuildError> {
        Self::with_config_and_timer(provider_id, spi, EventBusFacadeConfig::default(), timer)
    }

    /// Creates a facade with custom codec registrations and timer.
    ///
    /// # Parameters
    /// - `provider_id`: Stable identifier assigned to the provider.
    /// - `spi`: Provider implementation to wrap.
    /// - `config`: Facade middleware, codec, and delivery configuration.
    /// - `timer`: Timer used for deadlines and retry delays.
    ///
    /// # Returns
    /// A configured facade.
    ///
    /// # Errors
    /// Returns [`FacadeBuildError::Configuration`] for synchronous subscriber
    /// middleware, or [`FacadeBuildError::Spi`] if the provider capability call
    /// fails or panics.
    pub fn with_config_and_timer(
        provider_id: ProviderId,
        spi: Arc<dyn AsyncEventBusSpi>,
        config: EventBusFacadeConfig,
        timer: Arc<dyn Timer>,
    ) -> Result<Self, FacadeBuildError> {
        config.validate_for_async()?;
        let capabilities = catch_spi_call(provider_id.as_str(), "capabilities", None, || spi.capabilities())?;
        let scheduler = Arc::new(DeliverySchedulerCore::new(config.delivery_scheduling()));
        Ok(Self {
            inner: Arc::new(AsyncEventBusInner {
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
                state: Mutex::new(BusState::Running),
                next_subscription_id: AtomicU64::new(1),
                controls: Mutex::new(HashMap::new()),
                close_failure_ledger: Mutex::new(crate::facade::internal::CloseFailureLedger::new()),
                shutdown_active: AtomicBool::new(false),
                shutdown_immediate: AtomicBool::new(false),
                shutdown_signal: AsyncSignal::default(),
                shutdown_mode_signal: AsyncSignal::default(),
                shutdown_report: Mutex::new(None),
                abandoned_deliveries: AtomicU64::new(0),
                observers: Mutex::new(Vec::new()),
                tracker: Arc::new(AsyncTracker::default()),
                scheduler,
                timer,
                publish_metrics: PublishMetrics::default(),
                delivery_metrics: Arc::default(),
            }),
        })
    }

    /// Returns the provider capability snapshot captured during construction.
    ///
    /// # Returns
    /// The immutable capabilities reported by the provider when this facade
    /// was created.
    #[inline]
    pub fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities
    }

    /// Returns the stable identity assigned to this provider facade.
    ///
    /// # Returns
    /// The provider identity supplied to the facade constructor.
    #[inline]
    pub fn provider_id(&self) -> &ProviderId {
        &self.inner.provider_id
    }
}
