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
use crate::error::ProviderError;
use crate::error::SpiError;
use crate::facade::PublishMetrics;
use crate::facade::async_event_bus::AsyncEventBusInner;
use crate::facade::async_event_bus::AsyncSignal;
use crate::facade::async_event_bus::AsyncTracker;
use crate::facade::async_event_bus::BusState;
use crate::facade::internal::DeliverySchedulerCore;
use crate::local::LocalEventBusConfig;
use crate::model::ProviderId;
use crate::pipeline::PublisherPipeline;
use crate::spi::AsyncEventBusSpi;
use crate::spi::EventBusCapabilities;
use crate::spi::panic_boundary::catch_spi_call;

impl AsyncEventBus {
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
        let registry =
            AsyncEventBusRegistry::with_local().map_err(|source| ProviderError::Resolution {
                source: Box::new(source),
            })?;
        registry
            .create(&EventBusConfig::default().with_provider_options(config.provider_options()))
            .await
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
    /// Returns the provider's capability call failure. A Rust panic from that
    /// call is reported as a terminal `provider_panicked` SPI error.
    pub fn from_spi(
        provider_id: ProviderId,
        spi: Arc<dyn AsyncEventBusSpi>,
    ) -> Result<Self, SpiError> {
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
    /// Returns the provider's capability call failure, including a terminal
    /// `provider_panicked` error when the SPI unwinds.
    pub fn with_config(
        provider_id: ProviderId,
        spi: Arc<dyn AsyncEventBusSpi>,
        config: EventBusFacadeConfig,
    ) -> Result<Self, SpiError> {
        Self::with_config_and_timer(
            provider_id,
            spi,
            config,
            StdMonotonicClock::new().new_timer(),
        )
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
    /// Returns the provider's capability call failure, including a terminal
    /// `provider_panicked` error when the SPI unwinds.
    pub fn with_timer(
        provider_id: ProviderId,
        spi: Arc<dyn AsyncEventBusSpi>,
        timer: Arc<dyn Timer>,
    ) -> Result<Self, SpiError> {
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
    /// Returns the provider's capability call failure, including a terminal
    /// `provider_panicked` error when the SPI unwinds.
    pub fn with_config_and_timer(
        provider_id: ProviderId,
        spi: Arc<dyn AsyncEventBusSpi>,
        config: EventBusFacadeConfig,
        timer: Arc<dyn Timer>,
    ) -> Result<Self, SpiError> {
        let capabilities = catch_spi_call(provider_id.as_str(), "capabilities", None, || {
            spi.capabilities()
        })?;
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
                close_errors: Mutex::new(Vec::new()),
                close_error_snapshot: Mutex::new(None),
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
}
