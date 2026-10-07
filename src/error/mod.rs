// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors classified by event bus operation and layer.

pub use self::capability_error::CapabilityError;
pub use self::checked_publish_error::CheckedPublishError;
pub use self::codec_error::CodecError;
pub use self::configuration_error::ConfigurationError;
pub use self::delivery_attempt_error::DeliveryAttemptError;
pub use self::delivery_error::DeliveryError;
pub use self::event_bus_error::EventBusError;
pub use self::event_id_generation_error::EventIdGenerationError;
pub use self::facade_build_error::FacadeBuildError;
pub use self::lifecycle_error::LifecycleError;
pub use self::provider_error::ProviderError;
pub use self::publish_attempt_error::PublishAttemptError;
pub use self::publish_error::PublishError;
pub use self::publish_failure::PublishFailure;
pub use self::receive_error::ReceiveError;
pub use self::settlement_error::SettlementError;
pub use self::shutdown_error::ShutdownError;
pub use self::spi_error::SpiError;
pub use self::subscribe_error::SubscribeError;
pub use self::subscription_close_errors::SubscriptionCloseErrors;
pub use self::subscription_close_failure::SubscriptionCloseFailure;

mod capability_error;
mod checked_publish_error;
mod codec_error;
mod configuration_error;
mod delivery_attempt_error;
mod delivery_error;
mod event_bus_error;
mod event_id_generation_error;
mod facade_build_error;
mod lifecycle_error;
mod provider_error;
mod publish_attempt_error;
mod publish_error;
mod publish_failure;
mod receive_error;
mod settlement_error;
mod shutdown_error;
mod spi_error;
mod subscribe_error;
mod subscription_close_errors;
mod subscription_close_failure;
