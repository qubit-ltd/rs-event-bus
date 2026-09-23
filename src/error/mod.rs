// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors classified by event bus operation and layer.

mod capability_error;
mod codec_error;
mod configuration_error;
mod delivery_error;
mod event_bus_error;
mod lifecycle_error;
mod provider_error;
mod publish_error;
mod receive_error;
mod settlement_error;
mod shutdown_error;
mod spi_error;
mod subscribe_error;

pub use capability_error::CapabilityError;
pub use codec_error::CodecError;
pub use configuration_error::ConfigurationError;
pub use delivery_error::DeliveryError;
pub use event_bus_error::EventBusError;
pub use lifecycle_error::LifecycleError;
pub use provider_error::ProviderError;
pub use publish_error::PublishError;
pub use receive_error::ReceiveError;
pub use settlement_error::SettlementError;
pub use shutdown_error::ShutdownError;
pub use spi_error::SpiError;
pub use subscribe_error::SubscribeError;
