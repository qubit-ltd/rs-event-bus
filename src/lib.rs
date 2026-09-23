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
pub mod pipeline;
pub mod registry;
pub mod spi;

pub use error::CapabilityError;
pub use error::CodecError;
pub use error::ConfigurationError;
pub use error::DeliveryError;
pub use error::EventBusError;
pub use error::LifecycleError;
pub use error::ProviderError;
pub use error::PublishError;
pub use error::ReceiveError;
pub use error::SettlementError;
pub use error::ShutdownError;
pub use error::SpiError;
pub use error::SubscribeError;
pub use model::EventId;
pub use model::SubscriberId;
