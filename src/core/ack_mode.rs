// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Message acknowledgement modes.

/// Controls how subscriber handlers acknowledge event processing.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Hash)]
pub enum AckMode {
    /// A handler success automatically acknowledges the event.
    #[default]
    Auto,
    /// Handler code receives an [`crate::Acknowledgement`] and controls
    /// ACK/NACK.
    Manual,
}
