// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcomes of a transport receive operation.

use super::DeliveryGap;
use super::InboundMessage;

/// Result of a receive call that is not necessarily an error.
#[non_exhaustive]
pub enum ReceiveOutcome {
    /// A message was received.
    Message(InboundMessage),
    /// One or more messages may have been missed; receiving may continue.
    Gap(DeliveryGap),
    /// No message arrived before the requested timeout.
    TimedOut,
    /// The receiver has closed permanently.
    Closed,
}
