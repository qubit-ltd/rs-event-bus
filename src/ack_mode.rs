/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Message acknowledgement modes.

/// Controls how subscriber handlers acknowledge event processing.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Hash)]
pub enum AckMode {
    /// A handler success automatically acknowledges the event.
    #[default]
    Auto,
    /// Handler code receives an [`crate::Acknowledgement`] and controls ACK/NACK.
    Manual,
}
