/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Conversion trait for handler return values.

use crate::EventBusResult;

/// Converts closure return values into [`EventBusResult<()>`].
///
/// This trait lets event handlers and error handlers return either `()` for a
/// successful outcome or [`EventBusResult<()>`] when they need to report a
/// failure.
pub trait IntoEventBusResult {
    /// Converts the value into an event bus result.
    ///
    /// # Returns
    /// `Ok(())` for successful handler results, or the original error.
    fn into_event_bus_result(self) -> EventBusResult<()>;
}

impl IntoEventBusResult for () {
    /// Converts a unit return value into success.
    fn into_event_bus_result(self) -> EventBusResult<()> {
        Ok(())
    }
}

impl IntoEventBusResult for EventBusResult<()> {
    /// Returns the original event bus result.
    fn into_event_bus_result(self) -> EventBusResult<()> {
        self
    }
}
