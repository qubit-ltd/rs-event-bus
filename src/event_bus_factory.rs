/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Factory abstraction for event bus backends.

use crate::{EventBus, EventBusError, EventBusResult, TransactionalEventBus};

/// Factory contract for creating event bus instances.
///
/// This trait mirrors the Java factory interface while using associated types
/// for concrete Rust backends.
pub trait EventBusFactory {
    /// Concrete event bus created by this factory.
    type Bus: EventBus;

    /// Transactional event bus type returned when the backend supports it.
    type TransactionalBus: TransactionalEventBus;

    /// Returns whether this factory can create transactional event buses.
    ///
    /// # Returns
    /// `true` when [`create_transactional`](Self::create_transactional) can
    /// return a supported transactional backend.
    fn is_transactional_supported(&self) -> bool {
        false
    }

    /// Creates a stopped event bus.
    ///
    /// # Returns
    /// Event bus initialized with factory defaults.
    fn create(&self) -> Self::Bus;

    /// Creates and starts an event bus.
    ///
    /// # Returns
    /// Started event bus initialized with factory defaults.
    fn create_started(&self) -> Self::Bus {
        let bus = self.create();
        bus.start();
        bus
    }

    /// Creates a transactional event bus.
    ///
    /// # Returns
    /// Transactional event bus when supported by the backend.
    ///
    /// # Errors
    /// Returns [`EventBusError::UnsupportedOperation`] by default.
    fn create_transactional(&self) -> EventBusResult<Self::TransactionalBus> {
        Err(EventBusError::unsupported_operation("create_transactional"))
    }
}
