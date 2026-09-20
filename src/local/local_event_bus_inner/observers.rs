// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Observer registration and notification for the local event bus.

use std::sync::Arc;

use super::LocalEventBusInner;
use crate::DeliveryFailure;
use crate::EventBusError;
use crate::EventBusResult;

/// Callback notified about internal event-bus errors.
pub(super) type ErrorObserverFn = dyn Fn(&EventBusError) + Send + Sync + 'static;

/// Callback notified about terminal delivery failures.
pub(super) type DeliveryFailureObserverFn = dyn Fn(&DeliveryFailure) + Send + Sync + 'static;

impl LocalEventBusInner {
    /// Adds an error observer.
    ///
    /// # Parameters
    /// - `observer`: Callback notified about internal callback failures.
    ///
    /// # Returns
    /// `Ok(())` when the observer is stored.
    ///
    /// # Errors
    /// Returns a lock-poisoning error when the observer registry is
    /// unavailable.
    pub(crate) fn add_error_observer(&self, observer: Arc<ErrorObserverFn>) -> EventBusResult<()> {
        self.error_observers
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("error_observers"))?
            .push(observer);
        Ok(())
    }

    /// Adds a terminal delivery-failure observer.
    ///
    /// # Parameters
    /// - `observer`: Callback notified after a delivery fails permanently.
    ///
    /// # Returns
    /// `Ok(())` when the observer is stored.
    ///
    /// # Errors
    /// Returns a lock-poisoning error when the observer registry is
    /// unavailable.
    pub(crate) fn add_delivery_failure_observer(&self, observer: Arc<DeliveryFailureObserverFn>) -> EventBusResult<()> {
        self.delivery_failure_observers
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("delivery_failure_observers"))?
            .push(observer);
        Ok(())
    }

    /// Notifies registered delivery-failure observers.
    ///
    /// # Parameters
    /// - `failure`: Terminal delivery failure to observe.
    ///
    /// A poisoned observer registry suppresses notification. Panics from one
    /// observer are isolated so later observers are still notified.
    pub(crate) fn observe_delivery_failure(&self, failure: &DeliveryFailure) {
        let Ok(observers) = self
            .delivery_failure_observers
            .lock()
            .map(|observers| observers.clone())
        else {
            return;
        };
        for observer in observers {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(failure)));
        }
    }

    /// Notifies registered error observers.
    ///
    /// # Parameters
    /// - `error`: Internal failure to observe.
    ///
    /// A poisoned observer registry suppresses notification. Panics from one
    /// observer are isolated so later observers are still notified.
    pub(crate) fn observe_error(&self, error: &EventBusError) {
        let Ok(observers) = self.error_observers.lock().map(|observers| observers.clone()) else {
            return;
        };
        for observer in observers {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(error)));
        }
    }
}
