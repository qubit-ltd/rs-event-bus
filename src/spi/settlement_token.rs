// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Opaque, single-owner provider settlement token.

use std::any::Any;

use qubit_id::Id;

/// Opaque provider state bound to the subscription that issued it.
///
/// Providers must create tokens using the `subscription_id` from the matching
/// [`super::SpiSubscriptionRequest`]. A facade must only pass a token back to
/// the same subscription; [`Self::belongs_to`] lets it reject mismatches before
/// dispatching settlement. Tokens are intentionally non-cloneable.
pub struct SettlementToken {
    subscription_id: Id,
    state: Box<dyn Any + Send>,
}

impl SettlementToken {
    /// Wraps provider-owned state and binds it to its issuing subscription.
    pub fn new<T: Any + Send>(subscription_id: Id, state: T) -> Self {
        Self {
            subscription_id,
            state: Box::new(state),
        }
    }

    /// Returns whether this token was issued by the specified subscription.
    #[must_use]
    pub fn belongs_to(&self, subscription_id: Id) -> bool {
        self.subscription_id == subscription_id
    }

    /// Borrows provider state when its concrete type matches `T`.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.state.downcast_ref()
    }

    /// Mutably borrows provider state when its concrete type matches `T`.
    pub fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.state.downcast_mut()
    }
}
