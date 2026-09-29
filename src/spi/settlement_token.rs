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
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::SettlementToken;
/// use qubit_id::Id;
///
/// let subscription = Id::new(7);
/// let token = SettlementToken::new(subscription, 42_u64);
/// assert_eq!(token.downcast_ref::<u64>(), Some(&42));
/// ```
#[must_use]
pub struct SettlementToken {
    /// Identity of the receiver authorized to apply this token.
    subscription_id: Id,
    /// Opaque, non-cloneable state interpreted only by the issuing provider.
    state: Box<dyn Any + Send>,
}

impl SettlementToken {
    /// Wraps provider-owned state and binds it to its issuing subscription.
    ///
    /// # Type Parameters
    /// - `T`: provider state type, which must be `Any + Send`.
    ///
    /// # Parameters
    /// - `subscription_id`: identity of the subscription issuing this token.
    /// - `state`: opaque state needed for later settlement.
    ///
    /// # Returns
    /// A non-cloneable token bound to `subscription_id`.
    pub fn new<T: Any + Send>(subscription_id: Id, state: T) -> Self {
        Self {
            subscription_id,
            state: Box::new(state),
        }
    }

    /// Returns whether this token was issued by the specified subscription.
    ///
    /// # Parameters
    /// - `subscription_id`: subscription identity to compare with the issuer.
    ///
    /// # Returns
    /// `true` when the token belongs to the supplied subscription.
    #[must_use]
    #[inline]
    pub fn belongs_to(&self, subscription_id: Id) -> bool {
        self.subscription_id == subscription_id
    }

    /// Borrows provider state when its concrete type matches `T`.
    ///
    /// # Type Parameters
    /// - `T`: concrete state type requested by the provider.
    ///
    /// # Returns
    /// `Some` with a shared reference when the stored state is `T`, otherwise
    /// `None`.
    #[must_use]
    #[inline]
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.state.downcast_ref()
    }

    /// Mutably borrows provider state when its concrete type matches `T`.
    ///
    /// # Type Parameters
    /// - `T`: concrete state type requested by the provider.
    ///
    /// # Returns
    /// `Some` with a mutable reference when the stored state is `T`, otherwise
    /// `None`.
    #[must_use]
    #[inline]
    pub fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.state.downcast_mut()
    }
}
