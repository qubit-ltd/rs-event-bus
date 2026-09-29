// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Terminal processing choices for subscriber failures.

/// Distinguishes local retries from provider settlement actions.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliveryFailureAction {
    /// Re-run handler and middleware on the same received delivery.
    RetryLocally,
    /// Release the message for provider redelivery.
    Requeue,
    /// Publish a facade dead-letter record before rejecting the provider
    /// message.
    DeadLetter,
    /// Stop processing without dead-letter publication.
    Discard,
}
