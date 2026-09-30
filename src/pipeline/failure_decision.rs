// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared, side-effect-free choice among subscriber failure directives.

use crate::model::FailureDirective;

/// Combines callback decisions while applying the same retry and panic rules
/// to synchronous and asynchronous subscribers.
///
/// # Parameters
/// - `retry_enabled`: whether the subscriber has a retry policy.
/// - `directives`: callback outcomes in subscriber registration order; an error
///   represents a callback panic.
///
/// # Returns
/// The first terminal directive, a permitted retry request, or `Discard`.
pub(crate) fn choose_failure_directive(
    retry_enabled: bool,
    directives: impl IntoIterator<Item = Result<FailureDirective, ()>>,
) -> FailureDirective {
    let mut retry_requested = false;
    let mut terminal = None;
    for directive in directives {
        match directive {
            Ok(FailureDirective::Retry) => retry_requested = true,
            Ok(directive) => {
                terminal.get_or_insert(directive);
            }
            Err(()) => {
                terminal.get_or_insert(FailureDirective::Discard);
            }
        }
    }
    terminal.unwrap_or(if retry_requested && retry_enabled {
        FailureDirective::Retry
    } else {
        FailureDirective::Discard
    })
}
