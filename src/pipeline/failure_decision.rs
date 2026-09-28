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

#[cfg(test)]
mod tests {
    use super::choose_failure_directive;
    use crate::model::FailureDirective;

    #[test]
    fn test_failure_directive_retry_requires_retry_policy() {
        assert_eq!(
            FailureDirective::Discard,
            choose_failure_directive(false, [Ok(FailureDirective::Retry)]),
        );
        assert_eq!(
            FailureDirective::Retry,
            choose_failure_directive(true, [Ok(FailureDirective::Retry)]),
        );
    }

    #[test]
    fn test_failure_directive_panics_choose_discard_in_registration_order() {
        assert_eq!(
            FailureDirective::Discard,
            choose_failure_directive(true, [Ok(FailureDirective::Retry), Err(())]),
        );
        assert_eq!(
            FailureDirective::DeadLetter,
            choose_failure_directive(true, [Ok(FailureDirective::DeadLetter), Err(())]),
        );
    }
}
