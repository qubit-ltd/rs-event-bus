// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal failure-decision contract tests.

use crate::model::FailureDirective;
use crate::pipeline::choose_failure_directive;

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
