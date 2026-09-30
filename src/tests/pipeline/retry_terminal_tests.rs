// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal terminal retry-decision contract tests.

use qubit_retry::RetryErrorReason;
use qubit_retry::RetryLimitKind;

use crate::model::FailureDirective;
use crate::pipeline::terminal_directive;

#[test]
fn test_terminal_directive_preserves_non_retry_requests_across_terminal_reasons() {
    let reasons = [
        RetryErrorReason::Aborted,
        RetryErrorReason::Exhausted {
            limit: RetryLimitKind::Attempts,
        },
    ];
    let directives = [
        FailureDirective::Requeue,
        FailureDirective::DeadLetter,
        FailureDirective::Discard,
    ];

    for reason in &reasons {
        for requested in directives {
            assert_eq!(terminal_directive(reason, requested), requested);
        }
    }
}

#[test]
fn test_terminal_local_retry_is_discarded_after_each_non_rule_terminal_reason() {
    let reasons = [
        RetryErrorReason::Aborted,
        RetryErrorReason::Exhausted {
            limit: RetryLimitKind::Attempts,
        },
    ];

    for reason in &reasons {
        assert_eq!(
            terminal_directive(reason, FailureDirective::Retry),
            FailureDirective::Discard,
        );
    }
}
