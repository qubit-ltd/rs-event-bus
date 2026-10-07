// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal dead-letter envelope-sharing tests.

use std::sync::Arc;

use crate::model::DeadLetterEvent;
use crate::model::EventEnvelope;
use crate::model::EventId;
use crate::model::SubscriberId;
use crate::model::Topic;

struct NonClonePayload;

#[test]
fn test_constructor_retains_the_exact_non_clone_event_allocation() {
    let envelope = Arc::new(EventEnvelope::with_id(
        Topic::<NonClonePayload>::new("dead.internal").expect("valid topic"),
        NonClonePayload,
        EventId::new("internal-event").expect("valid event ID"),
    ));
    let dead_letter = DeadLetterEvent::new(
        Arc::clone(&envelope),
        SubscriberId::new("internal-audit").expect("valid subscriber ID"),
        "terminal failure".into(),
    );

    assert_eq!(dead_letter.original_event().id().as_str(), "internal-event");
    assert!(Arc::ptr_eq(&dead_letter.original_event_arc(), &envelope));
    assert_eq!(dead_letter.reason(), "terminal failure");
}
