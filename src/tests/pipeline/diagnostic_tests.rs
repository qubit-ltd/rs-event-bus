// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::pipeline::Diagnostic;
use crate::pipeline::DiagnosticObserver;
use crate::pipeline::emit_diagnostic;

struct PanicPayload;
impl Drop for PanicPayload {
    fn drop(&mut self) {
        panic!("panic payload destructor");
    }
}

#[test]
fn test_panic_payload_diagnostic_continues_to_next_observer() {
    let calls = Arc::new(AtomicUsize::new(0));
    let first: Arc<DiagnosticObserver> = Arc::new(|_| std::panic::panic_any(PanicPayload));
    let next_calls = calls.clone();
    let second: Arc<DiagnosticObserver> = Arc::new(move |_| {
        next_calls.fetch_add(1, Ordering::SeqCst);
    });
    let observers = [first, second];
    let outer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        emit_diagnostic(
            &observers,
            &Diagnostic::InternalFailure {
                origin: "test".into(),
                message: "test".into(),
            },
        )
    }));
    assert!(outer.is_ok(), "observer payload destructor must not escape");
    assert_eq!(1, calls.load(Ordering::SeqCst));
}
// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
