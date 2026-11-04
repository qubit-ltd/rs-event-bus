// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Benchmark-only access to the private facade signal implementation.

#[allow(dead_code)]
#[path = "../../src/facade/async_event_bus/internal/async_signal.rs"]
pub(crate) mod async_signal;

pub(crate) mod benchmark {
    use std::sync::Arc;

    use super::async_signal::AsyncSignal;
    use crate::CallbackCounts;
    use crate::Instant;
    use crate::Ordering;
    use crate::black_box;
    use crate::make_waker;

    pub(crate) const ITERATIONS: usize = 100_000;

    pub(crate) fn sample(alternating: bool) -> (u128, usize, usize, usize) {
        let signal = AsyncSignal::default();
        let first_counts = Arc::new(CallbackCounts::default());
        let second_counts = Arc::new(CallbackCounts::default());
        let first = make_waker(&first_counts);
        let second = make_waker(&second_counts);
        signal.register_waiter(black_box(1), &first);
        let start = Instant::now();
        for index in 0..ITERATIONS {
            let waker = if alternating && index % 2 == 1 { &second } else { &first };
            signal.register_waiter(black_box(1), black_box(waker));
        }
        let elapsed = start.elapsed().as_nanos() / ITERATIONS as u128;
        signal.notify();
        let clones = first_counts.clones.load(Ordering::Relaxed) + second_counts.clones.load(Ordering::Relaxed);
        let drops = first_counts.drops.load(Ordering::Relaxed) + second_counts.drops.load(Ordering::Relaxed);
        let wakes = first_counts.wakes.load(Ordering::Relaxed) + second_counts.wakes.load(Ordering::Relaxed);
        black_box((elapsed, clones, drops, wakes))
    }
}
