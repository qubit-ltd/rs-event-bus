// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Explores real scheduler transitions through its instrumented production
//! mutex.
//!
//! Models cover metadata grants and emitted wake targets. Adapter thread
//! parking and the final user-handler start fence have separate real-thread
//! regressions.

use std::num::NonZeroUsize;

use loom::model::Builder;
use loom::sync::Arc;
use loom::thread;
use qubit_id::Id;

use super::DeliverySchedulerCore;
use crate::facade::DeliveryMetricsSnapshot;
use crate::facade::DeliverySchedulingConfig;
use crate::pipeline::OrderingLaneKey;

/// Bounds exploration to the main thread, two racers, and two preemptions.
fn check_model(model: impl Fn() + Send + Sync + 'static) {
    let mut builder = Builder::new();
    builder.max_threads = 3;
    builder.preemption_bound = Some(2);
    builder.max_branches = 1_000;
    builder.check(model);
}

/// Creates a one-handler scheduler without sampling a real or synthetic clock.
fn scheduler(owned: usize, subscriptions: usize) -> Arc<DeliverySchedulerCore> {
    let positive = |value| NonZeroUsize::new(value).expect("positive model limit");
    let config = DeliverySchedulingConfig::new(
        positive(1),
        positive(owned),
        positive(owned),
        positive(subscriptions),
    )
    .expect("valid model limits");
    Arc::new(DeliverySchedulerCore::new(config))
}

/// Claims a real owned credit before the caller selects its enqueue kind.
fn reserve(core: &DeliverySchedulerCore, id: Id) -> u64 {
    core.request_receive(id);
    core.take_receive_reservation(id)
        .expect("model has owned capacity")
}

/// Counts all live ownership phases without including subset lane gauges.
fn owned_count(snapshot: DeliveryMetricsSnapshot) -> u64 {
    snapshot.reserved_receives + snapshot.queued + snapshot.running_handlers + snapshot.settling
}

#[test]
fn test_loom_scheduler_duplicate_release_races_handler_finish_and_reuse() {
    check_model(|| {
        let core = scheduler(1, 2);
        let first = Id::new(1);
        let next = Id::new(2);
        assert!(core.register(first));
        assert!(core.register(next));
        core.set_dispatch_active(first, true);
        let lease = reserve(&core, first);
        core.enqueue(lease, None);
        assert_eq!(core.take_ready(first), Some(lease));
        core.request_receive(next);

        let finishing_core = Arc::clone(&core);
        let finishing = thread::spawn(move || {
            finishing_core.handler_finished(lease);
            finishing_core.complete(lease);
        });
        let releasing_core = Arc::clone(&core);
        let releasing = thread::spawn(move || {
            releasing_core.complete(lease);
            let replacement = releasing_core
                .take_receive_reservation(next)
                .expect("exactly one replacement credit is granted");
            assert_ne!(replacement, lease, "released lease IDs cannot be reused");
            releasing_core.complete(lease);
            assert_eq!(owned_count(releasing_core.snapshot_gauges(None)), 1);
            assert_eq!(releasing_core.take_receive_reservation(next), None);
            releasing_core.request_receive(first);
            assert_eq!(
                releasing_core.take_receive_reservation(first),
                None,
                "duplicate release cannot admit a second owner above the sole owned slot"
            );
            releasing_core.cancel_receive(first);
            replacement
        });
        finishing.join().expect("handler completion racer exits");
        let replacement = releasing.join().expect("release and reuse racer exits");
        assert_eq!(core.snapshot_gauges(None).running_handlers, 0);
        assert_eq!(owned_count(core.snapshot_gauges(None)), 1);
        core.complete(replacement);
        assert_eq!(owned_count(core.snapshot_gauges(None)), 0);
        assert!(core.unregister(first));
        assert!(core.unregister(next));
    });
}

#[test]
fn test_loom_scheduler_stop_fences_handler_and_settlement_grants() {
    check_model(|| {
        let core = scheduler(3, 1);
        let id = Id::new(1);
        assert!(core.register(id));
        core.set_dispatch_active(id, true);
        let handler = reserve(&core, id);
        core.enqueue(handler, None);
        let settlement = reserve(&core, id);
        core.enqueue_settlement(settlement, None);
        core.request_receive(id);

        let stopping_core = Arc::clone(&core);
        let stopping = thread::spawn(move || {
            stopping_core.stop_subscription(id);
            // A stale owner activation must not reopen the permanent stop fence.
            stopping_core.set_dispatch_active(id, true);
            assert_eq!(stopping_core.take_ready(id), None);
            assert_eq!(stopping_core.take_settlement_ready(id), None);
            stopping_core.request_receive(id);
            assert_eq!(stopping_core.take_receive_reservation(id), None);
        });
        let granting_core = Arc::clone(&core);
        let granting = thread::spawn(move || {
            if let Some(lease) = granting_core.take_ready(id) {
                assert_eq!(lease, handler);
                granting_core.handler_finished(lease);
            }
            if let Some(lease) = granting_core.take_settlement_ready(id) {
                assert_eq!(lease, settlement);
            }
        });
        stopping.join().expect("stop racer exits");
        granting.join().expect("grant racer exits");
        assert_eq!(owned_count(core.snapshot_gauges(None)), 2);
        assert_eq!(core.snapshot_gauges(None).running_handlers, 0);
        assert!(
            !core.unregister(id),
            "stop retains the two claimed deliveries"
        );
        core.complete(handler);
        core.complete(settlement);
        assert_eq!(owned_count(core.snapshot_gauges(None)), 0);
        assert!(core.unregister(id));
    });
}

#[test]
fn test_loom_scheduler_handler_release_notification_survives_concurrent_drain() {
    check_model(|| {
        let core = scheduler(2, 2);
        let first = Id::new(1);
        let next = Id::new(2);
        assert!(core.register(first));
        assert!(core.register(next));
        core.set_dispatch_active(first, true);
        core.set_dispatch_active(next, true);
        let running = reserve(&core, first);
        core.enqueue(running, None);
        assert_eq!(core.take_ready(first), Some(running));
        let queued = reserve(&core, next);
        core.enqueue(queued, None);
        assert_eq!(core.take_ready(next), None);
        let _ = core.take_notifications();

        let releasing_core = Arc::clone(&core);
        let releasing = thread::spawn(move || releasing_core.handler_finished(running));
        let draining_core = Arc::clone(&core);
        let draining = thread::spawn(move || draining_core.take_notifications());
        releasing.join().expect("handler release racer exits");
        let mut notifications = draining.join().expect("notification drain racer exits");
        notifications.extend(core.take_notifications());
        assert!(
            notifications.contains(&next),
            "the newly runnable owner must be notified"
        );
        assert_eq!(core.take_ready(next), Some(queued));
        assert_eq!(core.snapshot_gauges(None).settling, 1);
        core.complete(running);
        core.complete(queued);
        assert_eq!(owned_count(core.snapshot_gauges(None)), 0);
    });
}

#[test]
fn test_loom_scheduler_owned_release_notification_survives_concurrent_drain() {
    check_model(|| {
        let core = scheduler(1, 2);
        let first = Id::new(1);
        let next = Id::new(2);
        assert!(core.register(first));
        assert!(core.register(next));
        let owned = reserve(&core, first);
        core.request_receive(next);
        assert_eq!(core.take_receive_reservation(next), None);
        let _ = core.take_notifications();

        let releasing_core = Arc::clone(&core);
        let releasing = thread::spawn(move || releasing_core.complete(owned));
        let draining_core = Arc::clone(&core);
        let draining = thread::spawn(move || draining_core.take_notifications());
        releasing.join().expect("owned release racer exits");
        let mut notifications = draining.join().expect("notification drain racer exits");
        notifications.extend(core.take_notifications());
        assert!(
            notifications.contains(&next),
            "the new receive reservation must be notified"
        );
        let replacement = core
            .take_receive_reservation(next)
            .expect("waiter receives freed capacity");
        assert_eq!(core.take_receive_reservation(next), None);
        core.complete(replacement);
        assert_eq!(owned_count(core.snapshot_gauges(None)), 0);
    });
}

#[test]
fn test_loom_scheduler_settlement_lane_release_wakes_without_handler_capacity() {
    check_model(|| {
        let core = scheduler(3, 1);
        let id = Id::new(1);
        assert!(core.register(id));
        core.set_dispatch_active(id, true);
        let running = reserve(&core, id);
        core.enqueue(running, None);
        assert_eq!(core.take_ready(id), Some(running));
        let lane = OrderingLaneKey::new("topic", Some("key"), id);
        let first = reserve(&core, id);
        core.enqueue_settlement(first, Some(lane.clone()));
        assert_eq!(core.take_settlement_ready(id), Some(first));
        let next = reserve(&core, id);
        core.enqueue_settlement(next, Some(lane));
        assert_eq!(core.take_settlement_ready(id), None);
        let _ = core.take_notifications();

        let releasing_core = Arc::clone(&core);
        let releasing = thread::spawn(move || releasing_core.complete(first));
        let draining_core = Arc::clone(&core);
        let draining = thread::spawn(move || draining_core.take_notifications());
        releasing
            .join()
            .expect("settlement lane release racer exits");
        let mut notifications = draining.join().expect("notification drain racer exits");
        notifications.extend(core.take_notifications());
        assert!(
            notifications.contains(&id),
            "released lane must notify its settlement owner"
        );
        assert_eq!(core.snapshot_gauges(None).running_handlers, 1);
        assert_eq!(core.take_settlement_ready(id), Some(next));
        assert_eq!(core.snapshot_gauges(None).running_handlers, 1);
        core.complete(next);
        core.complete(running);
        assert_eq!(owned_count(core.snapshot_gauges(None)), 0);
    });
}
