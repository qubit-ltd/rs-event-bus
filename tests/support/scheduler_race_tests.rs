// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::mpsc;
use std::time::Duration;

use qubit_id::Id;

use crate::facade::SyncDeliverySchedulerConfig;
use crate::facade::sync_delivery_scheduler::SyncDeliveryScheduler;

#[test]
fn reservation_cancel_race_does_not_run_owner_settlement_inline() {
    let scheduler = SyncDeliveryScheduler::new(SyncDeliverySchedulerConfig::new(1, 1).expect("valid scheduler config"));
    scheduler.start().expect("scheduler workers start");
    let subscription_id = Id::new(44);
    let reservation = scheduler
        .try_reserve(subscription_id, None)
        .expect("reservation succeeds before cancellation");
    scheduler.cancel_subscription(subscription_id);

    let (callback_started_tx, callback_started_rx) = mpsc::channel();
    let (release_callback_tx, release_callback_rx) = mpsc::channel();
    let (submit_returned_tx, submit_returned_rx) = mpsc::channel();
    let submitter = std::thread::spawn(move || {
        reservation.submit(move |cancelled| {
            assert!(cancelled);
            callback_started_tx.send(()).expect("callback observer remains alive");
            release_callback_rx.recv().expect("release callback");
        });
        submit_returned_tx.send(()).expect("submit observer remains alive");
    });

    callback_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("cancellation callback starts");
    let returned_before_callback_release = submit_returned_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    release_callback_tx.send(()).expect("release callback");
    submitter.join().expect("submitter exits");
    assert!(
        returned_before_callback_release,
        "submit must return so its owner coordinator can service settlement"
    );
    assert_eq!(scheduler.cancelled_subscription_count(), 1);
    scheduler.finish_subscription(subscription_id);
    assert_eq!(scheduler.cancelled_subscription_count(), 0);
    scheduler.stop_admission(false);
    scheduler.join();
}
