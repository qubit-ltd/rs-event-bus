// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded notification publication without waiting for queue capacity or
//! provider work on the calling thread.
//!
//! Admission uses an internal mutex, so calls can wait under mutex contention.

pub use notification_admission_error::NotificationAdmissionError;
pub use notification_outcome::NotificationOutcome;
pub use notification_publisher::NotificationPublisher;
pub use notification_rejection_reason::NotificationRejectionReason;
pub use notification_stats_snapshot::NotificationStatsSnapshot;
pub use try_publish_error::TryPublishError;
pub use weighted_notification_publisher::WeightedNotificationPublisher;

mod internal;
mod notification_admission_error;
mod notification_config;
mod notification_outcome;
mod notification_publisher;
mod notification_rejection_reason;
mod notification_stats;
mod notification_stats_snapshot;
mod try_publish_error;
mod weighted_notification_publisher;
