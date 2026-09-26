// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded, nonblocking publication for best-effort notifications.

mod notification_config;
mod notification_outcome;
mod notification_publisher;
mod notification_stats;
mod notification_stats_snapshot;
mod try_publish_error;

pub use notification_outcome::NotificationOutcome;
pub use notification_publisher::NotificationPublisher;
pub use notification_stats_snapshot::NotificationStatsSnapshot;
pub use try_publish_error::TryPublishError;
