// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pending and delayed event state for a local subscription.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

use super::delayed_queue_head::DelayedQueueHead;
use super::delayed_queue_head::QueueKey;
use super::event::LocalEvent;
use super::event::LocalInFlight;
use super::queue_lane::QueueLane;

/// Mutable state owned by one subscription receiver.
#[derive(Default)]
pub(in crate::local) struct LocalQueueState {
    /// FIFO lanes keyed by ordering key, including the unkeyed lane.
    pub(in crate::local) lanes: HashMap<QueueKey, QueueLane>,
    /// Number of pending events across all lanes.
    pub(in crate::local) pending_count: usize,
    /// Ready lane heads in round-robin order, with their generations.
    pub(in crate::local) ready_lanes: VecDeque<(QueueKey, u64)>,
    /// Delayed lane heads ordered by deadline with lazy stale-entry removal.
    pub(in crate::local) delayed_lanes: BinaryHeap<Reverse<DelayedQueueHead>>,
    /// Stable tie breaker for delayed heads.
    pub(in crate::local) next_delay_sequence: u64,
    /// Number of delayed heap entries matching current lane heads.
    pub(in crate::local) delayed_live_count: usize,
    /// Number of delayed heap entries known to be stale.
    pub(in crate::local) delayed_stale_count: usize,
    /// Received but not yet terminally settled delivery attempts.
    pub(in crate::local) in_flight: HashMap<Box<str>, LocalInFlight>,
    /// Whether receive calls should stop and return `Closed`.
    pub(in crate::local) closed: bool,
    /// Monotonic token component that distinguishes redelivery attempts.
    pub(in crate::local) next_delivery_token: u64,
}

impl LocalQueueState {
    /// Adds an event to its lane tail and schedules a head when the lane was
    /// empty.
    ///
    /// # Parameters
    /// - `event`: event appended behind the current lane contents.
    pub(in crate::local) fn enqueue_back(&mut self, event: LocalEvent) {
        let key = event.ordering_key.clone();
        let lane = self.lanes.entry(key.clone()).or_default();
        let was_empty = lane.events.is_empty();
        lane.events.push_back(event);
        self.pending_count += 1;
        if was_empty {
            self.schedule_lane_head(key);
        }
    }

    /// Restores a retried event at its lane front and reschedules the lane
    /// head.
    ///
    /// # Parameters
    /// - `event`: event restored ahead of its same-key successors.
    pub(in crate::local) fn enqueue_front(&mut self, event: LocalEvent) {
        let key = event.ordering_key.clone();
        self.lanes.entry(key.clone()).or_default().events.push_front(event);
        self.pending_count += 1;
        self.schedule_lane_head(key);
    }

    /// Returns the total number of queued events across all lanes.
    ///
    /// # Returns
    /// Number of pending events.
    #[must_use = "Use the returned pending count."]
    #[inline]
    pub(in crate::local) fn pending_count(&self) -> usize {
        self.pending_count
    }

    /// Returns whether no queued event remains.
    ///
    /// # Returns
    /// `true` when all ordering lanes are empty.
    #[must_use = "Use the returned is pending empty."]
    #[inline]
    pub(in crate::local) fn is_pending_empty(&self) -> bool {
        self.pending_count == 0
    }

    /// Removes pending and in-flight events for destruction after queue unlock.
    ///
    /// # Returns
    /// Events whose payload references must be released outside the queue lock.
    pub(in crate::local) fn clear_pending(&mut self) -> Vec<LocalEvent> {
        let mut discarded = self.lanes.drain().flat_map(|(_, lane)| lane.events).collect::<Vec<_>>();
        discarded.extend(self.in_flight.drain().map(|(_, flight)| flight.event));
        self.lanes.clear();
        self.ready_lanes.clear();
        self.delayed_lanes.clear();
        self.pending_count = 0;
        self.delayed_live_count = 0;
        self.delayed_stale_count = 0;
        discarded
    }

    /// Pops one currently ready lane head, promoting expired delayed heads
    /// first.
    ///
    /// # Parameters
    /// - `now`: monotonic time used to decide whether a head is ready.
    ///
    /// # Returns
    /// `Some` with one event whose delay has elapsed, otherwise `None`.
    ///
    /// # Panics
    /// Panics if a ready-lane or heap entry violates its queue-state invariant.
    pub(in crate::local) fn pop_ready(&mut self, now: Instant) -> Option<LocalEvent> {
        self.promote_due_heads(now);
        while let Some((key, version)) = self.ready_lanes.pop_front() {
            let Some(lane) = self.lanes.get_mut(&key) else {
                continue;
            };
            if lane.version != version {
                continue;
            }
            if lane
                .events
                .front()
                .is_none_or(|event| event.not_before.is_some_and(|deadline| deadline > now))
            {
                continue;
            }
            let event = lane.events.pop_front().expect("ready lane has a head");
            self.pending_count -= 1;
            if lane.events.is_empty() {
                self.lanes.remove(&key);
            } else {
                self.schedule_lane_head(key);
            }
            return Some(event);
        }
        None
    }

    /// Returns the delay until the earliest live delayed lane head.
    ///
    /// # Parameters
    /// - `now`: current monotonic time.
    ///
    /// # Returns
    /// `Some` with the time until the next delayed event, or `None` when no
    /// delayed head exists.
    pub(in crate::local) fn next_ready_delay(&mut self, now: Instant) -> Option<Duration> {
        self.discard_stale_delayed_heads();
        self.delayed_lanes
            .peek()
            .map(|Reverse(head)| head.deadline.saturating_duration_since(now))
    }

    /// Increments a lane generation and schedules its current ready or delayed
    /// head.
    ///
    /// # Parameters
    /// - `key`: ordering lane whose current head is scheduled.
    ///
    /// # Panics
    /// Panics if scheduling loses the lane before its delayed entry is stored.
    pub(in crate::local) fn schedule_lane_head(&mut self, key: QueueKey) {
        let Some((version, deadline, invalidated_delayed_head)) = (|| {
            let lane = self.lanes.get_mut(&key)?;
            let invalidated_delayed_head = lane.delayed_version.take().is_some();
            lane.version = lane.version.wrapping_add(1);
            Some((
                lane.version,
                lane.events.front().and_then(|head| head.not_before),
                invalidated_delayed_head,
            ))
        })() else {
            return;
        };

        if invalidated_delayed_head {
            self.delayed_live_count -= 1;
            self.delayed_stale_count += 1;
        }
        if deadline.is_none_or(|deadline| deadline <= Instant::now()) {
            self.ready_lanes.push_back((key, version));
        } else if let Some(deadline) = deadline {
            self.next_delay_sequence = self.next_delay_sequence.wrapping_add(1);
            self.delayed_lanes.push(Reverse(DelayedQueueHead {
                deadline,
                sequence: self.next_delay_sequence,
                key: key.clone(),
                version,
            }));
            self.lanes
                .get_mut(&key)
                .expect("scheduled lane remains present")
                .delayed_version = Some(version);
            self.delayed_live_count += 1;
        }
        self.compact_delayed_heap_if_needed();
    }

    /// Promotes all due, still-current delayed lane heads to the ready queue.
    ///
    /// # Parameters
    /// - `now`: current monotonic time.
    ///
    /// # Panics
    /// Panics if the delayed heap changes between checking and removing its
    /// head, which would violate the queue's internal synchronization
    /// invariant.
    pub(in crate::local) fn promote_due_heads(&mut self, now: Instant) {
        self.discard_stale_delayed_heads();
        while self
            .delayed_lanes
            .peek()
            .is_some_and(|Reverse(head)| head.deadline <= now)
        {
            let Reverse(head) = self.delayed_lanes.pop().expect("peeked delayed head exists");
            let is_live = self.lanes.get_mut(&head.key).is_some_and(|lane| {
                if lane.version == head.version && lane.delayed_version == Some(head.version) {
                    lane.delayed_version = None;
                    true
                } else {
                    false
                }
            });
            if is_live {
                self.delayed_live_count -= 1;
                self.ready_lanes.push_back((head.key, head.version));
            } else {
                self.delayed_stale_count -= 1;
            }
            self.discard_stale_delayed_heads();
        }
    }

    /// Removes delayed entries whose lane no longer has the recorded
    /// generation.
    pub(in crate::local) fn discard_stale_delayed_heads(&mut self) {
        while self.delayed_lanes.peek().is_some_and(|Reverse(head)| {
            self.lanes
                .get(&head.key)
                .is_none_or(|lane| lane.version != head.version || lane.delayed_version != Some(head.version))
        }) {
            self.delayed_lanes.pop();
            self.delayed_stale_count -= 1;
        }
    }

    /// Rebuilds the delayed heap when stale entries exceed live heads or fixed
    /// slack.
    pub(in crate::local) fn compact_delayed_heap_if_needed(&mut self) {
        if self.delayed_stale_count <= self.delayed_live_count.max(8) {
            return;
        }
        let mut delayed_lanes = BinaryHeap::new();
        let mut live_count = 0;
        for (key, lane) in &self.lanes {
            let Some(version) = lane.delayed_version else {
                continue;
            };
            let Some(deadline) = lane.events.front().and_then(|event| event.not_before) else {
                continue;
            };
            self.next_delay_sequence = self.next_delay_sequence.wrapping_add(1);
            delayed_lanes.push(Reverse(DelayedQueueHead {
                deadline,
                sequence: self.next_delay_sequence,
                key: key.clone(),
                version,
            }));
            live_count += 1;
        }
        self.delayed_lanes = delayed_lanes;
        self.delayed_live_count = live_count;
        self.delayed_stale_count = 0;
    }
}
