// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Ordered delivery lanes for the local event bus.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use qubit_executor::CancelResult;
use qubit_executor::ScheduledExecutorService;
use qubit_thread_pool::FixedThreadPool;

use super::LocalEventBusInner;
use super::lifecycle::LocalEventBusLifecycle;
use super::lifecycle::delay_scheduler_for_dispatch;
use super::lifecycle::executor_for_dispatch;
use super::scheduling::submit_processing_task_to_executor;
use crate::EventBusError;
use crate::EventBusResult;
use crate::core::SubscriptionState;
use crate::local::ordering_lane_key::OrderingLaneKey;
use crate::local::processing_task::ProcessingTask;

mod lane_types {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use super::super::LocalEventBusInner;
    use crate::core::SubscriptionState;
    use crate::local::ordering_lane_key::OrderingLaneKey;
    use crate::local::processing_task::ProcessingTask;

    pub struct OrderedProcessingEntry {
        /// Processing task held by this queue entry.
        pub(super) task: ProcessingTask,
        /// Whether this entry consumes a queue-capacity reservation.
        pub(super) reserved_queue_slot: bool,
        /// Instant at which delayed waiting began.
        pub(super) delay_started_at: Option<Instant>,
        /// Requested delay for this entry.
        pub(super) delay: Option<Duration>,
        /// Subscription state used to cancel delayed work.
        pub(super) subscription_state: Option<Arc<SubscriptionState>>,
    }

    pub struct OrderedProcessingLane {
        /// FIFO entries waiting for the lane runner.
        pub(super) queued: VecDeque<OrderedProcessingEntry>,
    }

    pub enum OrderedLaneTask {
        /// Ready task that can run immediately.
        Ready(ProcessingTask),
        /// Front task waiting for its delay to elapse.
        Delayed(Duration, Arc<SubscriptionState>),
    }

    pub struct OrderedLaneRunnerGuard {
        /// Shared bus used to cancel abandoned lane work.
        pub(super) bus: Arc<LocalEventBusInner>,
        /// Lane to cancel on early drop, if still armed.
        pub(super) lane_key: Option<OrderingLaneKey>,
    }

    pub enum OrderedLaneTurn {
        /// No queued work remains.
        Drained,
        /// A new lane runner was submitted.
        Rescheduled,
        /// Queue admission required continuing in the current worker.
        ContinueInline,
        /// Lane processing was cancelled after an error.
        Cancelled,
    }
}

use lane_types::OrderedLaneRunnerGuard;
use lane_types::OrderedLaneTask;
use lane_types::OrderedLaneTurn;
use lane_types::OrderedProcessingEntry;
pub(super) use lane_types::OrderedProcessingLane;

impl OrderedProcessingEntry {
    /// Creates an ordered processing entry.
    fn new(task: ProcessingTask, reserved_queue_slot: bool) -> Self {
        Self {
            task,
            reserved_queue_slot,
            delay_started_at: None,
            delay: None,
            subscription_state: None,
        }
    }

    /// Creates a delayed ordered processing entry.
    fn delayed(
        task: ProcessingTask,
        reserved_queue_slot: bool,
        delay: Duration,
        subscription_state: Arc<SubscriptionState>,
    ) -> Self {
        Self {
            task,
            reserved_queue_slot,
            delay_started_at: Some(Instant::now()),
            delay: Some(delay),
            subscription_state: Some(subscription_state),
        }
    }

    /// Returns whether the entry belongs to a cancelled subscription.
    fn is_inactive(&self) -> bool {
        self.subscription_state.as_ref().is_some_and(|state| !state.is_active())
    }

    /// Returns the remaining delay before this entry is ready.
    fn remaining_delay(&self) -> Option<(Duration, Arc<SubscriptionState>)> {
        let delay_started_at = self.delay_started_at?;
        let delay = self.delay?;
        let subscription_state = self.subscription_state.as_ref()?;
        delay
            .checked_sub(delay_started_at.elapsed())
            .filter(|remaining| !remaining.is_zero())
            .map(|remaining| (remaining, Arc::clone(subscription_state)))
    }
}

impl OrderedProcessingLane {
    /// Creates an empty active lane.
    fn new() -> Self {
        Self {
            queued: VecDeque::new(),
        }
    }

    /// Queues a task behind the active task.
    fn push(&mut self, task: ProcessingTask, reserved_queue_slot: bool) {
        self.queued
            .push_back(OrderedProcessingEntry::new(task, reserved_queue_slot));
    }

    /// Queues a delayed task behind the active task.
    fn push_delayed(
        &mut self,
        task: ProcessingTask,
        reserved_queue_slot: bool,
        delay: Duration,
        subscription_state: Arc<SubscriptionState>,
    ) {
        self.queued.push_back(OrderedProcessingEntry::delayed(
            task,
            reserved_queue_slot,
            delay,
            subscription_state,
        ));
    }

    /// Takes the next queued task.
    fn pop(&mut self) -> Option<OrderedProcessingEntry> {
        self.queued.pop_front()
    }

    /// Returns whether the lane has no queued work.
    fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    /// Releases the local reservation for the next task if one exists.
    fn release_front_queue_slot(&mut self) -> usize {
        let Some(entry) = self.queued.front_mut() else {
            return 0;
        };
        if entry.reserved_queue_slot {
            entry.reserved_queue_slot = false;
            1
        } else {
            0
        }
    }

    /// Releases all local reservations still held by the lane.
    fn release_all_queue_slots(&mut self) -> usize {
        let mut released = 0;
        for entry in &mut self.queued {
            if entry.reserved_queue_slot {
                entry.reserved_queue_slot = false;
                released += 1;
            }
        }
        released
    }
}

impl OrderedLaneRunnerGuard {
    /// Creates a lane runner guard.
    fn new(bus: Arc<LocalEventBusInner>, lane_key: OrderingLaneKey) -> Self {
        Self {
            bus,
            lane_key: Some(lane_key),
        }
    }

    /// Marks the lane as drained normally.
    fn disarm(&mut self) {
        self.lane_key = None;
    }
}

impl Drop for OrderedLaneRunnerGuard {
    /// Cancels queued tasks when the lane runner exits before draining them.
    fn drop(&mut self) {
        if let Some(lane_key) = self.lane_key.take() {
            self.bus.cancel_ordered_lane(&lane_key);
        }
    }
}

impl LocalEventBusInner {
    /// Reserves one local ordered-lane queue slot.
    fn reserve_ordered_queue_slot(&self, lifecycle: &LocalEventBusLifecycle) -> EventBusResult<()> {
        let Some(capacity) = self.delivery_limits.handler_queue_capacity() else {
            return Ok(());
        };
        let executor_queued = lifecycle
            .executor
            .as_ref()
            .map(FixedThreadPool::queued_count)
            .unwrap_or_default();
        self.ordered_queued_task_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |ordered_queued| {
                if ordered_queued.saturating_add(executor_queued) >= capacity {
                    None
                } else {
                    Some(ordered_queued + 1)
                }
            })
            .map(|_| ())
            .map_err(|_| EventBusError::execution_rejected("subscription handler queue capacity is saturated"))
    }

    /// Releases local ordered-lane queue slots.
    fn release_ordered_queue_slots(&self, slots: usize) {
        if slots == 0 {
            return;
        }
        let _ = self
            .ordered_queued_task_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(current.saturating_sub(slots))
            });
    }

    /// Submits a lane runner to a known-live handler executor.
    fn submit_ordered_lane_runner_to_executor(
        self: &Arc<Self>,
        executor: &FixedThreadPool,
        lane_key: OrderingLaneKey,
    ) -> EventBusResult<()> {
        let bus = Arc::clone(self);
        submit_processing_task_to_executor(executor, move || {
            bus.run_ordered_lane(lane_key);
        })
    }

    /// Submits the lane runner for an ordering lane to the handler executor.
    fn submit_ordered_lane_runner(
        self: &Arc<Self>,
        lane_key: OrderingLaneKey,
        allow_stopping: bool,
    ) -> EventBusResult<()> {
        let bus = Arc::clone(self);
        self.submit_processing_task(
            move || {
                bus.run_ordered_lane(lane_key);
            },
            allow_stopping,
        )
    }

    /// Resubmits an ordered lane after its front task delay elapses.
    fn submit_ordered_lane_runner_after_delay(
        self: &Arc<Self>,
        lane_key: OrderingLaneKey,
        delay: Duration,
        subscription_state: Arc<SubscriptionState>,
    ) -> EventBusResult<()> {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
        let delay_scheduler = delay_scheduler_for_dispatch(&lifecycle, true)?;
        let bus = Arc::clone(self);
        let registration = Arc::new(Mutex::new(None::<usize>));
        let fired = Arc::new(Mutex::new(false));
        let registration_for_delay = Arc::clone(&registration);
        let fired_for_delay = Arc::clone(&fired);
        let subscription_for_delay = Arc::clone(&subscription_state);
        let lane_key_for_delay = lane_key.clone();
        let scheduled = delay_scheduler.schedule(delay, move || {
            if let Ok(mut fired) = fired_for_delay.lock() {
                *fired = true;
            }
            if let Ok(mut registration) = registration_for_delay.lock()
                && let Some(registration_id) = registration.take()
            {
                subscription_for_delay.unregister_delay_cancellation(registration_id);
            }
            if subscription_for_delay.is_active() {
                match bus.submit_ordered_lane_runner(lane_key_for_delay.clone(), true) {
                    Ok(()) => {}
                    Err(error) => {
                        bus.reject_ordered_lane(&lane_key_for_delay, &error);
                    }
                }
            } else {
                bus.cancel_ordered_lane(&lane_key_for_delay);
            }
            Ok::<(), EventBusError>(())
        });
        let handle = match scheduled {
            Ok(handle) => handle,
            Err(error) => {
                return Err(EventBusError::execution_rejected(error.to_string()));
            }
        };
        if fired.lock().map(|fired| *fired).unwrap_or(true) {
            return Ok(());
        }
        let bus_for_cancel = Arc::clone(self);
        let lane_key_for_cancel = lane_key.clone();
        let handle_for_cancel = Arc::new(Mutex::new(Some(handle)));
        let registration_id = subscription_state.register_delay_cancellation(move || {
            if let Ok(mut handle) = handle_for_cancel.lock()
                && let Some(handle) = handle.take()
                && handle.cancel() == CancelResult::Cancelled
            {
                bus_for_cancel.cancel_ordered_lane(&lane_key_for_cancel);
            }
        });
        if let Some(registration_id) = registration_id
            && let Ok(mut registration) = registration.lock()
        {
            if fired.lock().map(|fired| *fired).unwrap_or(true) {
                subscription_state.unregister_delay_cancellation(registration_id);
            } else {
                *registration = Some(registration_id);
            }
        }
        Ok(())
    }

    /// Pops the next task for an ordered lane.
    fn pop_ordered_lane_task(
        &self,
        lane_key: &OrderingLaneKey,
        guard: &mut OrderedLaneRunnerGuard,
    ) -> Option<OrderedLaneTask> {
        loop {
            let Ok(mut lanes) = self.ordering_lanes.lock() else {
                let error = EventBusError::lock_poisoned("ordering_lanes");
                self.observe_error(&error);
                return None;
            };
            let Some(lane) = lanes.get_mut(lane_key) else {
                guard.disarm();
                return None;
            };

            let Some(front) = lane.queued.front() else {
                lanes.remove(lane_key);
                guard.disarm();
                return None;
            };
            if front.is_inactive() {
                let mut inactive_entry = lane.pop().expect("front entry should exist after inactive check");
                if inactive_entry.reserved_queue_slot {
                    inactive_entry.reserved_queue_slot = false;
                    self.release_ordered_queue_slots(1);
                }
                drop(inactive_entry);
                continue;
            }
            if let Some((remaining, subscription_state)) = front.remaining_delay() {
                return Some(OrderedLaneTask::Delayed(remaining, subscription_state));
            }

            let mut next_entry = lane.pop().expect("front entry should exist after readiness check");
            if next_entry.reserved_queue_slot {
                next_entry.reserved_queue_slot = false;
                self.release_ordered_queue_slots(1);
            }
            return Some(OrderedLaneTask::Ready(next_entry.task));
        }
    }

    /// Finishes one ordered lane turn and decides how the lane should continue.
    fn finish_ordered_lane_turn(
        self: &Arc<Self>,
        lane_key: &OrderingLaneKey,
        guard: &mut OrderedLaneRunnerGuard,
    ) -> OrderedLaneTurn {
        {
            let Ok(mut lanes) = self.ordering_lanes.lock() else {
                let error = EventBusError::lock_poisoned("ordering_lanes");
                self.observe_error(&error);
                return OrderedLaneTurn::Cancelled;
            };
            let Some(lane) = lanes.get_mut(lane_key) else {
                guard.disarm();
                return OrderedLaneTurn::Drained;
            };
            if lane.is_empty() {
                lanes.remove(lane_key);
                guard.disarm();
                return OrderedLaneTurn::Drained;
            }
            let released = lane.release_front_queue_slot();
            self.release_ordered_queue_slots(released);
        };
        match self.submit_ordered_lane_runner(lane_key.clone(), true) {
            Ok(()) => {
                guard.disarm();
                OrderedLaneTurn::Rescheduled
            }
            Err(EventBusError::ExecutionRejected { .. }) => OrderedLaneTurn::ContinueInline,
            Err(error) => {
                self.observe_error(&error);
                self.cancel_ordered_lane(lane_key);
                guard.disarm();
                OrderedLaneTurn::Cancelled
            }
        }
    }

    /// Submits subscriber processing work through a per-ordering-key lane.
    ///
    /// Tasks in the same lane are submitted to the handler executor one at a
    /// time, preserving publish order for a topic, subscriber, and ordering
    /// key.
    ///
    /// # Parameters
    /// - `lane_key`: Topic, subscriber, and ordering key identifying the lane.
    /// - `task`: Processing task to run or cancel.
    /// - `allow_stopping`: Whether already accepted internal work may continue
    ///   while the bus is stopping.
    ///
    /// # Returns
    /// `Ok(())` when the task is accepted into the ordering lane.
    ///
    /// # Errors
    /// Returns lock-poisoning, stopped-bus, or queue-saturation errors.
    pub(crate) fn submit_ordered_processing_task(
        self: &Arc<Self>,
        lane_key: OrderingLaneKey,
        task: ProcessingTask,
        allow_stopping: bool,
    ) -> EventBusResult<()> {
        let result = {
            let lifecycle = self
                .lifecycle
                .lock()
                .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
            let executor = executor_for_dispatch(&lifecycle, allow_stopping)?;
            let mut lanes = self
                .ordering_lanes
                .lock()
                .map_err(|_| EventBusError::lock_poisoned("ordering_lanes"))?;
            if let Some(lane) = lanes.get_mut(&lane_key) {
                self.reserve_ordered_queue_slot(&lifecycle)?;
                lane.push(task, true);
                return Ok(());
            }
            let mut lane = OrderedProcessingLane::new();
            lane.push(task, false);
            lanes.insert(lane_key.clone(), lane);
            drop(lanes);
            self.submit_ordered_lane_runner_to_executor(executor, lane_key.clone())
        };
        if result.is_err() {
            self.cancel_ordered_lane(&lane_key);
        }
        result
    }

    /// Submits delayed subscriber processing work through a per-ordering-key
    /// lane.
    pub(crate) fn submit_delayed_ordered_processing_task(
        self: &Arc<Self>,
        lane_key: OrderingLaneKey,
        task: ProcessingTask,
        delay: Duration,
        subscription_state: Arc<SubscriptionState>,
        allow_stopping: bool,
    ) -> EventBusResult<()> {
        let result = {
            let lifecycle = self
                .lifecycle
                .lock()
                .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
            let executor = executor_for_dispatch(&lifecycle, allow_stopping)?;
            let mut lanes = self
                .ordering_lanes
                .lock()
                .map_err(|_| EventBusError::lock_poisoned("ordering_lanes"))?;
            if let Some(lane) = lanes.get_mut(&lane_key) {
                self.reserve_ordered_queue_slot(&lifecycle)?;
                lane.push_delayed(task, true, delay, subscription_state);
                return Ok(());
            }
            let mut lane = OrderedProcessingLane::new();
            lane.push_delayed(task, false, delay, subscription_state);
            lanes.insert(lane_key.clone(), lane);
            drop(lanes);
            self.submit_ordered_lane_runner_to_executor(executor, lane_key.clone())
        };
        if result.is_err() {
            self.cancel_ordered_lane(&lane_key);
        }
        result
    }

    /// Drains one turn of an ordering lane on a handler-pool worker.
    fn run_ordered_lane(self: Arc<Self>, lane_key: OrderingLaneKey) {
        let mut guard = OrderedLaneRunnerGuard::new(Arc::clone(&self), lane_key.clone());
        loop {
            let Some(task) = self.pop_ordered_lane_task(&lane_key, &mut guard) else {
                return;
            };
            match task {
                OrderedLaneTask::Ready(task) => task.run(),
                OrderedLaneTask::Delayed(delay, subscription_state) => {
                    match self.submit_ordered_lane_runner_after_delay(lane_key.clone(), delay, subscription_state) {
                        Ok(()) => guard.disarm(),
                        Err(error) => {
                            self.reject_ordered_lane(&lane_key, &error);
                            guard.disarm();
                        }
                    }
                    return;
                }
            }
            match self.finish_ordered_lane_turn(&lane_key, &mut guard) {
                OrderedLaneTurn::ContinueInline => {}
                OrderedLaneTurn::Drained | OrderedLaneTurn::Rescheduled | OrderedLaneTurn::Cancelled => return,
            }
        }
    }

    /// Removes an ordering lane and drops all queued processing tasks.
    fn cancel_ordered_lane(&self, lane_key: &OrderingLaneKey) {
        let removed_lane = {
            let Ok(mut lanes) = self.ordering_lanes.lock() else {
                let error = EventBusError::lock_poisoned("ordering_lanes");
                self.observe_error(&error);
                return;
            };
            lanes.remove(lane_key)
        };
        if let Some(mut lane) = removed_lane {
            let released = lane.release_all_queue_slots();
            self.release_ordered_queue_slots(released);
        }
    }

    /// Rejects every accepted task in a lane after delayed executor admission
    /// fails.
    fn reject_ordered_lane(&self, lane_key: &OrderingLaneKey, cause: &EventBusError) {
        let removed_lane = match self.ordering_lanes.lock() {
            Ok(mut lanes) => lanes.remove(lane_key),
            Err(_) => {
                self.observe_error(&EventBusError::lock_poisoned("ordering_lanes"));
                return;
            }
        };
        if let Some(mut lane) = removed_lane {
            let released = lane.release_all_queue_slots();
            self.release_ordered_queue_slots(released);
            for entry in lane.queued {
                entry.task.reject(cause);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;

    use super::OrderedProcessingLane;
    use crate::EventBusError;
    use crate::LocalEventBus;
    use crate::Topic;
    use crate::local::ordering_lane_key::OrderingLaneKey;
    use crate::local::processing_task::DeliveryContext;
    use crate::local::processing_task::ProcessingTask;

    /// A rejected ordered lane reports each accepted delivery and releases
    /// every slot.
    #[test]
    fn test_rejected_ordered_lane_reports_every_delivery() {
        let bus = LocalEventBus::started().expect("bus should start");
        let topic = Topic::<String>::try_new("ordered-rejection").expect("topic should build");
        let topic_key = topic.key();
        let lane_key = OrderingLaneKey::new(topic_key.clone(), "same-key", 1);
        let errors = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&errors);
        bus.add_error_observer(move |error| {
            captured.lock().expect("errors should lock").push(error.to_string());
        })
        .expect("observer should register");
        let mut lane = OrderedProcessingLane::new();
        for (event_id, reserved) in [("event-1", false), ("event-2", true)] {
            bus.inner.start_processing(&topic_key).expect("tracking should start");
            let context = DeliveryContext {
                event_id: event_id.to_string(),
                topic_name: topic.name().to_string(),
                subscriber_id: "sub".to_string(),
            };
            lane.push(
                ProcessingTask::with_delivery_context(Arc::clone(&bus.inner), topic_key.clone(), context, || {}),
                reserved,
            );
        }
        bus.inner.ordered_queued_task_count.store(1, Ordering::SeqCst);
        bus.inner
            .ordering_lanes
            .lock()
            .expect("lanes should lock")
            .insert(lane_key.clone(), lane);
        bus.inner
            .reject_ordered_lane(&lane_key, &EventBusError::execution_rejected("queue full"));
        let errors = errors.lock().expect("errors should lock");
        assert_eq!(errors.len(), 2);
        assert!(errors.iter().any(|error| error.contains("event-1")));
        assert!(errors.iter().any(|error| error.contains("event-2")));
        assert_eq!(bus.inner.ordered_queued_task_count.load(Ordering::SeqCst), 0);
        bus.wait_for_idle(&topic)
            .expect("rejected tasks should release idle accounting");
    }
}
